from transforms.api import transform, Output, Input, incremental, configure
from pyspark.sql import functions as F
from pyspark.sql.functions import when, col, udf
from pyspark.sql.types import StringType

import re
import json
import time
import random
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from language_model_service_api.languagemodelservice_api_completion_v3 import GenericVisionCompletionRequest
from language_model_service_api.languagemodelservice_api import (
    ChatMessageRole,
    GenericMediaContent,
    GenericMessage,
    GenericMessageContent,
    MimeType,
)
from palantir_models.transforms import GenericVisionCompletionLanguageModelInput

# Setup logging
logger = logging.getLogger(__name__)

IMAGE_PATTERN = re.compile(r'!\[([^\]]*)\]\(data:image/(jpeg|png);base64,([^)]+)\)')

# Configuration constants
MAX_CONCURRENT_LLM_CALLS = 5  # Adjust based on API rate limits
LLM_TIMEOUT_SECONDS = 120  # Timeout per LLM call

prompt = """
FIRST : Check if photo is a logo, find out the company name and output "Logo" and skip the following instructions.
SECOND : Check if photo is a header/banner, if it is give a short description of the visual and skip the following instructions.
THIRD : Check if photo is a person/place/object/animal, if it is just give a short description of the photo and skip the following instructions.
THEN : You are an elite Macroeconomic Research Analyst, given the chart and the legends extract a table representing the information at all data points. Give output in the format of a markdown table. This table should be able to output the chart exactly later on. Omit any niceties, directly output the table.
Provide analysis on qunatitative data shown in the chart, specify values shown while giving the detailed analysis.
Only use "\n" to write new lines, dont use "\n\n"
At the end add "Warning : These values have been estimated from a chart, make sure to verify before use"
"""

@configure(
    profile=[
        "NUM_EXECUTORS_16",
        "EXECUTOR_MEMORY_LARGE",
        "DRIVER_MEMORY_MEDIUM",
    ]
)


@incremental(v2_semantics=True)
@transform(
    mds=Input("ri.foundry.main.dataset.280d7478-dbe2-4142-a864-73609ce2f8b4"),
    md_output=Output("/GI-DEV-SPACE-4ecd2f/DEV-UC-GIN-1219-Workflow/Transform_pipeline/App/Backing Data/[GI SC] Docling_Analyzed_2"),
    model=GenericVisionCompletionLanguageModelInput("ri.language-model-service..language-model.gemini-2-5-pro")
)
def compute(ctx, mds, model, md_output):

    def get_chart_analysis_fromBase64(im_b64):
        """Analyze image with vision LLM, with retry logic"""
        prompt_content = GenericMessageContent(text=prompt)
        if im_b64.startswith('/9'):
            im_type = MimeType.IMAGE_JPEG
        else:
            im_type = MimeType.IMAGE_PNG
        max_retries = 5

        for attempt in range(max_retries):
            try:
                request = GenericVisionCompletionRequest([
                    GenericMessage(contents=[
                            prompt_content,
                        GenericMessageContent(generic_media=GenericMediaContent(content = im_b64, mime_type= im_type))
                        ], role=ChatMessageRole.USER),
                    ], max_tokens=5000, temperature=0)
                response = model.create_vision_completion(request).completion
                return response

            except Exception as e:
                if attempt == max_retries - 1:
                    logger.error(f"Failed after {max_retries} attempts: {e}")
                    return f"Error processing image: {str(e)[:100]}"

                # Exponential backoff with jitter
                delay = (45) + random.uniform(0, 30)
                logger.warning(f"Attempt {attempt + 1} failed: {e}")
                logger.warning(f"Retrying in {delay:.2f} seconds...")
                time.sleep(delay)

    def call_llm_for_image(img_b64):
        """
        Wrapper function for calling LLM on a single image.
        This function is submitted to ThreadPoolExecutor.
        Returns tuple of (img_b64, analysis_result)
        """
        try:
            analysis = get_chart_analysis_fromBase64(img_b64)
            return (img_b64, analysis)
        except Exception as e:
            logger.error(f"Error in LLM call for image: {e}")
            return (img_b64, f"Error processing image: {str(e)[:100]}")

    markdowns = mds.dataframe()

    logger.info(f"Processing {markdowns.count()} markdown pages")

    def analyse_page(text):
        """Process all images in a page of markdown - PARALLELIZED VERSION"""
        if not text or len(text.strip()) == 0:
            return text

        try:
            image_dict = {}

            matches = IMAGE_PATTERN.findall(text)

            if not matches:
                return text

            logger.info(f"Processing {len(matches)} images in page")

            # Step 1: Identify unique images and filter by size
            for explanation, image_type, base64_data in matches:
                if base64_data not in image_dict:
                    estimated_size = len(base64_data) * 0.75
                    max_size = 5 * 1024 * 1024  # 5MB

                    if estimated_size > max_size:
                        logger.warning(f"Skipping large image (estimated {estimated_size/1024/1024:.1f}MB)")
                        image_dict[base64_data] = "Image too large to process safely"
                    else:
                        image_dict[base64_data] = None

            # Step 2: Parallel processing of images that need analysis
            images_to_process = [img_b64 for img_b64, analysis in image_dict.items() if analysis is None]

            if images_to_process:
                logger.info(f"Analyzing {len(images_to_process)} unique images in parallel (max_workers={MAX_CONCURRENT_LLM_CALLS})")
                parallel_start = time.time()

                # Use ThreadPoolExecutor for parallel LLM calls (similar to reference code)
                with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_LLM_CALLS) as executor:
                    # Submit all tasks
                    future_to_image = {
                        executor.submit(call_llm_for_image, img_b64): img_b64
                        for img_b64 in images_to_process
                    }

                    # Collect results as they complete
                    completed_count = 0
                    for future in as_completed(future_to_image, timeout=LLM_TIMEOUT_SECONDS * len(images_to_process)):
                        try:
                            img_b64, analysis = future.result(timeout=LLM_TIMEOUT_SECONDS)
                            image_dict[img_b64] = analysis
                            completed_count += 1

                            if completed_count % 5 == 0 or completed_count == len(images_to_process):
                                logger.info(f"Completed {completed_count}/{len(images_to_process)} image analyses")

                        except Exception as e:
                            img_b64 = future_to_image[future]
                            logger.error(f"Failed to analyze image: {e}")
                            image_dict[img_b64] = f"Error processing image: {str(e)[:100]}"

                parallel_duration = time.time() - parallel_start
                logger.info(f"Parallel processing completed {len(images_to_process)} images in {parallel_duration:.2f}s "
                           f"(avg {parallel_duration/len(images_to_process):.2f}s per image)")

            # Step 3: Replace images with analysis in markdown
            md = IMAGE_PATTERN.sub(
                lambda match: f"\n<!-- Picture description:{image_dict.get(match.group(3), 'Analysis not available')}-->\n",
                text
            )

            return md

        except Exception as e:
            logger.error(f"Error in analyse_page: {e}")
            return text  # Return original text on error

    process_page_udf = F.udf(analyse_page, StringType())

    current_timestamp = datetime.now().isoformat()
    current_stage = "image analysis completed"

    def merge_timestamp_udf(existing_timestamp: str) -> str:
        """Merge new stage timestamp with existing timestamp JSON"""
        try:
            if existing_timestamp and existing_timestamp.strip():
                timestamp_dict = json.loads(existing_timestamp)
            else:
                timestamp_dict = {}

            timestamp_dict[current_stage] = current_timestamp
            return json.dumps(timestamp_dict)
        except Exception:
            return json.dumps({current_stage: current_timestamp})

    merge_udf = udf(merge_timestamp_udf, StringType())

    logger.info("Detecting pages with images...")
    markdowns = markdowns.withColumn(
        "has_image",
        col("converted_markdown").rlike(r'!\[([^\]]*)\]\(data:image/(jpeg|png);base64,([^)]+)\)')
    )

    images_count = markdowns.filter(col("has_image") == True).count()
    logger.info(f"Found {images_count} pages with images")

    markdowns = markdowns.withColumn(
        "converted_markdown",
        when(col("has_image") == True,
             process_page_udf(col("converted_markdown")))
        .otherwise(col("converted_markdown"))
    ).withColumn(
        "status",
        F.lit("Images Analyzed")
    ).withColumn(
        "timestamp",
        merge_udf(F.col("timestamp"))
    )

    logger.info("Writing output dataframe")
    md_output.write_dataframe(markdowns)
    logger.info("Transform completed successfully")

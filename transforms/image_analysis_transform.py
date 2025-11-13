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

from language_model_service_api.languagemodelservice_api_completion_v3 import GenericVisionCompletionRequest
from language_model_service_api.languagemodelservice_api import (
    ChatMessageRole,
    GenericMediaContent,
    GenericMessage,
    GenericMessageContent,
    MimeType,
)
from palantir_models.transforms import GenericVisionCompletionLanguageModelInput

# Compile regex pattern at module level for efficiency
IMAGE_PATTERN = re.compile(r'!\[([^\]]*)\]\(data:image/(jpeg|png);base64,([^)]+)\)')

prompt = """
FIRST : Check if photo is a logo, find out the company name and output "Logo" and skip the following instructions.
SECOND : Check if photo is a header/banner, if it is give a short description of the visual and skip the following instructions.
THIRD : Check if photo is a person/place/object/animal, if it is just give a short description of the photo and skip the following instructions.
THEN : You are an elite Macroeconomic Research Analyst, given the chart and the legends extract a table representing the information at all data points. Give output in the format of a markdown table. This table should be able to output the chart exactly later on. Omit any niceties, directly output the table.
Provide analysis on quantitative data shown in the chart, specify values shown while giving the detailed analysis.
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
    # Get logger at module level (will be recreated in UDF)
    main_logger = logging.getLogger(__name__)

    def get_chart_analysis_fromBase64(im_b64, image_type, attempt_logger):
        """Analyze image with vision LLM, with proper exponential backoff retry logic

        Args:
            im_b64: Base64 encoded image data
            image_type: Image type from regex ('jpeg' or 'png')
            attempt_logger: Logger instance for this attempt
        """
        prompt_content = GenericMessageContent(text=prompt)
        # Use the captured image type from regex instead of unreliable base64 detection
        im_type = MimeType.IMAGE_JPEG if image_type == 'jpeg' else MimeType.IMAGE_PNG
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
                    attempt_logger.error(f"Failed after {max_retries} attempts: {e}")
                    return f"Error processing image: {str(e)[:100]}"

                # FIXED: Proper exponential backoff: 5s, 10s, 20s, 40s, 80s + jitter
                delay = (2 ** attempt) * 5 + random.uniform(0, 5)
                attempt_logger.warning(f"Attempt {attempt + 1}/{max_retries} failed: {e}")
                attempt_logger.warning(f"Retrying in {delay:.2f} seconds...")
                time.sleep(delay)

    markdowns = mds.dataframe()

    main_logger.info(f"Processing {markdowns.count()} markdown pages")

    def analyse_page(text):
        """Process all images in a page of markdown.

        FIXED: Removed ThreadPoolExecutor nested parallelism anti-pattern.
        Now processes images sequentially per page, letting Spark's 16 executors
        provide parallelism across pages. This prevents uncontrolled concurrency
        (previously: 16 executors × 5 threads = 80 concurrent API calls).
        """
        # FIXED: Initialize logger inside UDF for proper serialization
        page_logger = logging.getLogger(__name__)

        if not text or len(text.strip()) == 0:
            return text

        try:
            # Dictionary to store unique images and their analyses
            # Key: base64 data, Value: tuple of (image_type, analysis_result)
            image_dict = {}

            matches = IMAGE_PATTERN.findall(text)

            if not matches:
                return text

            page_logger.info(f"Processing {len(matches)} images in page")

            # Step 1: Identify unique images, filter by size, and store image type
            for explanation, image_type, base64_data in matches:
                if base64_data not in image_dict:
                    estimated_size = len(base64_data) * 0.75
                    max_size = 5 * 1024 * 1024  # 5MB

                    if estimated_size > max_size:
                        page_logger.warning(f"Skipping large image (estimated {estimated_size/1024/1024:.1f}MB)")
                        image_dict[base64_data] = (image_type, "Image too large to process safely")
                    else:
                        # Store image type from regex for later use
                        image_dict[base64_data] = (image_type, None)

            # Step 2: Sequential processing of images (Spark executors provide parallelism)
            images_to_process = [(img_b64, img_type) for img_b64, (img_type, analysis) in image_dict.items() if analysis is None]

            if images_to_process:
                page_logger.info(f"Analyzing {len(images_to_process)} unique images sequentially")
                processing_start = time.time()

                for idx, (img_b64, img_type) in enumerate(images_to_process, 1):
                    # FIXED: Pass image_type from regex and logger instance
                    analysis = get_chart_analysis_fromBase64(img_b64, img_type, page_logger)
                    image_dict[img_b64] = (img_type, analysis)

                    if idx % 5 == 0 or idx == len(images_to_process):
                        page_logger.info(f"Completed {idx}/{len(images_to_process)} image analyses")

                processing_duration = time.time() - processing_start
                page_logger.info(f"Sequential processing completed {len(images_to_process)} images in {processing_duration:.2f}s "
                           f"(avg {processing_duration/len(images_to_process):.2f}s per image)")

            # Step 3: Replace images with analysis in markdown
            def replace_image(match):
                base64_data = match.group(3)
                if base64_data in image_dict:
                    _, analysis = image_dict[base64_data]
                    return f"\n<!-- Picture description:{analysis}-->\n"
                return f"\n<!-- Picture description:Analysis not available-->\n"

            md = IMAGE_PATTERN.sub(replace_image, text)

            return md

        except Exception as e:
            page_logger.error(f"Error in analyse_page: {e}")
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

    main_logger.info("Detecting pages with images...")
    markdowns = markdowns.withColumn(
        "has_image",
        col("converted_markdown").rlike(r'!\[([^\]]*)\]\(data:image/(jpeg|png);base64,([^)]+)\)')
    )

    images_count = markdowns.filter(col("has_image") == True).count()
    main_logger.info(f"Found {images_count} pages with images")

    # FIXED: Process only pages with images, set status correctly, drop temp column
    markdowns = markdowns.withColumn(
        "converted_markdown",
        when(col("has_image") == True,
             process_page_udf(col("converted_markdown")))
        .otherwise(col("converted_markdown"))
    ).withColumn(
        "status",
        # FIXED: Only set "Images Analyzed" for pages that had images
        when(col("has_image") == True, F.lit("Images Analyzed"))
        .otherwise(F.coalesce(col("status"), F.lit("No images")))
    ).withColumn(
        "timestamp",
        merge_udf(F.col("timestamp"))
    ).drop("has_image")  # FIXED: Drop temporary column

    main_logger.info("Writing output dataframe")
    md_output.write_dataframe(markdowns)
    main_logger.info("Transform completed successfully")

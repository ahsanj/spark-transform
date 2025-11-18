from transforms.api import transform, Output, Input, configure, incremental
from pyspark.sql import functions as F
from pyspark.sql.functions import when, col, udf, explode, struct, lit, broadcast
from pyspark.sql.types import StringType, ArrayType, StructType, StructField, IntegerType
from pyspark import StorageLevel

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

logger = logging.getLogger(__name__)

MAX_IMAGE_SIZE_MB = 5
MAX_IMAGE_SIZE_BYTES = MAX_IMAGE_SIZE_MB * 1024 * 1024
MAX_IMAGES_PER_PAGE = 10
MAX_RETRIES = 5
BASE_RETRY_DELAY = 10

IMAGE_PATTERN = re.compile(r'!\[([^\]]*)\]\(data:image/(jpeg|png);base64,([^)]+)\)')

VISION_PROMPT = """
FIRST : Check if photo is a logo, find out the company name and output "Logo" and skip the following instructions.
SECOND : Check if photo is a header/banner, if it is give a short description of the visual and skip the following instructions.
THIRD : Check if photo is a person/place/object/animal, if it is just give a short description of the photo and skip the following instructions.
THEN : You are an elite Macroeconomic Research Analyst, given the chart and the legends extract a table representing the information at all data points. Give output in the format of a markdown table. This table should be able to output the chart exactly later on. Omit any niceties, directly output the table.
Provide analysis on quantitative data shown in the chart, specify values shown while giving the detailed analysis.
Only use "\n" to write new lines, dont use "\n\n"
At the end add "Warning : These values have been estimated from a chart, make sure to verify before use"
"""


@configure(profile=[
    "EXECUTOR_MEMORY_LARGE",
    "EXECUTOR_MEMORY_OFFHEAP_LARGE",
    "DRIVER_MEMORY_LARGE",
    "NUM_EXECUTORS_8",
])
@incremental(v2_semantics=True)
@transform(
    mds=Input("ri.foundry.main.dataset.e5c73bda-e6b4-4474-a2aa-59cc6d4c0015"),
    md_output=Output("/GI-DEV-SPACE-4ecd2f/DEV-UC-GIN-1219-Workflow/Transform_pipeline/Useful Datasets/images_analysis_03"),
    model=GenericVisionCompletionLanguageModelInput("ri.language-model-service..language-model.anthropic-claude-4-sonnet")
)
def compute(ctx, mds, model, md_output):
    """
    Parallel image analysis matching original sequential logic:
    - Process ALL rows
    - Only update converted_markdown when has_image=True
    - Update status and timestamp for ALL rows
    """
    logger.info("=== Starting Parallel Image Analysis Transform ===")
    start_time = time.time()

    markdowns = mds.dataframe()

    # Validate has_image column exists
    if "has_image" not in markdowns.columns:
        logger.warning("has_image column not found, computing it...")
        markdowns = markdowns.withColumn(
            "has_image",
            col("converted_markdown").rlike(r'!\[([^\]]*)\]\(data:image/(jpeg|png);base64,([^)]+)\)')
        )

    pages_with_images = markdowns.filter(col("has_image"))
    images_count = pages_with_images.count()

    logger.info(f"Found {images_count} pages with has_image=True")

    if images_count == 0:
        logger.info("No images to process, updating metadata only")
        result_df = apply_metadata_updates(markdowns)
        md_output.write_dataframe(result_df)

        total_time = time.time() - start_time
        logger.info(f"=== Transform completed in {total_time:.2f}s ===")
        return

    logger.info("Step 1: Extracting images to rows...")
    images_df = extract_images_to_rows(pages_with_images)
    images_df = images_df.persist(StorageLevel.MEMORY_AND_DISK)

    total_images = images_df.count()
    logger.info(f"Extracted {total_images} images")

    logger.info("Step 2: Analyzing images in parallel...")
    analyzed_images = analyze_images_parallel(images_df, model)

    images_df.unpersist()

    logger.info("Step 3: Reconstructing markdown...")
    result_df = reconstruct_markdown(markdowns, analyzed_images)

    logger.info("Step 4: Applying metadata updates to all rows...")
    result_df = apply_metadata_updates(result_df)

    logger.info("Writing processed records to output")
    md_output.write_dataframe(result_df)

    total_time = time.time() - start_time
    logger.info(f"=== Transform completed in {total_time:.2f}s ===")


def extract_images_to_rows(df_with_images):
    """Extract base64 images from markdown and create one row per image."""

    def extract_images_udf(markdown_text):
        import logging
        logger = logging.getLogger(__name__)

        if not markdown_text or len(markdown_text.strip()) == 0:
            return []

        try:
            # Use finditer to get full match objects (not just groups)
            matches = list(IMAGE_PATTERN.finditer(markdown_text))
            if not matches:
                return []

            matches = matches[:MAX_IMAGES_PER_PAGE]

            result = []
            for idx, match in enumerate(matches):
                explanation = match.group(1)
                image_type = match.group(2)
                base64_data = match.group(3)

                estimated_size = len(base64_data) * 0.75
                if estimated_size > MAX_IMAGE_SIZE_BYTES:
                    logger.warning(f"Skipping large image (estimated {estimated_size/1024/1024:.1f}MB)")
                    continue

                result.append({
                    "image_index": idx,
                    "image_base64": base64_data,
                    "image_type": image_type,
                    "original_match": match.group(0)  # Capture EXACT original string!
                })

            return result
        except Exception as e:
            logger.error(f"Error extracting images: {e}")
            return []

    schema = ArrayType(StructType([
        StructField("image_index", IntegerType(), False),
        StructField("image_base64", StringType(), False),
        StructField("image_type", StringType(), False),
        StructField("original_match", StringType(), False)
    ]))

    extract_udf = udf(extract_images_udf, schema)

    df = df_with_images.withColumn(
        "page_id",
        F.concat_ws("::", col("originalMediaItemRid"), col("pageNumber").cast("string"))
    ).withColumn(
        "extracted_images",
        extract_udf(col("converted_markdown"))
    )

    images_df = (
        df.filter(F.size(col("extracted_images")) > 0)
        .select(col("page_id"), explode(col("extracted_images")).alias("img"))
        .select(
            col("page_id"),
            col("img.image_index").alias("image_index"),
            col("img.image_base64").alias("image_base64"),
            col("img.image_type").alias("image_type"),
            col("img.original_match").alias("original_match")
        )
    )

    return images_df


def analyze_images_parallel(images_df, model):
    """Analyze each image with vision LLM (Spark parallelizes automatically)."""

    def analyze_image(image_base64, image_type):
        import logging
        logger = logging.getLogger(__name__)

        if not image_base64:
            return "Error: Empty image"

        try:
            mime_type = MimeType.IMAGE_JPEG if image_type == "jpeg" else MimeType.IMAGE_PNG

            for attempt in range(MAX_RETRIES):
                try:
                    request = GenericVisionCompletionRequest(
                        [GenericMessage(
                            contents=[
                                GenericMessageContent(text=VISION_PROMPT),
                                GenericMessageContent(
                                    generic_media=GenericMediaContent(
                                        content=image_base64,
                                        mime_type=mime_type
                                    )
                                )
                            ],
                            role=ChatMessageRole.USER
                        )],
                        max_tokens=5000,
                        temperature=0
                    )
                    response = model.create_vision_completion(request).completion
                    return response

                except Exception as e:
                    if attempt == MAX_RETRIES - 1:
                        logger.error(f"Failed after {MAX_RETRIES} attempts: {e}")
                        return f"Error: {str(e)[:100]}"

                    delay = (2 ** attempt) * BASE_RETRY_DELAY + random.uniform(0, 5)
                    logger.warning(f"Attempt {attempt + 1} failed: {e}. Retrying in {delay:.2f}s")
                    time.sleep(delay)

        except Exception as e:
            logger.error(f"Error processing image: {e}")
            return f"Error: {str(e)[:100]}"

    analyze_udf = udf(analyze_image, StringType())

    return images_df.withColumn(
        "analysis",
        analyze_udf(col("image_base64"), col("image_type"))
    ).drop("image_base64")


def reconstruct_markdown(all_markdowns, analyzed_images):
    """Join analysis results back to original markdown and replace images with analysis."""

    all_markdowns = all_markdowns.withColumn(
        "page_id",
        F.concat_ws("::", col("originalMediaItemRid"), col("pageNumber").cast("string"))
    )

    images_by_page = (
        analyzed_images
        .groupBy("page_id")
        .agg(
            F.collect_list(
                struct(
                    col("original_match"),
                    col("analysis"),
                    col("image_index")
                )
            ).alias("image_replacements")
        )
    )

    joined = all_markdowns.join(
        broadcast(images_by_page),
        on="page_id",
        how="left"
    )

    def replace_images(markdown_text, replacements):
        import logging
        logger = logging.getLogger(__name__)

        if not replacements or not markdown_text:
            return markdown_text

        try:
            result = markdown_text
            sorted_replacements = sorted(replacements, key=lambda x: x.image_index)

            for replacement in sorted_replacements:
                original = replacement.original_match
                analysis = replacement.analysis
                replacement_text = f"\n<!-- Picture description:{analysis}-->\n"
                result = result.replace(original, replacement_text, 1)

            return result
        except Exception as e:
            logger.error(f"Error replacing images: {e}")
            return markdown_text

    replace_udf = udf(replace_images, StringType())

    result = joined.withColumn(
        "converted_markdown",
        when(
            col("image_replacements").isNotNull(),
            replace_udf(col("converted_markdown"), col("image_replacements"))
        ).otherwise(col("converted_markdown"))
    ).drop("page_id", "image_replacements")

    return result


def apply_metadata_updates(df):
    """Apply status and timestamp updates to all rows."""
    if "timestamp" not in df.columns:
        df = df.withColumn("timestamp", lit("{}"))

    current_timestamp = datetime.now().isoformat()
    current_stage = "image analysis completed"

    def merge_timestamp(existing_timestamp: str) -> str:
        try:
            if existing_timestamp and existing_timestamp.strip():
                timestamp_dict = json.loads(existing_timestamp)
            else:
                timestamp_dict = {}

            timestamp_dict[current_stage] = current_timestamp
            return json.dumps(timestamp_dict)
        except Exception:
            return json.dumps({current_stage: current_timestamp})

    merge_udf = udf(merge_timestamp, StringType())

    df = df.withColumn(
        "status",
        lit("Images Analyzed")  # Set for ALL rows, matching original behavior
    ).withColumn(
        "timestamp",
        merge_udf(col("timestamp"))
    )

    return df

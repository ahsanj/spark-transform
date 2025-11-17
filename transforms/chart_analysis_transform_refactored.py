"""
Refactored Chart Analysis Transform
====================================

This transform processes images embedded in markdown documents using a vision LLM.
Key improvements over the original:
- Exploded images to separate rows for better Spark parallelism
- Removed ThreadPoolExecutor (nested parallelism anti-pattern)
- Fixed critical issues: has_image column, timestamp tracking
- Proper exponential backoff retry logic (10s, 20s, 40s, 80s, 160s)
- Optimized memory usage: dropped converted_markdown from exploded rows
- Broadcast join for efficient reconstruction
- Persistence for intermediate results to avoid recomputation
- Logger initialization inside UDFs for proper serialization
- Safe single-occurrence replacement to avoid duplicate image issues
"""

from transforms.api import transform, Output, Input, configure, incremental
from pyspark.sql import functions as F
from pyspark.sql.functions import when, col, udf, explode, array, struct, lit, broadcast
from pyspark.sql.types import (
    StringType, ArrayType, StructType, StructField,
    IntegerType, BooleanType
)
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

# Configure logging
logger = logging.getLogger(__name__)

# Constants
MAX_IMAGE_SIZE_MB = 5
MAX_IMAGE_SIZE_BYTES = MAX_IMAGE_SIZE_MB * 1024 * 1024
MAX_IMAGES_PER_PAGE = 10
MAX_RETRIES = 5
BASE_RETRY_DELAY = 10

# Compile regex pattern once (not on every UDF call)
IMAGE_PATTERN = re.compile(r'!\[([^\]]*)\]\(data:image/(jpeg|png);base64,([^)]+)\)')

# Vision LLM prompt
VISION_PROMPT = """
FIRST : Check if photo is a logo, find out the company name and output "Logo of {Company Name}" and skip the following instructions.
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
    "EXECUTOR_CORES_MEDIUM"
])
@incremental(v2_semantics=True)
@transform(
    mds=Input("ri.foundry.main.dataset.31e5c2b6-4d63-480e-b2e4-c6124990fbcb"),
    md_output=Output("/GI-DEV-SPACE-4ecd2f/DEV-UC-GIN-1226-Workflow/backing_data/v3/[UC-GIN-1226] pdf_analyzed_uploaded"),
    model=GenericVisionCompletionLanguageModelInput("ri.language-model-service..language-model.anthropic-claude-4-sonnet")
)
def compute(ctx, mds, model, md_output):
    """
    Main transform function - refactored with explode-based architecture
    """
    logger.info("=== Starting Chart Analysis Transform (Refactored) ===")
    start_time = time.time()

    # Load input data
    markdowns = mds.dataframe()

    # STEP 1: Extract images to separate rows
    logger.info("Step 1: Extracting images from markdown documents...")
    images_df = extract_images_to_rows(markdowns)

    # Persist extracted images to avoid recomputation (triggers count() action)
    images_df = images_df.persist(StorageLevel.MEMORY_AND_DISK)

    total_images = images_df.count()
    logger.info(f"Extracted {total_images} images from markdown documents")

    if total_images == 0:
        logger.info("No images to process, passing through original markdown")
        result_df = markdowns.select(
            "originalMediaItemRid",
            "originalMediaReference",
            "originalPath",
            "pageNumber",
            col("converted_markdown").alias("markdown")
        )
        md_output.write_dataframe(result_df)
        return

    # STEP 2: Process images (Spark handles parallelism)
    logger.info("Step 2: Processing images with vision LLM...")
    analyzed_images_df = process_images_with_llm(images_df, model)

    # Unpersist images_df after processing to free memory
    images_df.unpersist()

    # STEP 3: Join results back and reconstruct markdown
    logger.info("Step 3: Reconstructing markdown with image analysis...")
    final_df = reconstruct_markdown_with_analysis(markdowns, analyzed_images_df)

    # STEP 4: Add timestamp tracking and select output columns
    logger.info("Step 4: Adding timestamp tracking and finalizing output...")
    final_df = add_timestamp_tracking(final_df)

    # Select final output columns
    output_df = final_df.select(
        "originalMediaItemRid",
        "originalMediaReference",
        "originalPath",
        "pageNumber",
        "markdown"
    )

    # Write output
    logger.info(f"Writing {output_df.count()} processed records to output")
    md_output.write_dataframe(output_df)

    total_time = time.time() - start_time
    logger.info(f"=== Transform completed in {total_time:.2f}s ===")


def extract_images_to_rows(markdowns_df):
    """
    Extract all images from markdown documents and create one row per image.

    Returns DataFrame with columns:
    - page_id: unique identifier for the page
    - image_index: index of the image within the page
    - image_base64: the base64 image data
    - image_type: jpeg or png
    - original_match: the original markdown image syntax
    - (all original columns preserved)
    """

    def extract_images_udf(markdown_text):
        """Extract all images from markdown and return as array of structs"""
        import logging
        logger = logging.getLogger(__name__)

        if not markdown_text or len(markdown_text.strip()) == 0:
            return []

        try:
            matches = IMAGE_PATTERN.findall(markdown_text)

            if not matches:
                return []

            # Limit to prevent processing too many images per page
            matches = matches[:MAX_IMAGES_PER_PAGE]

            result = []
            for idx, (explanation, image_type, base64_data) in enumerate(matches):
                # Validate image size
                estimated_size = len(base64_data) * 0.75

                if estimated_size > MAX_IMAGE_SIZE_BYTES:
                    logger.warning(f"Skipping large image (estimated {estimated_size/1024/1024:.1f}MB)")
                    continue

                # Skip extremely large base64 strings (>50MB)
                if len(base64_data) > 50 * 1024 * 1024:
                    logger.warning(f"Skipping extremely large image ({len(base64_data)} bytes)")
                    continue

                # Create the original match string for later replacement
                original_match = f"![{explanation}](data:image/{image_type};base64,{base64_data})"

                result.append({
                    "image_index": idx,
                    "image_base64": base64_data,
                    "image_type": image_type,
                    "original_match": original_match,
                    "explanation": explanation
                })

            return result

        except Exception as e:
            logger.error(f"Error extracting images: {e}")
            return []

    # Define schema for extracted images
    image_struct_schema = ArrayType(StructType([
        StructField("image_index", IntegerType(), False),
        StructField("image_base64", StringType(), False),
        StructField("image_type", StringType(), False),
        StructField("original_match", StringType(), False),
        StructField("explanation", StringType(), True)
    ]))

    extract_udf = udf(extract_images_udf, image_struct_schema)

    # Create unique page ID
    markdowns_with_images = markdowns_df.withColumn(
        "page_id",
        F.concat_ws("::", col("originalMediaItemRid"), col("pageNumber").cast("string"))
    )

    # Extract images and detect if page has images
    markdowns_with_images = markdowns_with_images.withColumn(
        "extracted_images",
        extract_udf(col("converted_markdown"))
    )

    # Add has_image flag (FIX: this was missing in original code)
    markdowns_with_images = markdowns_with_images.withColumn(
        "has_image",
        F.size(col("extracted_images")) > 0
    )

    # Filter to only pages with images and explode
    # NOTE: We don't include converted_markdown here to avoid duplicating large text for each image
    images_df = (
        markdowns_with_images
        .filter(col("has_image"))
        .select(
            col("page_id"),
            col("originalMediaItemRid"),
            col("originalMediaReference"),
            col("originalPath"),
            col("pageNumber"),
            explode(col("extracted_images")).alias("image_data")
        )
        .select(
            col("page_id"),
            col("originalMediaItemRid"),
            col("originalMediaReference"),
            col("originalPath"),
            col("pageNumber"),
            col("image_data.image_index").alias("image_index"),
            col("image_data.image_base64").alias("image_base64"),
            col("image_data.image_type").alias("image_type"),
            col("image_data.original_match").alias("original_match"),
            col("image_data.explanation").alias("explanation")
        )
    )

    return images_df


def process_images_with_llm(images_df, model):
    """
    Process each image with the vision LLM.
    Spark naturally parallelizes this across executors.
    No ThreadPoolExecutor needed!
    """

    def analyze_image_udf(image_base64, image_type):
        """
        Analyze a single image using the vision LLM.
        This runs once per image, distributed by Spark.
        """
        import logging
        logger = logging.getLogger(__name__)

        if not image_base64:
            return "Error: Empty image data"

        try:
            # Determine MIME type from regex-captured image_type
            mime_type = MimeType.IMAGE_JPEG if image_type == "jpeg" else MimeType.IMAGE_PNG

            # Retry logic with exponential backoff
            for attempt in range(MAX_RETRIES):
                try:
                    # Create LLM request
                    prompt_content = GenericMessageContent(text=VISION_PROMPT)
                    media_content = GenericMessageContent(
                        generic_media=GenericMediaContent(
                            content=image_base64,
                            mime_type=mime_type
                        )
                    )

                    request = GenericVisionCompletionRequest(
                        [GenericMessage(
                            contents=[prompt_content, media_content],
                            role=ChatMessageRole.USER
                        )],
                        max_tokens=5000,
                        temperature=0
                    )

                    response = model.create_vision_completion(request).completion

                    return response

                except Exception as e:
                    if attempt == MAX_RETRIES - 1:
                        error_msg = f"Error after {MAX_RETRIES} retries: {str(e)}"
                        logger.error(error_msg)
                        return f"Error processing image: {str(e)[:100]}"

                    # Exponential backoff with jitter: 10s, 20s, 40s, 80s, 160s
                    delay = (2 ** attempt) * BASE_RETRY_DELAY + random.uniform(0, 5)

                    logger.warning(f"Attempt {attempt + 1} failed: {e}")
                    logger.warning(f"Retrying in {delay:.2f} seconds...")
                    time.sleep(delay)

        except Exception as e:
            error_msg = f"Error processing image: {str(e)}"
            logger.error(error_msg)
            return error_msg

    analyze_udf = udf(analyze_image_udf, StringType())

    # Process images - Spark distributes this automatically
    analyzed_df = images_df.withColumn(
        "analysis",
        analyze_udf(col("image_base64"), col("image_type"))
    )

    # Add processing status for error tracking
    analyzed_df = analyzed_df.withColumn(
        "processing_status",
        when(col("analysis").startswith("Error"), "failed")
        .otherwise("success")
    )

    # Drop the large base64 column to save memory
    analyzed_df = analyzed_df.drop("image_base64")

    return analyzed_df


def reconstruct_markdown_with_analysis(markdowns_df, analyzed_images_df):
    """
    Join analysis results back to original markdown and replace images with analysis.
    """

    # Create page_id in original dataframe
    markdowns_with_id = markdowns_df.withColumn(
        "page_id",
        F.concat_ws("::", col("originalMediaItemRid"), col("pageNumber").cast("string"))
    )

    # Group analyzed images by page_id
    images_by_page = (
        analyzed_images_df
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

    # Join back to original markdown (broadcast small images_by_page for efficiency)
    joined_df = markdowns_with_id.join(
        broadcast(images_by_page),
        on="page_id",
        how="left"
    )

    # UDF to replace images with analysis
    def replace_images_udf(markdown_text, replacements):
        """Replace all image references with their analysis"""
        import logging
        logger = logging.getLogger(__name__)

        if not replacements or not markdown_text:
            return markdown_text

        try:
            result = markdown_text

            # Sort by image_index to ensure consistent replacement order
            sorted_replacements = sorted(replacements, key=lambda x: x.image_index)

            for replacement in sorted_replacements:
                original = replacement.original_match
                analysis = replacement.analysis

                # Create replacement text
                replacement_text = f"\n<!-- Picture description: {analysis} -->\n"

                # Replace only the first occurrence to avoid issues with duplicate images
                result = result.replace(original, replacement_text, 1)

            return result

        except Exception as e:
            logger.error(f"Error replacing images: {e}")
            return markdown_text

    # Define schema for replacement struct
    replacement_schema = ArrayType(StructType([
        StructField("original_match", StringType(), True),
        StructField("analysis", StringType(), True),
        StructField("image_index", IntegerType(), True)
    ]))

    replace_udf = udf(replace_images_udf, StringType())

    # Apply replacement
    result_df = joined_df.withColumn(
        "markdown",
        when(
            col("image_replacements").isNotNull(),
            replace_udf(col("converted_markdown"), col("image_replacements"))
        ).otherwise(col("converted_markdown"))
    )

    return result_df


def add_timestamp_tracking(df):
    """
    Add timestamp tracking for this processing stage.
    FIX: This was defined but never applied in the original code!
    """
    # Add timestamp column if it doesn't exist
    if "timestamp" not in df.columns:
        df = df.withColumn("timestamp", lit("{}"))

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
        except Exception as e:
            # If parsing fails, create new timestamp with just the new stage
            logger.warning(f"Error parsing timestamp: {e}, creating new")
            return json.dumps({current_stage: current_timestamp})

    merge_udf = udf(merge_timestamp_udf, StringType())

    # Apply timestamp UDF (this was missing in original!)
    df = df.withColumn(
        "timestamp",
        merge_udf(col("timestamp"))
    )

    logger.info(f"Added timestamp for stage '{current_stage}' at {current_timestamp}")

    return df

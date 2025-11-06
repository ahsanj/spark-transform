from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructType, StructField, BooleanType, IntegerType, DoubleType
from pyspark.sql.functions import udf
from transforms.api import Input, Output, transform_df, configure, incremental
from transforms.external.systems import external_systems, Source, ResolvedSource
import requests
import json
import time
import logging
import random
import traceback
import math
from datetime import datetime
from sc_v2_dev_sdk import FoundryClient, UserTokenAuth
from sc_v2_dev_sdk.ontology.objects import ScV2Document
from typing import Optional, Iterator

from myproject.datasets import utils
from myproject.datasets.config import (
    API_TIMEOUT,
    CLIENT_TIMEOUT,
    MIN_ENCODING_LENGTH,
    MAX_PARTITIONS,
    LARGE_DATASET_THRESHOLD,
    INCLUDE_IMAGES,
    INCLUDE_OCR
)

logger = logging.getLogger(__name__)

# Constants
MAX_ENCODING_SIZE = 50_000_000
WORD_COUNT_LARGE_THRESHOLD = 1_000_000
WORD_COUNT_SAMPLE_SIZE = 100_000

_session = None
_token_data = {"token": None, "last_refresh": 0}


def _create_error_result(message: str) -> dict:
    """Helper function to create consistent error result dictionaries."""
    return {
        "content": message,
        "layout_score": None,
        "parse_score": None,
        "ocr_score": None,
        "table_score": None
    }


def _safe_convert_score(score):
    """Safely convert confidence scores, handling NaN values."""
    if score is None:
        return None
    if isinstance(score, float):
        if math.isnan(score) or math.isinf(score):
            return None
        return score
    if isinstance(score, str) and score.lower() in ('nan', 'inf', '-inf'):
        return None
    try:
        float_val = float(score)
        if math.isnan(float_val) or math.isinf(float_val):
            return None
        return float_val
    except (ValueError, TypeError):
        return None


# USE THIS FOR BLACKLINE CHECK
def scv2_getblacklinestatus(path: str, docling_connection=None) -> Optional[bool]:
    """
    Given a document path, return the is_blackline property of the ScV2Document with that path.
    Returns None if not found.
    """
    try:
        client = FoundryClient(
            hostname="aisuite.palantirfoundry.com",
            auth=UserTokenAuth(_get_fresh_token_if_needed(docling_connection))
        )
        return client.ontology.queries.sc_v2_get_blackline_status(path=path)
    except Exception as e:
        # default to non-blackline if the Client could not be instantiated
        error_msg = f"ERROR: Failed to product Foundry Client - {str(e)}"
        logger.error(error_msg)
        return None


def _get_session():
    global _session
    if _session is None:
        _session = requests.Session()
        _session.timeout = CLIENT_TIMEOUT
    return _session


def _get_fresh_token_if_needed(docling_connection=None):
    REFRESH_RATE_IN_S = 1800
    current_time = time.time()
    if (current_time - _token_data["last_refresh"]) >= REFRESH_RATE_IN_S:
        if docling_connection is None:
            logger.error("No docling connection available for token refresh")
            return None
        logger.info(f"Token refresh needed (>{REFRESH_RATE_IN_S / 3600} hour since last refresh)")
        new_token = get_auth_token_from_secrets(docling_connection)
        if new_token:
            _token_data["token"] = new_token
            _token_data["last_refresh"] = current_time
            logger.info("Token refreshed successfully")
        else:
            logger.error("Failed to refresh token")
    return _token_data["token"]


def construct_docling_http_parameters(encoding: str, page_number: int | None) -> dict[str, dict]:
    payload = {
        "parameters": {
            "pdf_encoding": encoding,
            "include_images": INCLUDE_IMAGES,
            "include_ocr": INCLUDE_OCR
        }
    }

    if page_number is not None:
        payload["parameters"]["page_number"] = page_number

    return payload


def construct_blackline_http_parameters(encoding) -> dict[str, dict]:
    payload = {
        "parameters": {
            "page_image_base64_encoding": encoding,
        }
    }

    return payload


def docling_convert_pdf(
    encoding: str,
    page_number: int | None = None,
    auth_token: str | None = None,
    base_url: str | None = None,
    is_blackline: str | None = None,
    docling_connection=None
) -> dict:
    api_start_time = time.time()
    processing_mode_str = "Blackline" if is_blackline else "Docling"

    try:
        if not encoding or encoding.strip() == "":
            logger.warning("Empty or null encoding provided")
            return _create_error_result("ERROR: Empty or null encoding provided")

        encoding_size = len(encoding)
        if encoding_size > MAX_ENCODING_SIZE:
            return _create_error_result("ERROR: Encoding exceeds size limit")

        # Get fresh token if needed
        fresh_token = _get_fresh_token_if_needed(docling_connection)
        if fresh_token:
            auth_token = fresh_token

        if not auth_token or not base_url:
            logger.error("Missing authentication token or base URL")
            return _create_error_result("ERROR: Missing authentication configuration")

        if logger.isEnabledFor(logging.DEBUG) and random.random() < 0.001:
            logger.debug(f"Sample API call: encoding_len={encoding_size}, page={page_number}")

        client = _get_session()

        payload = (construct_blackline_http_parameters(encoding) if is_blackline
                  else construct_docling_http_parameters(encoding, page_number))
        query_api_name = "transcribeGiScBlacklineToMarkdown" if is_blackline else "singlePageDocling"

        full_url = f"{base_url}/api/v2/ontologies/ontology-e5bf1f53-8923-47b9-8391-14b13acd038b/queries/{query_api_name}/execute"

        headers = {
            "Authorization": f"Bearer {auth_token}",
            "Content-Type": "application/json"
        }

        response = client.post(full_url, headers=headers, json=payload, timeout=API_TIMEOUT)

        # Handle 401 authentication error - force refresh token and retry
        if response.status_code == 401:
            logger.warning("Received 401 error, forcing token refresh")
            # Force refresh by setting timestamp to 0
            _token_data["last_refresh"] = 0
            refreshed_token = _get_fresh_token_if_needed(docling_connection)
            if refreshed_token and refreshed_token != auth_token:
                logger.info("Retrying request with refreshed token")
                headers["Authorization"] = f"Bearer {refreshed_token}"
                response = client.post(full_url, headers=headers, json=payload, timeout=API_TIMEOUT)

        if not response.ok:
            return _create_error_result(f"ERROR: {processing_mode_str} API failed {response.status_code}")

        # At this point response.ok is True, so we can process the response
        result = response.json()
        if "value" in result:
            try:
                # Parse the JSON string in the value field
                value_data = json.loads(result["value"])
                if "content" in value_data:
                    content = value_data["content"]
                    confidence = value_data.get("confidence", {})

                    # Extract and safely convert confidence scores
                    layout_score = _safe_convert_score(confidence.get("layout_score"))
                    parse_score = _safe_convert_score(confidence.get("parse_score"))
                    ocr_score = _safe_convert_score(confidence.get("ocr_score"))
                    table_score = _safe_convert_score(confidence.get("table_score"))

                    result_length = len(content) if content else 0
                    logger.info(f"Successful conversion, content length: {result_length}")
                    return {
                        "content": content,
                        "layout_score": layout_score,
                        "parse_score": parse_score,
                        "ocr_score": ocr_score,
                        "table_score": table_score
                    }
                else:
                    error_msg = f"ERROR: No 'content' field in value: {value_data}"
                    logger.error(error_msg)
                    return _create_error_result(error_msg)
            except json.JSONDecodeError as json_err:
                # If value is not JSON, return it as is (fallback)
                result_length = len(result["value"]) if result["value"] else 0
                logger.warning(f"Value is not JSON, returning raw value. Length: {result_length}")
                return {
                    "content": result["value"],
                    "layout_score": None,
                    "parse_score": None,
                    "ocr_score": None,
                    "table_score": None
                }
        else:
            error_msg = f"ERROR: No 'value' field in response: {result}"
            logger.error(error_msg)
            return _create_error_result(error_msg)

    except ImportError as e:
        error_msg = f"ERROR: Import error - {str(e)}"
        logger.error(error_msg)
        return _create_error_result(error_msg)
    except requests.exceptions.RequestException as e:
        api_duration = time.time() - api_start_time
        error_msg = f"ERROR: Network error - {str(e)}"
        logger.error(f"{error_msg} after {api_duration:.2f}s")
        return _create_error_result(error_msg)
    except json.JSONDecodeError as e:
        api_duration = time.time() - api_start_time
        error_msg = f"ERROR: JSON decode error - {str(e)}"
        logger.error(f"{error_msg} after {api_duration:.2f}s")
        return _create_error_result(error_msg)
    except Exception as e:
        api_duration = time.time() - api_start_time
        error_msg = f"ERROR: Unexpected error - {str(e)}\nTraceback: {traceback.format_exc()}"
        logger.error(f"{error_msg} after {api_duration:.2f}s")
        return _create_error_result(error_msg)
    # NOTE: Removed finally block that was closing singleton session - this was causing connection issues


def get_auth_token_from_secrets(docling_connection) -> str:
    oauth_client = None

    try:
        logger.info("Starting OAuth2 token acquisition")

        base_url = docling_connection.get_https_connection().url
        client_id = None
        client_secret = None

        if hasattr(docling_connection, 'secrets'):
            secrets = docling_connection.secrets
            client_id = secrets.get('additionalSecretDoclingUserId')
            client_secret = secrets.get('additionalSecretDoclingUserSecret')

            if client_id and client_secret:
                logger.info("Found client credentials in resolved source secrets")

        if not client_id or not client_secret:
            logger.error("Unable to extract client credentials from external connection")
            return None

        token_url = "https://aisuite.palantirfoundry.com/multipass/api/oauth2/token"
        oauth_client = requests.Session()

        response = oauth_client.post(
            token_url,
            data={
                "grant_type": "client_credentials",
                "client_id": client_id,
                "client_secret": client_secret,
                "scope": "api:datasets-read api:datasets-write api:usage:ontologies-read api:usage:ontologies-write api:ontologies-read api:ontologies-write api:compass-read api:compass-write"
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=120
        )

        if response.ok:
            token_data = response.json()
            access_token = token_data.get("access_token")
            if access_token:
                logger.info("Successfully obtained OAuth2 access token")
                return access_token
            else:
                logger.error("No access_token in OAuth2 response")
                return None
        else:
            logger.error(f"OAuth2 token request failed: {response.status_code}")
            return None

    except Exception as e:
        logger.error(f"Failed to get auth token via OAuth2: {str(e)}")
        return None
    finally:
        if oauth_client:
            oauth_client.close()


def analyze_content_udf(markdown_content: str) -> dict:
    if not markdown_content or markdown_content.startswith("ERROR:"):
        return {"has_table": False, "has_image": False, "word_count": 0}

    has_table = "|--" in markdown_content and "--|" in markdown_content
    has_image = "[Image]" in markdown_content or "![" in markdown_content

    if len(markdown_content) > WORD_COUNT_LARGE_THRESHOLD:
        sample_size = min(WORD_COUNT_SAMPLE_SIZE, len(markdown_content))
        sample = markdown_content[:sample_size]
        sample_words = len(sample.split())
        word_count = int((sample_words / len(sample)) * len(markdown_content))
    else:
        word_count = len(markdown_content.split())

    return {
        "has_table": has_table,
        "has_image": has_image,
        "word_count": word_count
    }


@external_systems(
    doclingccapi=Source("ri.magritte..source.37b3405b-2390-47fd-a8cc-82cb96343d11")
)
@configure(
    profile=[
        "NUM_EXECUTORS_16",
        "EXECUTOR_MEMORY_LARGE",
        "DRIVER_MEMORY_MEDIUM",
    ]
)
@incremental(v2_semantics=True)
@transform_df(
    Output("ri.foundry.main.dataset.d1e12201-7d95-45ea-a09b-5cb05806e4ea"),
    source_df=Input("ri.foundry.main.dataset.0b7ccf02-b6c0-4b06-8812-9ce4d7bb4b74"),
)
def docling_transform(doclingccapi: ResolvedSource, source_df: DataFrame) -> DataFrame:
    transform_start_time = time.time()
    logger.info("Docling Transform Starting - Optimized Version")

    base_url = doclingccapi.get_https_connection().url
    logger.info(f"External connection base URL: {base_url}")

    # Get initial token (will refresh if needed)
    auth_token = _get_fresh_token_if_needed(doclingccapi)
    if not auth_token:
        logger.error("Step 1 FAILED: Could not obtain OAuth2 authentication token")
        return (source_df
                .withColumn("converted_markdown", F.lit("ERROR: OAuth2 authentication failed"))
                .withColumn("markdown_layout_score", F.lit(None).cast(DoubleType()))
                .withColumn("markdown_parse_score", F.lit(None).cast(DoubleType()))
                .withColumn("markdown_ocr_score", F.lit(None).cast(DoubleType()))
                .withColumn("markdown_table_score", F.lit(None).cast(DoubleType()))
                .withColumn("status", F.lit("OAuth2 authentication failed"))
                .withColumn("timestamp", F.lit(f'{{"markdown conversion failed": "{datetime.now().isoformat()}"}}'))
                .withColumn("has_table", F.lit(False))
                .withColumn("has_image", F.lit(False))
                .withColumn("word_count", F.lit(0))
                .select(
                    "primaryKey",
                    "originalMediaItemRid",
                    "originalPath",
                    "originalMediaReference",
                    "pageNumber",
                    "totalPages",
                    "pageBase64",
                    "pageImageBase64",
                    "status",
                    "timestamp",
                    "converted_markdown",
                    "markdown_layout_score",
                    "markdown_parse_score",
                    "markdown_ocr_score",
                    "markdown_table_score",
                    "has_table",
                    "has_image",
                    "word_count"
                ))

    logger.info("OAuth2 authentication successful")

    total_rows = source_df.count()
    logger.info(f"Processing {total_rows} rows")

    # Use the fixed column name 'pageBase64' for PDF encoding
    encoding_column = "pageBase64"
    logger.info(f"Using column '{encoding_column}' for PDF encoding")

    # Create UDF that accepts two parameters: encoding and page_number
    def docling_udf_func(encoding, page_number, is_blackline):
        # Handle null page_number from Spark
        page_num = page_number if page_number is not None else None
        is_blackline_param = is_blackline or None
        return docling_convert_pdf(encoding, page_num, auth_token, base_url, is_blackline_param, doclingccapi)

    # Define schema for the return type
    docling_schema = StructType([
        StructField("content", StringType(), True),
        StructField("layout_score", DoubleType(), True),
        StructField("parse_score", DoubleType(), True),
        StructField("ocr_score", DoubleType(), True),
        StructField("table_score", DoubleType(), True)
    ])

    docling_udf = udf(docling_udf_func, docling_schema)

    analysis_schema = StructType([
        StructField("has_table", BooleanType(), True),
        StructField("has_image", BooleanType(), True),
        StructField("word_count", IntegerType(), True)
    ])
    analyze_udf = udf(analyze_content_udf, analysis_schema)

    is_valid = (
        F.col("pageBase64").isNotNull() &
        (F.length(F.col("pageBase64")) > MIN_ENCODING_LENGTH)
    )

    page_number_col = F.col("pageNumber") if "pageNumber" in source_df.columns else F.lit(None)

    # Create UDF for blackline status check
    def get_blackline_status_udf(path: str) -> bool:
        result = scv2_getblacklinestatus(path, doclingccapi)
        return result if result is not None else False

    blackline_status_udf = udf(get_blackline_status_udf, BooleanType())

    # Create error result struct
    error_result = F.struct(
        F.lit("ERROR: Invalid or missing encoding").alias("content"),
        F.lit(None).cast(DoubleType()).alias("layout_score"),
        F.lit(None).cast(DoubleType()).alias("parse_score"),
        F.lit(None).cast(DoubleType()).alias("ocr_score"),
        F.lit(None).cast(DoubleType()).alias("table_score")
    )

    # FIX: Cache blackline status first to avoid duplicate API calls
    result_df = (source_df
        .withColumn("is_blackline", blackline_status_udf(F.col("originalMediaItemRid")))
        .withColumn("docling_result",
            F.when(is_valid,
                docling_udf(
                    F.when(F.col("is_blackline"), F.col("pageImageBase64"))
                        .otherwise(F.col("pageBase64")),
                    page_number_col,
                    F.col("is_blackline")
                )
            ).otherwise(error_result))
        .withColumn("converted_markdown", F.col("docling_result.content"))
        .withColumn("markdown_layout_score", F.col("docling_result.layout_score"))
        .withColumn("markdown_parse_score", F.col("docling_result.parse_score"))
        .withColumn("markdown_ocr_score", F.col("docling_result.ocr_score"))
        .withColumn("markdown_table_score", F.col("docling_result.table_score"))
        .withColumn("content_analysis",
            F.when(F.col("converted_markdown").startswith("ERROR:"),
                   F.struct(F.lit(False).alias("has_table"),
                           F.lit(False).alias("has_image"),
                           F.lit(0).alias("word_count")))
             .otherwise(analyze_udf(F.col("converted_markdown"))))
        .withColumn("has_table", F.col("content_analysis.has_table"))
        .withColumn("has_image", F.col("content_analysis.has_image"))
        .withColumn("word_count", F.col("content_analysis.word_count"))
        .withColumn("status", F.lit("markdown conversion completed"))
        .drop("content_analysis", "docling_result", "is_blackline"))
        # FIX: Removed .persist() which was causing OOM issues

    logger.info("Collecting metrics in single pass")

    metrics = result_df.agg(
        F.count("*").alias("processed_count"),
        F.sum(F.when(~F.col("converted_markdown").startswith("ERROR:"), 1).otherwise(0)).alias("success_count"),
        F.sum(F.when(F.col("has_table"), 1).otherwise(0)).alias("table_count"),
        F.sum(F.when(F.col("has_image"), 1).otherwise(0)).alias("image_count")
    ).collect()[0]

    processed_count = metrics["processed_count"]
    success_count = metrics["success_count"] or 0
    error_count = processed_count - success_count
    table_count = metrics["table_count"] or 0
    image_count = metrics["image_count"] or 0

    total_transform_time = time.time() - transform_start_time

    logger.info("Docling Transform Complete")
    logger.info(f"Total processing time: {total_transform_time:.2f}s")
    logger.info(f"Rows processed: {processed_count}")
    logger.info(f"Successful conversions: {success_count}")
    logger.info(f"Failed conversions: {error_count}")

    if processed_count > 0:
        success_rate = (success_count / processed_count) * 100
        logger.info(f"Success rate: {success_rate:.1f}%")
        logger.info(f"Pages with tables: {table_count}")
        logger.info(f"Pages with images: {image_count}")

    # Debug: Show sample timestamp values before update
    sample_timestamps = result_df.select("timestamp").limit(3).collect()
    for i, row in enumerate(sample_timestamps):
        logger.info(f"Sample timestamp {i+1}: {row['timestamp']}")

    # Add our processing timestamp - merge approach (preserves existing timestamps)
    current_timestamp = datetime.now().isoformat()
    current_stage = "markdown conversion completed"

    # Create UDF to merge timestamps
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
            # If parsing fails, create new timestamp with just the new stage
            return json.dumps({current_stage: current_timestamp})

    # Register UDF
    merge_udf = udf(merge_timestamp_udf, StringType())

    # Apply UDF to merge timestamps
    result_df = result_df.withColumn("timestamp", merge_udf(F.col("timestamp")))

    logger.info(f"Merged timestamp for stage '{current_stage}' at {current_timestamp}")

    # Reorder columns to match reference schema
    result_df = result_df.select(
        "primaryKey",
        "originalMediaItemRid",
        "originalPath",
        "originalMediaReference",
        "pageNumber",
        "totalPages",
        "pageBase64",
        "pageImageBase64",
        "status",
        "timestamp",
        "converted_markdown",
        "markdown_layout_score",
        "markdown_parse_score",
        "markdown_ocr_score",
        "markdown_table_score",
        "has_table",
        "has_image",
        "word_count"
    )

    return result_df

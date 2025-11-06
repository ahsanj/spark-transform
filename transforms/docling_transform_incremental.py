from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField,
    StringType, BooleanType, IntegerType, DoubleType,
    TimestampType, DateType,
)
from pyspark import StorageLevel
from transforms.api import Input, Output, transform, configure, incremental
from transforms.external.systems import external_systems, Source
import requests
import time, json, logging, random, traceback, math
from datetime import datetime, timedelta
from sc_v2_dev_sdk import FoundryClient, UserTokenAuth
from typing import Optional

logger = logging.getLogger(__name__)

MAX_RETRIES = 3
CIRCUIT_BREAKER_ERROR_THRESHOLD = 0.9
CIRCUIT_BREAKER_MIN_SAMPLES = 100
CIRCUIT_BREAKER_MIN_FAILURES = 100
MIN_ENCODING_LEN = 200
MAX_RIDS_TO_COLLECT = 50000
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


def df_is_empty(df: DataFrame) -> bool:
    return df.rdd.isEmpty()


def ensure_columns(df: DataFrame, schema: StructType) -> DataFrame:
    for field in schema.fields:
        if field.name not in df.columns:
            df = df.withColumn(field.name, F.lit(None).cast(field.dataType))
    return df.select([field.name for field in schema.fields])


def with_retry(callable_fn, *, max_retries=MAX_RETRIES, base_sleep=0.25):
    last_exc = None
    for attempt in range(max_retries):
        try:
            return callable_fn()
        except Exception as e:
            last_exc = e
            if attempt < max_retries - 1:
                sleep_for = base_sleep * (2 ** attempt) + random.random() * 0.1
                logger.warning(f"Retry attempt {attempt + 1}/{max_retries} after error: {str(e)[:100]}")
                time.sleep(sleep_for)
    logger.error(f"All {max_retries} retry attempts failed. Last error: {str(last_exc)[:200]}")
    raise last_exc


def classify_error(error_msg: str) -> str:
    if not error_msg or not isinstance(error_msg, str):
        return "UNKNOWN"

    error_lower = error_msg.lower()

    # Check for permanent failures first
    if "invalid or missing encoding" in error_lower:
        return "PERMANENT_INVALID_ENCODING"
    elif "encoding exceeds size limit" in error_lower:
        return "PERMANENT_SIZE_LIMIT"
    elif "timeout" in error_lower or "timed out" in error_lower:
        return "TIMEOUT"
    elif "429" in error_msg or "rate limit" in error_lower or "too many requests" in error_lower:
        return "RATE_LIMIT"
    elif any(code in error_msg for code in ["500", "502", "503", "504"]):
        return "SERVER_ERROR"
    elif "401" in error_msg or "403" in error_msg or "unauthorized" in error_lower or "forbidden" in error_lower:
        return "AUTH_ERROR"
    elif "connection" in error_lower or "network" in error_lower:
        return "NETWORK_ERROR"
    elif "400" in error_msg or "bad request" in error_lower:
        return "BAD_REQUEST"
    else:
        return "UNKNOWN"


def _get_session():
    global _session
    if _session is None:
        _session = requests.Session()
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


def scv2_getblacklinestatus(path: str, docling_connection=None) -> Optional[bool]:
    try:
        client = FoundryClient(
            hostname="aisuite.palantirfoundry.com",
            auth=UserTokenAuth(_get_fresh_token_if_needed(docling_connection))
        )
        return client.ontology.queries.sc_v2_get_blackline_status(path=path)
    except Exception as e:
        error_msg = f"ERROR: Failed to produce Foundry Client - {str(e)}"
        logger.error(error_msg)
        return None


def construct_docling_http_parameters(encoding: str, page_number: int | None) -> dict:
    INCLUDE_IMAGES = True
    INCLUDE_OCR = True

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


def construct_blackline_http_parameters(encoding: str) -> dict:
    payload = {
        "parameters": {
            "page_image_base64_encoding": encoding,
        }
    }
    return payload


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


def docling_convert_pdf(encoding: str, page_number: int | None = None, auth_token: str | None = None,
                        base_url: str | None = None, is_blackline: bool | None = None,
                        docling_connection=None) -> dict:
    API_TIMEOUT = 120
    api_start_time = time.time()
    processing_mode_str = "Blackline" if is_blackline else "Docling"

    try:
        if not encoding or encoding.strip() == "":
            logger.warning("Empty or null encoding provided")
            return _create_error_result("ERROR: Empty or null encoding provided")

        encoding_size = len(encoding)
        if encoding_size > MAX_ENCODING_SIZE:
            return _create_error_result("ERROR: Encoding exceeds size limit")

        fresh_token = _get_fresh_token_if_needed(docling_connection)
        if fresh_token:
            auth_token = fresh_token

        if not auth_token or not base_url:
            logger.error("Missing authentication token or base URL")
            return _create_error_result("ERROR: Missing authentication configuration")

        if logger.isEnabledFor(logging.DEBUG) and random.random() < 0.001:
            logger.debug(f"Sample API call: encoding_len={encoding_size}, page={page_number}")

        client = _get_session()

        payload = construct_blackline_http_parameters(encoding) if is_blackline else construct_docling_http_parameters(encoding, page_number)
        query_api_name = "transcribeGiScBlacklineToMarkdown" if is_blackline else "singlePageDocling"

        full_url = f"{base_url}/api/v2/ontologies/ontology-e5bf1f53-8923-47b9-8391-14b13acd038b/queries/{query_api_name}/execute"

        headers = {
            "Authorization": f"Bearer {auth_token}",
            "Content-Type": "application/json"
        }

        response = client.post(full_url, headers=headers, json=payload, timeout=API_TIMEOUT)

        if response.status_code == 401:
            logger.warning("Received 401 error, forcing token refresh")
            _token_data["last_refresh"] = 0
            refreshed_token = _get_fresh_token_if_needed(docling_connection)
            if refreshed_token and refreshed_token != auth_token:
                logger.info("Retrying request with refreshed token")
                headers["Authorization"] = f"Bearer {refreshed_token}"
                response = client.post(full_url, headers=headers, json=payload, timeout=API_TIMEOUT)

        if not response.ok:
            body = response.text[:500] if hasattr(response, 'text') and response.text else ""
            return _create_error_result(f"ERROR: {processing_mode_str} API failed {response.status_code}: {body}")

        result = response.json()
        if "value" in result:
            try:
                value_data = json.loads(result["value"])
                if "content" in value_data:
                    content = value_data["content"]
                    confidence = value_data.get("confidence", {})

                    # Use safe conversion for all scores
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
            except json.JSONDecodeError:
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


def _docling_wrapper(encoding, page_number, auth_token, base_url, is_blackline, conn):
    def _one():
        return docling_convert_pdf(
            encoding=encoding,
            page_number=page_number,
            auth_token=auth_token,
            base_url=base_url,
            is_blackline=is_blackline,
            docling_connection=conn
        )
    return with_retry(_one)


DOC_SCHEMA = StructType([
    StructField("content", StringType(), True),
    StructField("layout_score", DoubleType(), True),
    StructField("parse_score", DoubleType(), True),
    StructField("ocr_score", DoubleType(), True),
    StructField("table_score", DoubleType(), True)
])

ANALYSIS_SCHEMA = StructType([
    StructField("has_table", BooleanType(), True),
    StructField("has_image", BooleanType(), True),
    StructField("word_count", IntegerType(), True)
])

ERROR_SCHEMA = StructType([
    StructField("record_id", StringType(), False),
    StructField("content_hash", StringType(), False),
    StructField("attempt_count", IntegerType(), False),
    StructField("last_error", StringType(), True),
    StructField("error_category", StringType(), True),
    StructField("last_attempt_at", TimestampType(), True),
    StructField("error_date", DateType(), True),
])

SUCCESS_SCHEMA = StructType([
    StructField("primaryKey", StringType(), True),
    StructField("originalMediaItemRid", StringType(), True),
    StructField("originalPath", StringType(), True),
    StructField("originalMediaReference", StringType(), True),
    StructField("pageNumber", IntegerType(), True),
    StructField("totalPages", IntegerType(), True),
    StructField("pageBase64", StringType(), True),
    StructField("pageImageBase64", StringType(), True),
    StructField("status", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("converted_markdown", StringType(), True),
    StructField("markdown_layout_score", DoubleType(), True),
    StructField("markdown_parse_score", DoubleType(), True),
    StructField("markdown_ocr_score", DoubleType(), True),
    StructField("markdown_table_score", DoubleType(), True),
    StructField("has_table", BooleanType(), True),
    StructField("has_image", BooleanType(), True),
    StructField("word_count", IntegerType(), True),
])


def analyze_content(markdown_content: str) -> dict:
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


def make_docling_udf(auth_token: str, base_url: str, conn):
    session_holder = {"s": None}

    def docling_udf_func(encoding, page_number, is_blackline):
        if session_holder["s"] is None:
            session_holder["s"] = _get_session()
        page_num = page_number if page_number is not None else None
        is_blackline_param = is_blackline or None
        return _docling_wrapper(encoding, page_num, auth_token, base_url, is_blackline_param, conn)

    return F.udf(docling_udf_func, DOC_SCHEMA)


def with_keys(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn(
            "record_id",
            F.concat_ws("::",
                F.col("originalMediaItemRid"),
                F.coalesce(F.col("pageNumber").cast("string"), F.lit("-1"))
            )
        )
        .withColumn("pageBase64_hash", F.sha2(F.coalesce(F.col("pageBase64"), F.lit("")), 256))
        .withColumn("pageImageBase64_hash", F.sha2(F.coalesce(F.col("pageImageBase64"), F.lit("")), 256))
        .withColumn("content_hash",
            F.sha2(F.concat_ws("||", F.col("pageBase64_hash"), F.col("pageImageBase64_hash")), 256)
        )
        .drop("pageBase64_hash", "pageImageBase64_hash")
    )


@external_systems(
    doclingccapi=Source("ri.magritte..source.37b3405b-2390-47fd-a8cc-82cb96343d11")
)
@incremental(v2_semantics=True)
@configure(profile=["NUM_EXECUTORS_16",
        "EXECUTOR_MEMORY_LARGE",
        "DRIVER_MEMORY_MEDIUM",])
@transform(
    out_success=Output("ri.foundry.main.dataset.e6baaac9-6bf6-4b9a-90f2-71adfddbcf6c"),
    out_errors=Output("/GI-DEV-SPACE-4ecd2f/DEV-UC-GIN-1219-Workflow/Transform_pipeline/Final Datasets/[GI SC] out_errors"),
    input_dataset=Input("ri.foundry.main.dataset.f1f89c5a-99c9-4681-80ff-a438f9e036f3"),
)
def compute(ctx, doclingccapi, input_dataset, out_success, out_errors):
    transform_start = time.time()
    logger.info("Starting Docling Transform with incremental mode retry logic")

    base_url = doclingccapi.get_https_connection().url
    auth_token = _get_fresh_token_if_needed(doclingccapi)

    if not auth_token:
        logger.error("Failed to obtain authentication token")
        raise RuntimeError("Authentication failed - no token available")

    logger.info(f"Successfully obtained auth token for base URL: {base_url}")

    df_input_added = input_dataset.dataframe("added")
    df_input_previous = input_dataset.dataframe("previous")

    added_count = df_input_added.count()
    added_distinct_count = df_input_added.select("primaryKey").distinct().count()
    if added_count > added_distinct_count:
        duplicates = added_count - added_distinct_count
        logger.warning(f"Found {duplicates} duplicate primaryKeys in input 'added' batch. Deduplicating...")
        window_spec = Window.partitionBy("primaryKey").orderBy(F.lit(1))
        df_input_added = df_input_added.withColumn("_row_num", F.row_number().over(window_spec)).filter(F.col("_row_num") == 1).drop("_row_num")
        logger.info(f"After deduplication: {df_input_added.count()} records in 'added'")

    try:
        df_error_output_previous = out_errors.dataframe("previous")
        df_error_output_previous = ensure_columns(df_error_output_previous, ERROR_SCHEMA)
    except Exception as e:
        logger.info(f"No previous errors found (likely first build): {str(e)}")
        df_error_output_previous = ctx.spark_session.createDataFrame([], ERROR_SCHEMA)

    df_added_keyed = with_keys(df_input_added)
    df_prev_keyed = with_keys(df_input_previous)

    logger.info(f"Added records (with keys): {df_added_keyed.count()}")
    logger.info(f"Previous records (with keys): {df_prev_keyed.count()}")

    try:
        df_success_previous = out_success.dataframe("previous")
        success_count = df_success_previous.count()

        if success_count > 0:
            df_success_previous_keys = df_success_previous.select("primaryKey").distinct()
            distinct_success_count = df_success_previous_keys.count()

            if success_count != distinct_success_count:
                duplicates_in_previous = success_count - distinct_success_count
                logger.error(f"WARNING: Previous output already contains {duplicates_in_previous} duplicate primaryKeys!")
                logger.error(f"Total records: {success_count}, Distinct primaryKeys: {distinct_success_count}")
                logger.error("Consider rebuilding this dataset from scratch to remove existing duplicates.")

            sample_keys = df_success_previous.select("primaryKey").limit(5).collect()
            logger.info(f"Sample primaryKeys in previous successes: {[r['primaryKey'] for r in sample_keys]}")
            logger.info(f"Found {success_count} previously successful records ({distinct_success_count} distinct primaryKeys)")
        else:
            df_success_previous_keys = ctx.spark_session.createDataFrame([], "primaryKey string")
            distinct_success_count = 0

        if distinct_success_count > 1_000_000:
            logger.warning(f"Large success dataset ({distinct_success_count} distinct keys). Using regular join instead of broadcast.")
            use_broadcast_for_success = False
        else:
            use_broadcast_for_success = True

    except Exception as e:
        logger.info(f"No previous successes found (likely first build): {str(e)}")
        df_success_previous_keys = ctx.spark_session.createDataFrame([], "primaryKey string")
        distinct_success_count = 0
        use_broadcast_for_success = True

    # Filter out permanent failures from retry candidates
    if not df_is_empty(df_error_output_previous):
        # Separate permanent failures from retriable errors
        permanent_failures = df_error_output_previous.filter(
            F.col("error_category").isin(["PERMANENT_INVALID_ENCODING", "PERMANENT_SIZE_LIMIT"]) |
            (F.col("attempt_count") >= F.lit(MAX_RETRIES))
        )

        retriable_errors = df_error_output_previous.filter(
            ~F.col("error_category").isin(["PERMANENT_INVALID_ENCODING", "PERMANENT_SIZE_LIMIT"]) &
            (F.col("attempt_count") < F.lit(MAX_RETRIES))
        )

        permanent_count = permanent_failures.count()
        retriable_count = retriable_errors.count()
        logger.info(f"Previous errors: {permanent_count} permanent failures, {retriable_count} retriable")

        errors_ready_to_retry = (
            retriable_errors
            .withColumn("last_attempt_ts", F.col("last_attempt_at"))
            .withColumn("wait_minutes", F.pow(F.lit(2), F.col("attempt_count")).cast("int"))
            .withColumn("retry_after", F.expr("timestampadd(MINUTE, wait_minutes, last_attempt_ts)"))
            .filter(
                F.coalesce(F.col("retry_after") <= F.current_timestamp(), F.lit(True))
            )
            .select("record_id", "content_hash", "attempt_count", "last_attempt_at")
        )

        retry_ready_count = errors_ready_to_retry.count()
        logger.info(f"Errors ready to retry now: {retry_ready_count}")
    else:
        errors_ready_to_retry = df_error_output_previous
        permanent_failures = ctx.spark_session.createDataFrame([], ERROR_SCHEMA)

    if not df_is_empty(errors_ready_to_retry) and not df_is_empty(df_prev_keyed):
        df_previous_errors = (
            df_prev_keyed.join(
                F.broadcast(errors_ready_to_retry.select("record_id", "content_hash", "attempt_count", "last_attempt_at")),
                ["record_id", "content_hash"],
                "inner"
            )
        )
    else:
        df_previous_errors = ctx.spark_session.createDataFrame([], df_added_keyed.schema)

    df_added_dedup = df_added_keyed.join(
        F.broadcast(df_previous_errors.select("record_id").distinct()),
        ["record_id"],
        "left_anti"
    )

    if distinct_success_count > 0:
        if use_broadcast_for_success:
            df_added_dedup = df_added_dedup.join(
                F.broadcast(df_success_previous_keys),
                ["primaryKey"],
                "left_anti"
            )
        else:
            df_added_dedup = df_added_dedup.join(
                df_success_previous_keys,
                ["primaryKey"],
                "left_anti"
            )

    deduped_count = df_added_dedup.count()
    logger.info(f"After deduplication: {deduped_count} new records to process")

    df_added_dedup = (
        df_added_dedup
        .withColumn("attempt_count", F.lit(0))
        .withColumn("last_attempt_at", F.lit(None).cast(TimestampType()))
    )

    # Filter out retry records that have already succeeded
    if not df_is_empty(df_previous_errors) and distinct_success_count > 0:
        if use_broadcast_for_success:
            df_previous_errors = df_previous_errors.join(
                F.broadcast(df_success_previous_keys),
                ["primaryKey"],
                "left_anti"
            )
        else:
            df_previous_errors = df_previous_errors.join(
                df_success_previous_keys,
                ["primaryKey"],
                "left_anti"
            )
        retry_count_after_filter = df_previous_errors.count()
        logger.info(f"Filtered retry records: {retry_count_after_filter} records to retry (excluding already successful ones)")

    df_final_input = df_added_dedup.unionByName(df_previous_errors, allowMissingColumns=True)

    final_input_count = df_final_input.count()
    logger.info(f"Total records to process: {final_input_count}")

    if final_input_count == 0:
        logger.info("No records to process, writing back permanent failures only")
        out_success.write_dataframe(ctx.spark_session.createDataFrame([], SUCCESS_SCHEMA))
        # Write back permanent failures to maintain them in error catalog
        out_errors.write_dataframe(permanent_failures if not df_is_empty(permanent_failures) else ctx.spark_session.createDataFrame([], ERROR_SCHEMA))
        return

    analyze_udf = F.udf(analyze_content, ANALYSIS_SCHEMA)
    to_process = df_final_input

    # Optimize blackline fetching for large datasets
    distinct_ids_df = to_process.select("originalMediaItemRid").distinct()
    total_rids = distinct_ids_df.count()

    logger.info(f"Fetching blackline status for {total_rids} distinct originalMediaItemRids")

    # If we have too many RIDs, use UDF approach instead of collect
    if total_rids > MAX_RIDS_TO_COLLECT:
        logger.warning(f"Large RID count ({total_rids}), using distributed blackline check via UDF")

        # Create UDF for blackline status - will be parallelized across executors
        def get_blackline_udf_func(rid: str) -> bool:
            result = scv2_getblacklinestatus(rid, doclingccapi)
            return result if result is not None else False

        blackline_udf = F.udf(get_blackline_udf_func, BooleanType())

        # Cache blackline status directly in DataFrame
        to_process = to_process.withColumn("is_blackline", blackline_udf(F.col("originalMediaItemRid")))
    else:
        # Original approach for smaller datasets
        distinct_ids = distinct_ids_df.collect()
        ids = [r[0] for r in distinct_ids]

        if not ids:
            logger.info("No distinct RIDs to fetch blackline status for")
            bl_df = ctx.spark_session.createDataFrame([], "originalMediaItemRid string, is_blackline boolean")
        else:
            logger.info(f"Fetching blackline status for {len(ids)} distinct RIDs via driver")
            pairs = [(rid, scv2_getblacklinestatus(rid, doclingccapi) or False) for rid in ids]
            bl_df = ctx.spark_session.createDataFrame(pairs, "originalMediaItemRid string, is_blackline boolean")

        to_process = (
            to_process
            .join(bl_df, "originalMediaItemRid", "left")
            .na.fill({"is_blackline": False})
        )

    # FIX: Don't persist the data with large columns - let Spark manage memory naturally
    # The .persist() with 50MB+ columns was causing executor OOM and shuffle failures
    to_process = (
        to_process
        .withColumn("encoding_to_use",
            F.when(F.col("is_blackline"), F.col("pageImageBase64"))
            .otherwise(F.col("pageBase64"))
        )
        .withColumn("is_valid_enc",
            F.col("encoding_to_use").isNotNull() &
            (F.length(F.col("encoding_to_use")) > F.lit(MIN_ENCODING_LEN))
        )
        # Removed .persist() here - this was causing OOM issues
    )

    valid_to_process = to_process.filter(F.col("is_valid_enc"))
    invalid_to_process = to_process.filter(~F.col("is_valid_enc"))

    counts = to_process.agg(
        F.sum(F.when(F.col("is_valid_enc"), 1).otherwise(0)).alias("valid_count"),
        F.sum(F.when(~F.col("is_valid_enc"), 1).otherwise(0)).alias("invalid_count")
    ).collect()[0]

    valid_count = counts["valid_count"]
    invalid_count = counts["invalid_count"]
    logger.info(f"Processing {valid_count} valid encodings, {invalid_count} invalid encodings")

    doc_udf = make_docling_udf(auth_token, base_url, doclingccapi)

    api_start_time = time.time()

    valid_results = (
        valid_to_process
        .withColumn("docling_result",
                    doc_udf(F.col("encoding_to_use"), F.col("pageNumber"), F.col("is_blackline")))
        .withColumn("converted_markdown", F.col("docling_result.content"))
        .withColumn("markdown_layout_score", F.col("docling_result.layout_score"))
        .withColumn("markdown_parse_score", F.col("docling_result.parse_score"))
        .withColumn("markdown_ocr_score", F.col("docling_result.ocr_score"))
        .withColumn("markdown_table_score", F.col("docling_result.table_score"))
        .drop("docling_result")
    )

    invalid_results = (
        invalid_to_process
        .withColumn("converted_markdown", F.lit("ERROR: Invalid or missing encoding"))
        .withColumn("markdown_layout_score", F.lit(None).cast(DoubleType()))
        .withColumn("markdown_parse_score", F.lit(None).cast(DoubleType()))
        .withColumn("markdown_ocr_score", F.lit(None).cast(DoubleType()))
        .withColumn("markdown_table_score", F.lit(None).cast(DoubleType()))
    )

    # FIX: Don't persist results either - let Spark handle memory management
    # This was the second .persist() causing memory pressure
    results = valid_results.unionByName(invalid_results, allowMissingColumns=True)

    if valid_count > 0:
        api_duration = time.time() - api_start_time
        records_per_second = valid_count / api_duration if api_duration > 0 else 0
        num_partitions = valid_to_process.rdd.getNumPartitions()
        logger.info(f"API call perf: {valid_count} records in {api_duration:.1f}s "
                   f"({records_per_second:.1f} r/s) across {num_partitions} partitions")

    is_error = F.coalesce(F.col("converted_markdown"), F.lit("")).startswith("ERROR:")
    successes = results.filter(~is_error)
    failures = results.filter(is_error)

    result_counts = results.agg(
        F.sum(F.when(~is_error, 1).otherwise(0)).alias("success_count"),
        F.sum(F.when(is_error, 1).otherwise(0)).alias("failure_count")
    ).collect()[0]

    success_count = result_counts["success_count"]
    failure_count = result_counts["failure_count"]

    logger.info(f"Results: {success_count} successes, {failure_count} failures")

    if final_input_count >= CIRCUIT_BREAKER_MIN_SAMPLES:
        error_rate = failure_count / final_input_count if final_input_count > 0 else 0
        if error_rate >= CIRCUIT_BREAKER_ERROR_THRESHOLD and failure_count >= CIRCUIT_BREAKER_MIN_FAILURES:
            classify_error_udf = F.udf(classify_error, StringType())
            error_preview = (
                failures
                .select("converted_markdown")
                .withColumn("error_category", classify_error_udf(F.col("converted_markdown")))
                .groupBy("error_category")
                .count()
                .collect()
            )
            category_summary = ", ".join([f"{r['error_category']}: {r['count']}" for r in error_preview[:5]])

            error_msg = (
                f"CIRCUIT BREAKER TRIGGERED: Error rate is {error_rate:.1%} "
                f"({failure_count}/{final_input_count} failures). "
                f"Error categories: {category_summary}. "
                f"External service may be down. Stopping build to prevent mass failures."
            )
            logger.error(error_msg)
            raise RuntimeError(error_msg)

    current_timestamp = datetime.now().isoformat()
    timestamp_json = f'{{"markdown conversion completed": "{current_timestamp}"}}'

    successes_output = (
        successes
        .withColumn("status", F.lit("markdown conversion completed"))
        .withColumn("analysis", analyze_udf(F.col("converted_markdown")))
        .withColumn("has_table", F.col("analysis.has_table"))
        .withColumn("has_image", F.col("analysis.has_image"))
        .withColumn("word_count", F.col("analysis.word_count"))
        .withColumn("timestamp", F.lit(timestamp_json))
        .drop("analysis", "encoding_to_use", "is_valid_enc", "is_blackline", "last_attempt_at", "attempt_count", "record_id", "content_hash")
    )

    successes_output = ensure_columns(successes_output, SUCCESS_SCHEMA)

    final_success_count_before_dedup = successes_output.count()
    successes_output = successes_output.dropDuplicates(["primaryKey"])
    final_success_count_after_dedup = successes_output.count()

    if final_success_count_before_dedup != final_success_count_after_dedup:
        duplicates_removed = final_success_count_before_dedup - final_success_count_after_dedup
        logger.warning(f"Removed {duplicates_removed} duplicate primaryKeys from final output!")

    # Final safeguard: filter out any records that already exist in the success dataset
    if distinct_success_count > 0:
        count_before_final_filter = successes_output.count()
        if use_broadcast_for_success:
            successes_output = successes_output.join(
                F.broadcast(df_success_previous_keys),
                ["primaryKey"],
                "left_anti"
            )
        else:
            successes_output = successes_output.join(
                df_success_previous_keys,
                ["primaryKey"],
                "left_anti"
            )
        count_after_final_filter = successes_output.count()
        if count_before_final_filter != count_after_final_filter:
            filtered_out = count_before_final_filter - count_after_final_filter
            logger.warning(f"Final filter: Removed {filtered_out} records that already exist in success dataset")

    logger.info(f"Writing {successes_output.count()} successful records to output")

    classify_error_udf = F.udf(classify_error, StringType())

    new_errors = (
        failures
        .select("record_id", "content_hash", "converted_markdown", "attempt_count")
        .withColumnRenamed("converted_markdown", "last_error")
        .withColumn("error_category", classify_error_udf(F.col("last_error")))
        .withColumn("attempt_count", F.col("attempt_count") + F.lit(1))
        .withColumn("last_attempt_at", F.current_timestamp())
        .withColumn("error_date", F.current_date())
    )

    new_errors = ensure_columns(new_errors, ERROR_SCHEMA)

    # Combine new errors with permanent failures to maintain them in the catalog
    all_errors = new_errors.unionByName(permanent_failures, allowMissingColumns=True) if not df_is_empty(permanent_failures) else new_errors

    logger.info(f"Writing {failure_count} new error records + {permanent_failures.count() if not df_is_empty(permanent_failures) else 0} permanent failures to error catalog")

    if failure_count > 0:
        error_stats = (
            new_errors
            .groupBy("error_category")
            .agg(
                F.count("*").alias("count"),
                F.avg("attempt_count").alias("avg_attempts")
            )
            .collect()
        )

        logger.info("Error summary by category:")
        for row in error_stats:
            logger.info(f"  {row['error_category']}: {row['count']} errors (avg {row['avg_attempts']:.1f} attempts)")

        max_retry_count = new_errors.filter(F.col("attempt_count") >= MAX_RETRIES).count()
        permanent_error_count = new_errors.filter(F.col("error_category").isin(["PERMANENT_INVALID_ENCODING", "PERMANENT_SIZE_LIMIT"])).count()

        if max_retry_count > 0:
            logger.warning(f"{max_retry_count} records have reached max retries and will not be retried again")
        if permanent_error_count > 0:
            logger.warning(f"{permanent_error_count} records have permanent errors (invalid encoding or size limit) and will not be retried")

    out_success.write_dataframe(successes_output)
    out_errors.write_dataframe(all_errors)

    # No need to unpersist since we removed .persist() calls

    total_duration = time.time() - transform_start
    logger.info(f"Transform completed in {total_duration:.1f}s")

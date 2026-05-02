from __future__ import annotations

from typing import Any

import boto3
from botocore.exceptions import ClientError

from aws_local import (
    ANALYTICS_BUCKET,
    ANALYTICS_DATABASE,
    AWS_ENDPOINT_URL,
    AWS_REGION,
    BUSINESS_DATABASE,
    DATA_BUCKET,
)


def _glue_client() -> Any:
    return boto3.client(
        "glue",
        endpoint_url=AWS_ENDPOINT_URL,
        region_name=AWS_REGION,
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )


def _columns(columns: list[tuple[str, str]]) -> list[dict[str, str]]:
    return [{"Name": name, "Type": dtype} for name, dtype in columns]


def _create_database(glue: Any, name: str, description: str) -> None:
    try:
        glue.create_database(DatabaseInput={"Name": name, "Description": description})
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") != "AlreadyExistsException":
            raise


def _create_table(
    glue: Any,
    database: str,
    table_name: str,
    bucket: str,
    prefix: str,
    columns: list[tuple[str, str]],
    partition_keys: list[tuple[str, str]],
) -> None:
    try:
        glue.create_table(
            DatabaseName=database,
            TableInput={
                "Name": table_name,
                "StorageDescriptor": {
                    "Columns": _columns(columns),
                    "Location": f"s3://{bucket}/{prefix}/",
                    "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                    "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                    "SerdeInfo": {
                        "SerializationLibrary": "org.openx.data.jsonserde.JsonSerDe",
                        "Parameters": {"paths": "anomesdia,ingestion_id,execution_id,product,status,step_name"},
                    },
                },
                "PartitionKeys": _columns(partition_keys),
                "TableType": "EXTERNAL_TABLE",
            },
        )
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") != "AlreadyExistsException":
            raise


ANALYTICS_INGESTION_RUNS_COLUMNS: list[tuple[str, str]] = [
    ("ingestion_id", "string"),
    ("execution_id", "string"),
    ("correlation_id", "string"),
    ("product", "string"),
    ("domain", "string"),
    ("source_system", "string"),
    ("source_bucket", "string"),
    ("source_key", "string"),
    ("source_file_name", "string"),
    ("source_file_etag", "string"),
    ("status", "string"),
    ("failure_step", "string"),
    ("started_at", "string"),
    ("finished_at", "string"),
    ("duration_seconds", "bigint"),
    ("total_records", "bigint"),
    ("processed_records", "bigint"),
    ("rejected_records", "bigint"),
    ("error_records", "bigint"),
    ("raw_path", "string"),
    ("processed_path", "string"),
    ("curated_path", "string"),
    ("rejected_path", "string"),
    ("error_message", "string"),
]

ANALYTICS_INGESTION_STEPS_COLUMNS: list[tuple[str, str]] = [
    ("ingestion_id", "string"),
    ("execution_id", "string"),
    ("correlation_id", "string"),
    ("product", "string"),
    ("domain", "string"),
    ("step_name", "string"),
    ("step_order", "int"),
    ("attempt", "int"),
    ("glue_job_name", "string"),
    ("glue_job_run_id", "string"),
    ("status", "string"),
    ("started_at", "string"),
    ("finished_at", "string"),
    ("duration_seconds", "bigint"),
    ("input_records", "bigint"),
    ("output_records", "bigint"),
    ("rejected_records", "bigint"),
    ("error_records", "bigint"),
    ("input_path", "string"),
    ("output_path", "string"),
    ("error_message", "string"),
    ("error_type", "string"),
    ("error_code", "string"),
    ("error_category", "string"),
    ("is_retryable", "boolean"),
    ("source_bucket", "string"),
    ("source_key", "string"),
    ("payload_ref", "string"),
    ("occurred_at", "string"),
    ("rule_name", "string"),
    ("rule_type", "string"),
    ("rule_result", "string"),
    ("valid_records", "bigint"),
    ("invalid_records", "bigint"),
    ("warning_records", "bigint"),
    ("threshold_value", "string"),
    ("measured_value", "string"),
    ("quality_details", "string"),
    ("measured_at", "string"),
    ("schema_name", "string"),
    ("schema_version", "string"),
    ("validation_result", "string"),
    ("missing_columns", "string"),
    ("unexpected_columns", "string"),
    ("invalid_types", "string"),
    ("validation_message", "string"),
    ("validated_at", "string"),
    ("rejection_reason", "string"),
    ("rejection_category", "string"),
    ("rejected_count_summary", "bigint"),
    ("total_step_records_summary", "bigint"),
    ("rejection_percent", "double"),
    ("rejected_detail_path", "string"),
    ("sample_message", "string"),
    ("artifact_type", "string"),
    ("artifact_role", "string"),
    ("lineage_bucket", "string"),
    ("lineage_key", "string"),
    ("lineage_format", "string"),
    ("record_count_lineage", "bigint"),
    ("file_size_bytes", "bigint"),
    ("parent_bucket", "string"),
    ("parent_key", "string"),
    ("lineage_created_at", "string"),
]

BUSINESS_RAW_COLUMNS: list[tuple[str, str]] = [
    ("transaction_id", "string"),
    ("customer_id", "string"),
    ("amount", "string"),
    ("transaction_date", "string"),
    ("domain", "string"),
    ("product", "string"),
    ("business_date", "string"),
    ("source_file", "string"),
]

BUSINESS_PROCESSED_COLUMNS: list[tuple[str, str]] = [
    ("transaction_id", "string"),
    ("customer_id", "string"),
    ("amount", "string"),
    ("transaction_date", "string"),
]

BUSINESS_CURATED_COLUMNS: list[tuple[str, str]] = [
    ("transaction_id", "string"),
    ("customer_id", "string"),
    ("amount", "string"),
    ("transaction_date", "string"),
    ("domain", "string"),
    ("product", "string"),
    ("business_date", "string"),
    ("enriched_at", "string"),
]

BUSINESS_REJECTED_COLUMNS: list[tuple[str, str]] = [
    ("run_id", "string"),
    ("stage", "string"),
    ("product", "string"),
    ("business_date", "string"),
    ("file_name", "string"),
    ("row_number", "int"),
    ("reason", "string"),
    ("raw_row", "string"),
]

BUSINESS_PARTITION = [("year", "string"), ("month", "string"), ("day", "string")]

ANALYTICS_PARTITION = [("anomesdia", "string")]

ANALYTICS_TABLES: list[tuple[str, str, list[tuple[str, str]]]] = [
    ("analytics_ingestion_runs", "observability/ingestion_runs", ANALYTICS_INGESTION_RUNS_COLUMNS),
    ("analytics_ingestion_steps", "observability/ingestion_steps", ANALYTICS_INGESTION_STEPS_COLUMNS),
]

BUSINESS_TABLES: list[tuple[str, str, list[tuple[str, str]]]] = [
    ("business_raw", "raw", BUSINESS_RAW_COLUMNS),
    ("business_processed", "processed", BUSINESS_PROCESSED_COLUMNS),
    ("business_curated", "curated", BUSINESS_CURATED_COLUMNS),
    ("business_rejected", "rejected", BUSINESS_REJECTED_COLUMNS),
]


def bootstrap_glue_catalog() -> dict[str, Any]:
    glue = _glue_client()

    _create_database(glue, ANALYTICS_DATABASE, "POC Data Ingestion operational analytics")
    _create_database(glue, BUSINESS_DATABASE, "POC Data Ingestion business data lake")

    analytics_tables = []
    for table_name, prefix, columns in ANALYTICS_TABLES:
        _create_table(glue, ANALYTICS_DATABASE, table_name, ANALYTICS_BUCKET, prefix, columns, ANALYTICS_PARTITION)
        analytics_tables.append(f"{ANALYTICS_DATABASE}.{table_name}")

    business_tables = []
    for table_name, prefix, columns in BUSINESS_TABLES:
        _create_table(glue, BUSINESS_DATABASE, table_name, DATA_BUCKET, prefix, columns, BUSINESS_PARTITION)
        business_tables.append(f"{BUSINESS_DATABASE}.{table_name}")

    return {
        "analytics_database": ANALYTICS_DATABASE,
        "business_database": BUSINESS_DATABASE,
        "analytics_tables": analytics_tables,
        "business_tables": business_tables,
    }

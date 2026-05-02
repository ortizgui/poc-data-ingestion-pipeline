from __future__ import annotations

import json
import os
import tempfile
from typing import Any

import boto3
import duckdb

from aws_local import ANALYTICS_BUCKET, AWS_ENDPOINT_URL, AWS_REGION, DATA_BUCKET


ANALYTICS_PREFIXES = {
    "analytics_ingestion_runs": "observability/ingestion_runs",
    "analytics_ingestion_steps": "observability/ingestion_steps",
    "analytics_ingestion_errors": "observability/ingestion_errors",
    "analytics_ingestion_rejections_summary": "observability/ingestion_rejections",
    "analytics_data_quality_summary": "quality/data_quality_summary",
    "analytics_schema_validation": "quality/schema_validation",
    "analytics_file_lineage": "audit/file_lineage",
    "analytics_execution_events": "audit/execution_events",
}

BUSINESS_PREFIXES = {
    "business_curated": "curated",
    "business_rejected": "rejected",
}


def _s3_client() -> Any:
    return boto3.client(
        "s3",
        endpoint_url=AWS_ENDPOINT_URL,
        region_name=AWS_REGION,
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )


def _list_objects(bucket: str, prefix: str) -> list[dict[str, Any]]:
    s3 = _s3_client()
    objects: list[dict[str, Any]] = []
    continuation_token = None
    while True:
        params: dict[str, Any] = {"Bucket": bucket, "Prefix": prefix}
        if continuation_token:
            params["ContinuationToken"] = continuation_token
        response = s3.list_objects_v2(**params)
        for item in response.get("Contents", []):
            objects.append({"key": item["Key"], "size": item["Size"]})
        if not response.get("IsTruncated"):
            break
        continuation_token = response.get("NextContinuationToken")
    return objects


def _load_jsonl(bucket: str, prefix: str) -> str:
    s3 = _s3_client()
    objects = _list_objects(bucket, prefix)
    parts: list[str] = []
    for obj in objects:
        if obj["size"] == 0:
            continue
        body = s3.get_object(Bucket=bucket, Key=obj["key"])["Body"].read().decode("utf-8")
        parts.append(body)
    return "".join(parts)


def _load_table(con: duckdb.DuckDBPyConnection, table_name: str, bucket: str, prefix: str) -> int:
    jsonl = _load_jsonl(bucket, prefix)
    if not jsonl.strip():
        return 0
    with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as tmp:
        tmp.write(jsonl)
        tmp_path = tmp.name
    try:
        con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_json_auto('{tmp_path}')")
        count = con.execute(f"SELECT count(*) FROM {table_name}").fetchone()
        return count[0] if count else 0
    finally:
        os.unlink(tmp_path)


def run_analytics_queries() -> dict[str, Any]:
    con = duckdb.connect(":memory:")

    table_counts: dict[str, int] = {}
    for table_name, prefix in ANALYTICS_PREFIXES.items():
        table_counts[table_name] = _load_table(con, table_name, ANALYTICS_BUCKET, prefix)

    for table_name, prefix in BUSINESS_PREFIXES.items():
        table_counts[table_name] = _load_table(con, table_name, DATA_BUCKET, prefix)

    queries: dict[str, str] = {}
    if table_counts.get("analytics_ingestion_runs", 0) > 0:
        queries["runs_by_status"] = """
            SELECT anomesdia, product, status, count(*) AS total_runs,
                   sum(total_records) AS total_records,
                   sum(processed_records) AS processed_records,
                   sum(rejected_records) AS rejected_records
            FROM analytics_ingestion_runs
            GROUP BY anomesdia, product, status
            ORDER BY anomesdia, product
        """

    if table_counts.get("analytics_ingestion_steps", 0) > 0:
        queries["steps_summary"] = """
            SELECT anomesdia, product, step_order, step_name, status, count(*) AS attempts,
                   sum(input_records) AS input_records,
                   sum(output_records) AS output_records,
                   sum(rejected_records) AS rejected_records
            FROM analytics_ingestion_steps
            GROUP BY anomesdia, product, step_order, step_name, status
            ORDER BY anomesdia, product, step_order, step_name
        """

    if table_counts.get("analytics_ingestion_errors", 0) > 0:
        queries["errors_by_product"] = """
            SELECT anomesdia, product, step_name, error_type, error_category, count(*) AS errors
            FROM analytics_ingestion_errors
            GROUP BY anomesdia, product, step_name, error_type, error_category
            ORDER BY anomesdia, product
        """

    if table_counts.get("analytics_ingestion_rejections_summary", 0) > 0:
        queries["rejections_summary"] = """
            SELECT anomesdia, product, step_name, rejection_reason,
                   sum(rejected_count) AS total_rejected,
                   round(avg(rejection_percent), 2) AS avg_pct,
                   min(rejected_detail_path) AS detail_path,
                   min(sample_message) AS sample
            FROM analytics_ingestion_rejections_summary
            GROUP BY anomesdia, product, step_name, rejection_reason
            ORDER BY anomesdia, product
        """

    if table_counts.get("analytics_data_quality_summary", 0) > 0:
        queries["data_quality"] = """
            SELECT anomesdia, product, step_name, rule_name, rule_result,
                   sum(total_records) AS total_records,
                   sum(valid_records) AS valid_records,
                   sum(invalid_records) AS invalid_records
            FROM analytics_data_quality_summary
            GROUP BY anomesdia, product, step_name, rule_name, rule_result
            ORDER BY anomesdia, product
        """

    if table_counts.get("analytics_schema_validation", 0) > 0:
        queries["schema_validation"] = """
            SELECT anomesdia, product, step_name, validation_result, count(*) AS checks
            FROM analytics_schema_validation
            GROUP BY anomesdia, product, step_name, validation_result
            ORDER BY anomesdia, product
        """

    if table_counts.get("analytics_file_lineage", 0) > 0:
        queries["file_lineage"] = """
            SELECT anomesdia, product, artifact_type, count(*) AS files,
                   sum(record_count) AS total_records
            FROM analytics_file_lineage
            GROUP BY anomesdia, product, artifact_type
            ORDER BY anomesdia, product, artifact_type
        """

    if table_counts.get("analytics_execution_events", 0) > 0:
        queries["execution_events"] = """
            SELECT anomesdia, product, event_type, count(*) AS events
            FROM analytics_execution_events
            GROUP BY anomesdia, product, event_type
            ORDER BY anomesdia, product
        """

    if table_counts.get("business_curated", 0) > 0 and table_counts.get("analytics_ingestion_runs", 0) > 0:
        queries["curated_with_run_context"] = """
            SELECT c.product, c.business_date, c.domain,
                   count(*) AS curated_records,
                   r.status AS run_status
            FROM business_curated c
            LEFT JOIN analytics_ingestion_runs r
                ON c.product = r.product AND strftime(c.business_date, '%Y%m%d') = r.anomesdia AND r.status = 'SUCCEEDED'
            GROUP BY c.product, c.business_date, c.domain, r.status
            ORDER BY c.product, c.business_date
        """

    if table_counts.get("business_rejected", 0) > 0:
        queries["rejected_detail"] = """
            SELECT product, stage, reason, count(*) AS rows
            FROM business_rejected
            GROUP BY product, stage, reason
            ORDER BY product, stage
        """

    if table_counts.get("analytics_ingestion_errors", 0) > 0 and table_counts.get("analytics_ingestion_runs", 0) > 0:
        queries["error_detail_with_source"] = """
            SELECT e.anomesdia, e.product, e.step_name, e.error_type, e.error_code,
                   e.error_message, e.error_category, e.occurred_at,
                   e.source_bucket, e.source_key,
                   r.source_file_name, r.status AS run_status
            FROM analytics_ingestion_errors e
            LEFT JOIN analytics_ingestion_runs r ON e.ingestion_id = r.ingestion_id
            ORDER BY e.anomesdia, e.product, e.occurred_at
        """

    if table_counts.get("analytics_ingestion_runs", 0) > 0:
        queries["troubleshooting_dashboard"] = """
            SELECT r.anomesdia, r.product, r.status AS run_status,
                   r.source_bucket, r.source_key, r.source_file_name,
                   r.total_records, r.processed_records, r.rejected_records, r.error_records,
                   r.failure_step, r.error_message AS run_error,
                   CASE WHEN r.rejected_records > 0 THEN 1 ELSE 0 END AS has_rejections
            FROM analytics_ingestion_runs r
            ORDER BY r.anomesdia, r.product
        """

    results: dict[str, Any] = {"table_counts": table_counts, "queries": {}}
    for name, sql in queries.items():
        try:
            rows = con.execute(sql).fetchall()
            columns = [desc[0] for desc in con.description]
            results["queries"][name] = [dict(zip(columns, [str(v) for v in row])) for row in rows]
        except Exception as exc:
            results["queries"][name] = {"error": str(exc)}

    con.close()
    return results


def print_analytics_report(result: dict[str, Any]) -> None:
    print("\n===== Analytics Query Results (DuckDB local simulation) =====")

    tc = result["table_counts"]
    print(f"\nTables loaded: {sum(1 for v in tc.values() if v > 0)} of {len(tc)}")
    for table, count in tc.items():
        status = "OK" if count > 0 else "EMPTY"
        print(f"  {table}: {count} rows [{status}]")

    for name, rows in result.get("queries", {}).items():
        if isinstance(rows, dict) and "error" in rows:
            print(f"\n--- {name}: ERROR: {rows['error']}")
            continue
        print(f"\n--- {name} ({len(rows)} rows) ---")
        for row in rows:
            print(f"  {row}")

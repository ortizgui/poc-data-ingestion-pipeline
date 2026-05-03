from __future__ import annotations

import json
import os
import tempfile
from typing import Any

import duckdb

from aws_local import ANALYTICS_BUCKET, DATA_BUCKET, service_client


ANALYTICS_PREFIXES = {
    "analytics_ingestion_runs": "observability/ingestion_runs",
    "analytics_ingestion_steps": "observability/ingestion_steps",
}

BUSINESS_PREFIXES = {
    "business_curated": "curated",
    "business_rejected": "rejected",
}


def _list_objects(bucket: str, prefix: str) -> list[dict[str, Any]]:
    s3 = service_client("s3")
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
    s3 = service_client("s3")
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


def _has_column(con: duckdb.DuckDBPyConnection, table: str, column: str) -> bool:
    try:
        con.execute(f"SELECT {column} FROM {table} LIMIT 0")
        return True
    except Exception:
        return False


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

        queries["troubleshooting_dashboard"] = """
            SELECT anomesdia, product, status AS run_status,
                   source_bucket, source_key, source_file_name,
                   total_records, processed_records, rejected_records, error_records,
                   failure_step, error_message AS run_error,
                   raw_path, processed_path, curated_path, rejected_path
            FROM analytics_ingestion_runs
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

        has_enriched = _has_column(con, "analytics_ingestion_steps", "error_type")
        if has_enriched:
            queries["errors_from_steps"] = """
                SELECT anomesdia, product, step_name, error_type, error_code,
                       error_message, error_category, source_bucket, source_key, occurred_at
                FROM analytics_ingestion_steps
                WHERE error_message IS NOT NULL AND error_message != ''
                ORDER BY anomesdia, product, occurred_at
            """

            queries["quality_from_steps"] = """
                SELECT anomesdia, product, step_name, rule_name, rule_type, rule_result,
                       sum(valid_records) AS valid_records,
                       sum(invalid_records) AS invalid_records
                FROM analytics_ingestion_steps
                WHERE rule_name IS NOT NULL AND rule_name != ''
                GROUP BY anomesdia, product, step_name, rule_name, rule_type, rule_result
                ORDER BY anomesdia, product
            """

            queries["schema_from_steps"] = """
                SELECT anomesdia, product, step_name, schema_name, validation_result,
                       missing_columns, unexpected_columns, validated_at
                FROM analytics_ingestion_steps
                WHERE schema_name IS NOT NULL AND schema_name != ''
                ORDER BY anomesdia, product, validated_at
            """

            queries["rejections_from_steps"] = """
                SELECT anomesdia, product, step_name, rejection_reason, rejection_category,
                       sum(rejected_count_summary) AS total_rejected,
                       round(avg(rejection_percent), 2) AS avg_pct,
                       min(rejected_detail_path) AS detail_path,
                       min(sample_message) AS sample
                FROM analytics_ingestion_steps
                WHERE rejection_reason IS NOT NULL AND rejection_reason != ''
                GROUP BY anomesdia, product, step_name, rejection_reason, rejection_category
                ORDER BY anomesdia, product
            """

            queries["lineage_from_steps"] = """
                SELECT anomesdia, product, artifact_type, count(*) AS files,
                       sum(record_count_lineage) AS total_records
                FROM analytics_ingestion_steps
                WHERE artifact_type IS NOT NULL AND artifact_type != ''
                GROUP BY anomesdia, product, artifact_type
                ORDER BY anomesdia, product, artifact_type
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

    if table_counts.get("analytics_ingestion_steps", 0) > 0 and table_counts.get("analytics_ingestion_runs", 0) > 0:
        if _has_column(con, "analytics_ingestion_steps", "error_type"):
            queries["error_detail_with_source"] = """
                SELECT s.anomesdia, s.product, s.step_name, s.error_type, s.error_code,
                       s.error_message, s.error_category, s.occurred_at,
                       s.source_bucket, s.source_key,
                       r.source_file_name, r.status AS run_status
                FROM analytics_ingestion_steps s
                LEFT JOIN analytics_ingestion_runs r ON s.ingestion_id = r.ingestion_id
                WHERE s.error_message IS NOT NULL AND s.error_message != ''
                ORDER BY s.anomesdia, s.product, s.occurred_at
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
            print(f"\n--- {name}: ERROR: {rows['error'][:120]}")
            continue
        print(f"\n--- {name} ({len(rows)} rows) ---")
        for row in rows:
            print(f"  {row}")

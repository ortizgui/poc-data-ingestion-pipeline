import json
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from analytics_writer import analytics_dimensions, sanitize_fragment
from aws_local import (
    ANALYTICS_BUCKET,
    DATA_BUCKET,
    DOMAIN_REQUIRED_FIELDS,
    PipelineError,
    RejectedRowsThresholdError,
    anomesdia_for,
    load_json,
    normalize_rejection_policy,
    parse_business_date,
    partition_prefix,
    read_csv_text,
    rejected_key_for,
    rejection_record,
    rejection_threshold_exceeded,
    validate_mapping,
    validate_product_config,
)
from ecs_worker import process_message, product_from_file_key, run_id_from_file_key, s3_records
from enrichment_batch import enrich_row, run_enrichment
from evidence import Evidence, evidence_table, write_local_report
from glue_catalog import (
    ANALYTICS_TABLES,
    BUSINESS_TABLES,
    ANALYTICS_INGESTION_RUNS_COLUMNS,
    ANALYTICS_INGESTION_STEPS_COLUMNS,
    BUSINESS_CURATED_COLUMNS,
    BUSINESS_REJECTED_COLUMNS,
)
from glue_harmonization import run_harmonization
from glue_landing import run_landing
from local_sfn_runner import validate_event
from pipeline import run_id_for


class FakeTable:
    def __init__(self, items):
        self.items = items

    def get_item(self, Key):
        item = self.items.get(Key["product"])
        return {"Item": item} if item else {}


class FakeDynamoDB:
    def __init__(self, items):
        self.items = items

    def Table(self, _name):
        return FakeTable(self.items)


class FakeAws:
    def __init__(self, items):
        self.dynamodb = FakeDynamoDB(items)


class FakeBody:
    def __init__(self, text):
        self.text = text

    def read(self):
        return self.text.encode("utf-8")


class FakeS3:
    def __init__(self, objects):
        self.objects = objects

    def get_object(self, Bucket, Key):
        return {"Body": FakeBody(self.objects[(Bucket, Key)])}

    def put_object(self, Bucket, Key, Body):
        self.objects[(Bucket, Key)] = Body.decode("utf-8")


class FakeSns:
    def __init__(self):
        self.messages = []

    def publish(self, TopicArn, Message):
        self.messages.append(json.loads(Message))


class PipelineUnitTest(unittest.TestCase):
    def test_validate_event_accepts_product_configured_in_dynamodb(self):
        aws = FakeAws(
            {
                "orders": {
                    "domain": "transaction",
                    "mapping_key": "de-para/orders-transaction.json",
                    "publish": {"destinations": ["billing-events"]},
                }
            }
        )

        result = validate_event(
            {"product": "orders", "file_name": "orders.csv", "business_date": "2026-05-01"},
            aws,
        )

        self.assertEqual(result["product"], "orders")

    def test_validate_event_rejects_product_not_configured_in_dynamodb(self):
        with self.assertRaisesRegex(PipelineError, "product not configured"):
            validate_event(
                {"product": "invoices", "file_name": "invoices.csv", "business_date": "2026-05-01"},
                FakeAws({}),
            )

    def test_validate_mapping_rejects_missing_standard_domain_field(self):
        with self.assertRaisesRegex(PipelineError, "missing domain fields"):
            validate_mapping(
                "orders",
                {
                    "domain": "transaction",
                    "layout_mapping": {
                        "pedido_id": "transaction_id",
                        "cliente_id": "customer_id",
                        "valor_total": "amount",
                    },
                    "domain_required_fields": list(DOMAIN_REQUIRED_FIELDS),
                },
            )

    def test_validate_event_rejects_non_transaction_domain(self):
        aws = FakeAws(
            {
                "orders": {
                    "domain": "order",
                    "mapping_key": "de-para/orders-transaction.json",
                    "publish": {"destinations": ["billing-events"]},
                }
            }
        )

        with self.assertRaisesRegex(PipelineError, "must map to domain transaction"):
            validate_event(
                {"product": "orders", "file_name": "orders.csv", "business_date": "2026-05-01"},
                aws,
            )

    def test_partition_prefix_uses_s3_date_layout(self):
        self.assertEqual(partition_prefix("2026-05-01"), "year=2026/month=05/day=01")

    def test_read_csv_text(self):
        rows = read_csv_text("pedido_id,valor_total\n1001,159.90\n")
        self.assertEqual(rows, [{"pedido_id": "1001", "valor_total": "159.90"}])

    def test_evidence_table_shows_step_status(self):
        evidence = Evidence(run_id="run-1", product="orders", file_name="orders.csv", business_date="2026-05-01")
        evidence.ok("EventBridge", "accepted", "event-bus://ingestion-events")
        evidence.fail("DynamoDBConfig", "product not configured")

        table = evidence_table(evidence.as_dict())

        self.assertIn("FAIL", table)
        self.assertIn("DynamoDBConfig", table)
        self.assertIn("product not configured", table)

    def test_configured_products_use_same_transaction_domain(self):
        config = load_json(Path("config/products.json"))
        products = config["products"]

        self.assertEqual(products["orders"]["domain"], "transaction")
        self.assertEqual(products["payments"]["domain"], "transaction")
        self.assertEqual(products["orders"]["mapping_key"], "de-para/orders-transaction.json")
        self.assertEqual(products["payments"]["mapping_key"], "de-para/payments-transaction.json")

    def test_mapping_files_define_same_domain_schema(self):
        for mapping_path in Path("config/mappings").glob("*.json"):
            mapping = validate_mapping(mapping_path.stem, load_json(mapping_path))

            self.assertEqual(mapping["domain"], "transaction")
            self.assertEqual(set(mapping["domain_required_fields"]), set(DOMAIN_REQUIRED_FIELDS))

    def test_rejection_policy_defaults_and_threshold(self):
        policy = normalize_rejection_policy("orders", {"domain": "transaction", "mapping_key": "x", "publish": {"destinations": ["billing"]}})

        self.assertEqual(policy["max_error_percent"], 1)
        self.assertEqual(policy["max_error_count"], 1000)
        self.assertFalse(rejection_threshold_exceeded(100, 1, policy))
        self.assertTrue(rejection_threshold_exceeded(100, 2, policy))

    def test_harmonization_writes_rejected_rows_under_threshold(self):
        mapping = {
            "domain": "transaction",
            "domain_required_fields": list(DOMAIN_REQUIRED_FIELDS),
            "layout_mapping": {
                "pedido_id": "transaction_id",
                "cliente_id": "customer_id",
                "valor_total": "amount",
                "data_pedido": "transaction_date",
            },
        }
        objects = {
            (DATA_BUCKET, "de-para/orders-transaction.json"): json.dumps(mapping),
            (DATA_BUCKET, "raw/orders/file.csv"): (
                "pedido_id,cliente_id,valor_total,data_pedido\n"
                "1001,C1,10.00,2026-05-01\n"
                "1002,C2,,2026-05-01\n"
            ),
        }
        aws = SimpleNamespace(s3=FakeS3(objects))
        event = {
            "product": "orders",
            "file_name": "orders.csv",
            "business_date": "2026-05-01",
            "product_config": {
                "mapping_key": "de-para/orders-transaction.json",
                "rejection_policy": {
                    "max_error_percent": 50,
                    "max_error_count": 10,
                    "destinations": ["data-quality-events"],
                },
            },
        }

        processed_key, processed_rows, _mapping_key, rejection_key, rejected_rows = run_harmonization(
            "raw/orders/file.csv", event, aws, "run-1"
        )

        self.assertEqual(processed_rows, 1)
        self.assertEqual(rejected_rows, 1)
        self.assertIn((DATA_BUCKET, processed_key), objects)
        self.assertIn((DATA_BUCKET, rejection_key), objects)
        self.assertIn("missing domain fields: amount", objects[(DATA_BUCKET, rejection_key)])
        self.assertTrue(any(bucket == ANALYTICS_BUCKET for bucket, _key in objects))

    def test_harmonization_stops_before_processed_when_threshold_exceeded(self):
        mapping = {
            "domain": "transaction",
            "domain_required_fields": list(DOMAIN_REQUIRED_FIELDS),
            "layout_mapping": {
                "pedido_id": "transaction_id",
                "cliente_id": "customer_id",
                "valor_total": "amount",
                "data_pedido": "transaction_date",
            },
        }
        objects = {
            (DATA_BUCKET, "de-para/orders-transaction.json"): json.dumps(mapping),
            (DATA_BUCKET, "raw/orders/file.csv"): (
                "pedido_id,cliente_id,valor_total,data_pedido\n"
                "1001,C1,10.00,2026-05-01\n"
                "1002,C2,,2026-05-01\n"
            ),
        }
        aws = SimpleNamespace(s3=FakeS3(objects))
        event = {
            "product": "orders",
            "file_name": "orders.csv",
            "business_date": "2026-05-01",
            "product_config": {
                "mapping_key": "de-para/orders-transaction.json",
                "rejection_policy": {
                    "max_error_percent": 1,
                    "max_error_count": 0,
                    "destinations": ["data-quality-events"],
                },
            },
        }

        with self.assertRaisesRegex(RejectedRowsThresholdError, "rejected rows exceeded threshold"):
            run_harmonization("raw/orders/file.csv", event, aws, "run-1")

        keys = [key for _bucket, key in objects]
        self.assertTrue(any(key.startswith("rejected/harmonization/orders/") for key in keys))
        self.assertFalse(any(key.startswith("processed/orders/") for key in keys))

    def test_enrichment_writes_rejected_rows_under_threshold(self):
        processed_key = "processed/orders/year=2026/month=05/day=01/run-1/orders.parquet"
        objects = {
            (DATA_BUCKET, processed_key): (
                json.dumps({"transaction_id": "1001", "customer_id": "C1", "amount": "10.00", "transaction_date": "2026-05-01"})
                + "\n"
                + json.dumps({"transaction_id": "1002", "customer_id": "bad", "amount": "20.00", "transaction_date": "2026-05-01"})
                + "\n"
            ),
        }
        aws = SimpleNamespace(s3=FakeS3(objects))
        event = {
            "run_id": "run-1",
            "product": "orders",
            "file_name": "orders.csv",
            "business_date": "2026-05-01",
            "product_config": {
                "rejection_policy": {
                    "max_error_percent": 50,
                    "max_error_count": 10,
                    "destinations": ["data-quality-events"],
                },
            },
        }

        def enrich_or_fail(row, valid_event):
            if row["customer_id"] == "bad":
                raise ValueError("customer not found in enrichment base")
            return {"enriched": True, **row}

        with patch("enrichment_batch.enrich_row", side_effect=enrich_or_fail):
            curated_key, curated_rows, rejection_key, rejected_rows = run_enrichment(processed_key, event, aws)

        self.assertEqual(curated_rows, 1)
        self.assertEqual(rejected_rows, 1)
        self.assertIn((DATA_BUCKET, curated_key), objects)
        self.assertIn((DATA_BUCKET, rejection_key), objects)
        self.assertIn("customer not found in enrichment base", objects[(DATA_BUCKET, rejection_key)])
        self.assertTrue(any(bucket == ANALYTICS_BUCKET for bucket, _key in objects))

    def test_enrichment_stops_before_curated_when_threshold_exceeded(self):
        processed_key = "processed/orders/year=2026/month=05/day=01/run-1/orders.parquet"
        objects = {
            (DATA_BUCKET, processed_key): (
                json.dumps({"transaction_id": "1001", "customer_id": "C1", "amount": "10.00", "transaction_date": "2026-05-01"})
                + "\n"
                + json.dumps({"transaction_id": "1002", "customer_id": "bad", "amount": "20.00", "transaction_date": "2026-05-01"})
                + "\n"
            ),
        }
        aws = SimpleNamespace(s3=FakeS3(objects))
        event = {
            "run_id": "run-1",
            "product": "orders",
            "file_name": "orders.csv",
            "business_date": "2026-05-01",
            "product_config": {
                "rejection_policy": {
                    "max_error_percent": 1,
                    "max_error_count": 0,
                    "destinations": ["data-quality-events"],
                },
            },
        }

        def enrich_or_fail(row, valid_event):
            if row["customer_id"] == "bad":
                raise ValueError("customer not found in enrichment base")
            return {"enriched": True, **row}

        with (
            patch("enrichment_batch.enrich_row", side_effect=enrich_or_fail),
            self.assertRaisesRegex(RejectedRowsThresholdError, "rejected rows exceeded threshold"),
        ):
            run_enrichment(processed_key, event, aws)

        keys = [key for _bucket, key in objects]
        self.assertTrue(any(key.startswith("rejected/enrichment/orders/") for key in keys))
        self.assertFalse(any(key.startswith("curated/orders/") for key in keys))

    def test_worker_publishes_rejected_records_to_quality_destination(self):
        key = "rejected/harmonization/orders/year=2026/month=05/day=01/run-1/orders.jsonl"
        objects = {
            (DATA_BUCKET, key): json.dumps(
                {
                    "run_id": "run-1",
                    "stage": "harmonization",
                    "product": "orders",
                    "reason": "missing domain fields: amount",
                },
                sort_keys=True,
            )
            + "\n"
        }
        sns = FakeSns()

        with (
            patch(
                "ecs_worker.product_config",
                return_value={
                    "domain": "transaction",
                    "mapping_key": "de-para/orders-transaction.json",
                    "publish": {"destinations": ["billing-events"]},
                    "rejection_policy": {
                        "max_error_percent": 1,
                        "max_error_count": 1000,
                        "destinations": ["data-quality-events"],
                    },
                },
            ),
            patch("ecs_worker.ensure_topic_and_destination_queue", return_value=("topic:data-quality-events", "queue")),
        ):
            published = process_message({"bucket": DATA_BUCKET, "key": key}, sns, SimpleNamespace(), FakeS3(objects))

        self.assertEqual(published, ["data-quality-events"])
        self.assertEqual(sns.messages[0]["event_type"], "ingestion.records-rejected")
        self.assertEqual(sns.messages[0]["rejected_record"]["reason"], "missing domain fields: amount")

    def test_glue_catalog_analytics_tables_are_registered(self):
        calls = []

        class FakeGlue:
            def create_database(self, DatabaseInput):
                calls.append(("create_database", DatabaseInput))
            def create_table(self, DatabaseName, TableInput):
                calls.append(("create_table", DatabaseName, TableInput["Name"]))

        with patch("glue_catalog.service_client", return_value=FakeGlue()):
            from glue_catalog import bootstrap_glue_catalog
            result = bootstrap_glue_catalog()

        self.assertEqual(len(result["analytics_tables"]), 2)
        self.assertEqual(len(result["business_tables"]), 4)
        database_calls = [c for c in calls if c[0] == "create_database"]
        table_calls = [c for c in calls if c[0] == "create_table"]
        self.assertEqual(len(database_calls), 2)
        self.assertEqual(len(table_calls), 6)

    def test_glue_catalog_step_columns_include_enriched_fields(self):
        step_columns = [name for name, _ in ANALYTICS_INGESTION_STEPS_COLUMNS]
        for col in ["error_type", "error_message", "rule_name", "rule_result",
                     "schema_name", "validation_result", "rejection_reason",
                     "artifact_type", "lineage_key", "rejected_detail_path"]:
            self.assertIn(col, step_columns, f"{col} missing from enriched steps")

    def test_glue_catalog_run_column_count(self):
        self.assertEqual(len(ANALYTICS_INGESTION_RUNS_COLUMNS), 24)

    def test_glue_catalog_all_tables_have_anomesdia_partition(self):
        from glue_catalog import ANALYTICS_PARTITION
        self.assertEqual(ANALYTICS_PARTITION, [("anomesdia", "string")])

    def test_glue_catalog_tables_have_required_columns(self):
        run_columns = [name for name, _ in ANALYTICS_INGESTION_RUNS_COLUMNS]
        for col in ["ingestion_id", "execution_id", "product", "status"]:
            self.assertIn(col, run_columns, f"{col} missing from analytics_ingestion_runs")

        step_columns = [name for name, _ in ANALYTICS_INGESTION_STEPS_COLUMNS]
        for col in ["step_name", "status", "error_message", "rule_result", "validation_result", "artifact_type"]:
            self.assertIn(col, step_columns, f"{col} missing from analytics_ingestion_steps")

        curated_columns = [name for name, _ in BUSINESS_CURATED_COLUMNS]
        for col in ["transaction_id", "customer_id", "amount", "product", "business_date"]:
            self.assertIn(col, curated_columns, f"{col} missing from business_curated")

        rejected_columns = [name for name, _ in BUSINESS_REJECTED_COLUMNS]
        for col in ["run_id", "stage", "product", "reason", "row_number"]:
            self.assertIn(col, rejected_columns, f"{col} missing from business_rejected")


    def test_parse_business_date_valid(self):
        dt = parse_business_date("2026-05-01")
        self.assertEqual(dt.year, 2026)
        self.assertEqual(dt.month, 5)
        self.assertEqual(dt.day, 1)

    def test_parse_business_date_invalid_format_raises_error(self):
        with self.assertRaises(PipelineError):
            parse_business_date("01-05-2026")

    def test_anomesdia_for_converts_date(self):
        self.assertEqual(anomesdia_for("2026-05-01"), "20260501")

    def test_normalize_rejection_policy_applies_defaults(self):
        policy = normalize_rejection_policy("orders", {"domain": "transaction", "mapping_key": "x", "publish": {"destinations": ["billing"]}})
        self.assertEqual(policy["max_error_percent"], 1.0)
        self.assertEqual(policy["max_error_count"], 1000)
        self.assertIn("data-quality-events", policy["destinations"])

    def test_normalize_rejection_policy_accepts_max_values(self):
        policy = normalize_rejection_policy("orders", {"domain": "transaction", "mapping_key": "x", "publish": {"destinations": ["billing"]}, "rejection_policy": {"max_error_percent": 100, "max_error_count": 999999, "destinations": ["ops"]}})
        self.assertEqual(policy["max_error_percent"], 100.0)
        self.assertEqual(policy["max_error_count"], 999999)

    def test_normalize_rejection_policy_invalid_percent_raises_error(self):
        with self.assertRaisesRegex(PipelineError, "must be between 0 and 100"):
            normalize_rejection_policy("orders", {"domain": "transaction", "mapping_key": "x", "publish": {"destinations": ["billing"]}, "rejection_policy": {"max_error_percent": 101, "max_error_count": 10, "destinations": ["ops"]}})

    def test_rejection_threshold_exceeded_zero_total(self):
        policy = {"max_error_count": 100, "max_error_percent": 1.0}
        self.assertTrue(rejection_threshold_exceeded(0, 1, policy))

    def test_rejection_threshold_exceeded_boundary(self):
        policy = {"max_error_count": 10, "max_error_percent": 50.0}
        self.assertFalse(rejection_threshold_exceeded(10, 5, policy))
        self.assertTrue(rejection_threshold_exceeded(10, 11, policy))

    def test_validate_product_config_missing_mapping_key_raises_error(self):
        with self.assertRaisesRegex(PipelineError, "missing mapping_key"):
            validate_product_config("orders", {"domain": "transaction", "publish": {"destinations": ["billing"]}})

    def test_validate_product_config_missing_destinations_raises_error(self):
        with self.assertRaisesRegex(PipelineError, "missing publish destinations"):
            validate_product_config("orders", {"domain": "transaction", "mapping_key": "x"})

    def test_rejected_key_for_constructs_correct_path(self):
        event = {"product": "orders", "business_date": "2026-05-01"}
        path = rejected_key_for(event, "run-abc", "orders", "harmonization")
        self.assertIn("rejected/harmonization/orders/", path)
        self.assertIn("run-abc", path)
        self.assertIn("orders.jsonl", path)

    def test_rejection_record_has_expected_structure(self):
        event = {"product": "orders", "business_date": "2026-05-01", "file_name": "orders.csv"}
        record = rejection_record(event, "run-1", "harmonization", 5, "missing field", {"id": "123"})
        self.assertEqual(record["run_id"], "run-1")
        self.assertEqual(record["stage"], "harmonization")
        self.assertEqual(record["product"], "orders")
        self.assertEqual(record["row_number"], 5)
        self.assertEqual(record["reason"], "missing field")
        self.assertEqual(record["raw_row"], {"id": "123"})

    def test_run_id_for_has_timestamp_and_hash(self):
        result = run_id_for({"product": "orders", "file_name": "test.csv"})
        self.assertRegex(result, r"^\d{14}-[a-f0-9]{8}$")

    def test_product_from_file_key_curated_path(self):
        self.assertEqual(product_from_file_key("curated/orders/.../file.parquet"), "orders")

    def test_product_from_file_key_rejected_path(self):
        self.assertEqual(product_from_file_key("rejected/harmonization/orders/.../file.jsonl"), "orders")

    def test_product_from_file_key_invalid_raises_error(self):
        with self.assertRaises(ValueError):
            product_from_file_key("unknown/prefix/file.csv")

    def test_run_id_from_file_key_curated_path(self):
        rid = run_id_from_file_key("curated/orders/year=2026/month=05/day=01/run-123/orders.parquet")
        self.assertEqual(rid, "run-123")

    def test_run_id_from_file_key_rejected_path(self):
        rid = run_id_from_file_key("rejected/harmonization/orders/year=2026/month=05/day=01/run-456/orders.jsonl")
        self.assertEqual(rid, "run-456")

    def test_s3_records_parses_s3_notification_event(self):
        msg = {"Records": [{"s3": {"bucket": {"name": "data-lake"}, "object": {"key": "curated/orders/file.parquet"}}}]}
        records = s3_records(msg)
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["bucket"], "data-lake")
        self.assertEqual(records[0]["key"], "curated/orders/file.parquet")

    def test_s3_records_parses_direct_message(self):
        records = s3_records({"bucket": "data-lake", "key": "curated/orders/file.parquet"})
        self.assertEqual(len(records), 1)

    def test_s3_records_test_event_returns_empty(self):
        self.assertEqual(s3_records({"Event": "s3:TestEvent"}), [])

    def test_sanitize_fragment_replaces_special_chars(self):
        self.assertEqual(sanitize_fragment("hello world!"), "hello-world")

    def test_sanitize_fragment_strips_leading_trailing_dashes(self):
        self.assertEqual(sanitize_fragment("--hello--"), "hello")

    def test_sanitize_fragment_empty_returns_item(self):
        self.assertEqual(sanitize_fragment(""), "item")

    def test_analytics_dimensions_uses_ingestion_id_from_context(self):
        dims = analytics_dimensions({"ingestion_id": "ing-1", "business_date": "2026-05-01"})
        self.assertEqual(dims["ingestion_id"], "ing-1")
        self.assertEqual(dims["correlation_id"], "ing-1")

    def test_analytics_dimensions_fallback_to_run_id(self):
        dims = analytics_dimensions({"business_date": "2026-05-01"}, run_id="fallback-run")
        self.assertEqual(dims.get("ingestion_id"), "fallback-run")

    def test_evidence_status_fail_when_any_step_fails(self):
        ev = Evidence("r1", "orders", "f.csv", "2026-05-01")
        ev.ok("step1", "done")
        ev.fail("step2", "error")
        d = ev.as_dict()
        self.assertEqual(d["status"], "FAIL")

    def test_write_local_report_creates_file(self):
        ev = Evidence("r1", "orders", "f.csv", "2026-05-01")
        ev.ok("step1", "done")
        path = write_local_report(ev.as_dict())
        self.assertTrue(path.exists())
        content = json.loads(path.read_text())
        self.assertEqual(content["run_id"], "r1")
        path.unlink()

    def test_enrich_row_adds_domain_and_product(self):
        row = {"transaction_id": "t1", "customer_id": "c1", "amount": "10.0", "transaction_date": "2026-05-01"}
        event = {"product": "orders", "business_date": "2026-05-01"}
        result = enrich_row(row, event)
        self.assertEqual(result["domain"], "transaction")
        self.assertEqual(result["product"], "orders")
        self.assertEqual(result["business_date"], "2026-05-01")
        self.assertIn("enriched_at", result)

    def test_run_landing_missing_source_raises_error(self):
        objects = {}
        aws = SimpleNamespace(s3=FakeS3(objects))
        event = {"product": "orders", "file_name": "missing.csv", "business_date": "2026-05-01"}
        with self.assertRaises(Exception):
            run_landing(event, aws, "run-1")


if __name__ == "__main__":
    unittest.main()

import json
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from aws_local import (
    DATA_BUCKET,
    DOMAIN_REQUIRED_FIELDS,
    PipelineError,
    RejectedRowsThresholdError,
    load_json,
    normalize_rejection_policy,
    partition_prefix,
    read_csv_text,
    rejection_threshold_exceeded,
    validate_mapping,
)
from evidence import Evidence, evidence_table
from ecs_worker import process_message
from enrichment_batch import run_enrichment
from glue_harmonization import run_harmonization
from local_sfn_runner import validate_event


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


if __name__ == "__main__":
    unittest.main()

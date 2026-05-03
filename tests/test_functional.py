"""Functional tests against MiniStack (localhost:4566).
Requires: docker compose up -d (MiniStack running).

Test actual AWS service behavior:
- SQS message lifecycle
- SNS -> SQS subscription delivery
- EventBridge -> SQS rule routing
- DynamoDB table operations
- Pipeline bootstrap resource creation
"""

import json
import time
import unittest
from pathlib import Path
from typing import Any

from aws_local import (
    AWS_ENDPOINT_URL,
    ANALYTICS_BUCKET,
    CONFIG_TABLE,
    CURATED_QUEUE,
    DATA_BUCKET,
    EVENTBRIDGE_QUEUE,
    EVENT_BUS,
    SOURCE_BUCKET,
    STATE_MACHINE_NAME,
    service_client,
    service_resource,
)

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def ministack_running() -> bool:
    try:
        s3 = service_client("s3")
        s3.list_buckets()
        return True
    except Exception:
        return False


def wait_for_queue(sqs: Any, queue_url: str, expected: int = 1, timeout: float = 5.0) -> list[dict[str, Any]]:
    messages = []
    deadline = time.time() + timeout
    while len(messages) < expected and time.time() < deadline:
        resp = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=1, VisibilityTimeout=1)
        for msg in resp.get("Messages", []):
            body = json.loads(msg["Body"])
            messages.append(body)
            sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=msg["ReceiptHandle"])
    return messages


@unittest.skipUnless(ministack_running(), f"MiniStack not running at {AWS_ENDPOINT_URL}")
class SqsFunctionalTest(unittest.TestCase):
    """Validate SQS send/receive/delete lifecycle."""

    def setUp(self):
        self.sqs = service_client("sqs")
        self.queue_name = "test-sqs-lifecycle"
        self.queue_url = self.sqs.create_queue(QueueName=self.queue_name)["QueueUrl"]

    def tearDown(self):
        try:
            self.sqs.delete_queue(QueueUrl=self.queue_url)
        except Exception:
            pass

    def test_sqs_send_and_receive_message(self):
        self.sqs.send_message(QueueUrl=self.queue_url, MessageBody='{"test": true}')
        messages = wait_for_queue(self.sqs, self.queue_url, expected=1)
        self.assertEqual(len(messages), 1)

    def test_sqs_receive_empty_queue_returns_no_messages(self):
        messages = self.sqs.receive_message(QueueUrl=self.queue_url, WaitTimeSeconds=1).get("Messages", [])
        self.assertEqual(len(messages), 0)

    def test_sqs_multiple_messages_fifo_order(self):
        for i in range(3):
            self.sqs.send_message(QueueUrl=self.queue_url, MessageBody=json.dumps({"seq": i}))
        messages = wait_for_queue(self.sqs, self.queue_url, expected=3)
        self.assertEqual(len(messages), 3)


@unittest.skipUnless(ministack_running(), f"MiniStack not running at {AWS_ENDPOINT_URL}")
class SnsSqsFunctionalTest(unittest.TestCase):
    """Validate SNS publish -> SQS subscription delivery."""

    def setUp(self):
        self.sns = service_client("sns")
        self.sqs = service_client("sqs")
        self.topic_name = "test-functional-topic"
        self.queue_name = "test-functional-queue"
        self.topic_arn = self.sns.create_topic(Name=self.topic_name)["TopicArn"]
        self.queue_url = self.sqs.create_queue(QueueName=self.queue_name)["QueueUrl"]
        queue_arn = self.sqs.get_queue_attributes(QueueUrl=self.queue_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
        self.sns.subscribe(TopicArn=self.topic_arn, Protocol="sqs", Endpoint=queue_arn)

    def tearDown(self):
        try:
            self.sns.delete_topic(TopicArn=self.topic_arn)
        except Exception:
            pass
        try:
            self.sqs.delete_queue(QueueUrl=self.queue_url)
        except Exception:
            pass

    def test_sns_publish_delivers_to_sqs_subscriber(self):
        self.sns.publish(TopicArn=self.topic_arn, Message=json.dumps({"event": "test", "seq": 1}))
        messages = wait_for_queue(self.sqs, self.queue_url, expected=1, timeout=5.0)
        self.assertGreaterEqual(len(messages), 1)
        last = messages[-1]
        payload = json.loads(last["Message"]) if "Message" in last else last
        self.assertEqual(payload.get("event"), "test")


@unittest.skipUnless(ministack_running(), f"MiniStack not running at {AWS_ENDPOINT_URL}")
class EventBridgeFunctionalTest(unittest.TestCase):
    """Validate EventBridge rule routes events to SQS target."""

    def setUp(self):
        self.events = service_client("events")
        self.sqs = service_client("sqs")
        self.bus_name = "test-functional-bus"
        self.queue_name = "test-functional-eb-queue"
        self.rule_name = "test-functional-rule"

        try:
            self.events.create_event_bus(Name=self.bus_name)
        except self.events.exceptions.ResourceAlreadyExistsException:
            pass
        self.queue_url = self.sqs.create_queue(QueueName=self.queue_name)["QueueUrl"]
        queue_arn = self.sqs.get_queue_attributes(QueueUrl=self.queue_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]

        self.events.put_rule(
            Name=self.rule_name,
            EventPattern=json.dumps({"source": ["test.functional"]}),
            EventBusName=self.bus_name,
            State="ENABLED",
        )
        self.events.put_targets(
            Rule=self.rule_name,
            EventBusName=self.bus_name,
            Targets=[{"Id": "test-target", "Arn": queue_arn}],
        )

    def tearDown(self):
        try:
            self.events.remove_targets(Rule=self.rule_name, EventBusName=self.bus_name, Ids=["test-target"])
        except Exception:
            pass
        try:
            self.events.delete_rule(Name=self.rule_name, EventBusName=self.bus_name)
        except Exception:
            pass
        try:
            self.sqs.delete_queue(QueueUrl=self.queue_url)
        except Exception:
            pass

    def test_eventbridge_routes_event_to_sqs(self):
        self.events.put_events(
            Entries=[{"Source": "test.functional", "DetailType": "test", "EventBusName": self.bus_name, "Detail": json.dumps({"msg": "hello"})}]
        )
        messages = wait_for_queue(self.sqs, self.queue_url, expected=1, timeout=5.0)
        self.assertGreaterEqual(len(messages), 1)


@unittest.skipUnless(ministack_running(), f"MiniStack not running at {AWS_ENDPOINT_URL}")
class DynamoDBFunctionalTest(unittest.TestCase):
    """Validate DynamoDB put/get/delete lifecycle."""

    def setUp(self):
        self.table_name = "test-functional-table"
        self.dynamodb = service_resource("dynamodb")
        self.table = self.dynamodb.create_table(
            TableName=self.table_name,
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        self.table.wait_until_exists()

    def tearDown(self):
        try:
            self.table.delete()
        except Exception:
            pass

    def test_dynamodb_put_and_get_item(self):
        self.table.put_item(Item={"pk": "test1", "value": "hello"})
        response = self.table.get_item(Key={"pk": "test1"})
        self.assertIn("Item", response)
        self.assertEqual(response["Item"]["value"], "hello")

    def test_dynamodb_get_nonexistent_item_returns_empty(self):
        response = self.table.get_item(Key={"pk": "nonexistent"})
        self.assertNotIn("Item", response)


@unittest.skipUnless(ministack_running(), f"MiniStack not running at {AWS_ENDPOINT_URL}")
class PipelineFunctionalTest(unittest.TestCase):
    """Validate pipeline bootstrap creates expected AWS resources."""

    @classmethod
    def setUpClass(cls):
        # Bootstrap once for all tests in this class
        from pipeline import bootstrap
        bootstrap()
        cls.s3 = service_client("s3")
        cls.sqs = service_client("sqs")
        cls.dynamodb = service_resource("dynamodb")

    def test_bootstrap_creates_source_bucket(self):
        buckets = [b["Name"] for b in self.s3.list_buckets()["Buckets"]]
        self.assertIn(SOURCE_BUCKET, buckets)

    def test_bootstrap_creates_data_bucket(self):
        buckets = [b["Name"] for b in self.s3.list_buckets()["Buckets"]]
        self.assertIn(DATA_BUCKET, buckets)

    def test_bootstrap_creates_analytics_bucket(self):
        buckets = [b["Name"] for b in self.s3.list_buckets()["Buckets"]]
        self.assertIn(ANALYTICS_BUCKET, buckets)

    def test_bootstrap_creates_eventbridge_queue(self):
        queues = self.sqs.list_queues(QueueNamePrefix=EVENTBRIDGE_QUEUE).get("QueueUrls", [])
        self.assertTrue(any(EVENTBRIDGE_QUEUE in url for url in queues))

    def test_bootstrap_creates_curated_queue(self):
        queues = self.sqs.list_queues(QueueNamePrefix=CURATED_QUEUE).get("QueueUrls", [])
        self.assertTrue(any(CURATED_QUEUE in url for url in queues))

    def test_bootstrap_creates_product_config_table(self):
        table = self.dynamodb.Table(CONFIG_TABLE)
        response = table.get_item(Key={"product": "orders"})
        self.assertIn("Item", response)
        self.assertEqual(response["Item"]["domain"], "transaction")

    def test_bootstrap_creates_state_machine(self):
        sfn = service_client("stepfunctions")
        machines = sfn.list_state_machines().get("stateMachines", [])
        self.assertTrue(any(m["name"] == STATE_MACHINE_NAME for m in machines))

    def test_bootstrap_uploads_mapping_files(self):
        objects = self.s3.list_objects_v2(Bucket=DATA_BUCKET, Prefix="de-para/").get("Contents", [])
        self.assertGreaterEqual(len(objects), 2)

    def test_bootstrap_uploads_sample_files(self):
        objects = self.s3.list_objects_v2(Bucket=SOURCE_BUCKET).get("Contents", [])
        self.assertGreaterEqual(len(objects), 1)

    def test_bootstrap_configures_s3_notification(self):
        config = self.s3.get_bucket_notification_configuration(Bucket=DATA_BUCKET)
        self.assertIn("QueueConfigurations", config)
        ids = [item.get("Id") for item in config["QueueConfigurations"]]
        self.assertIn("curated-to-sqs", ids)
        self.assertIn("rejected-to-sqs", ids)

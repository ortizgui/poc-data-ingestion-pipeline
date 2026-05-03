# Pattern: Boto3 Local Clients

## Description

Create AWS clients and resources with the configured local endpoint so the same Boto3 service APIs can be used against MiniStack.

## When to Use

Use this pattern when a script needs to access emulated AWS services during local execution or tests against MiniStack.

## Pattern

Keep endpoint, region, and resource names centralized. Build clients with explicit local credentials and `endpoint_url`.

## Example

```python
AWS_ENDPOINT_URL = os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")


def clients() -> AwsClients:
    kwargs = {
        "endpoint_url": AWS_ENDPOINT_URL,
        "region_name": AWS_REGION,
        "aws_access_key_id": "test",
        "aws_secret_access_key": "test",
    }
    return AwsClients(
        s3=boto3.client("s3", **kwargs),
        dynamodb=boto3.resource("dynamodb", **kwargs),
        sqs=boto3.client("sqs", **kwargs),
        events=boto3.client("events", **kwargs),
        sfn=boto3.client("stepfunctions", **kwargs),
    )
```

## Files Using This Pattern

- `aws_local.py` - centralizes default endpoint, region, buckets, queues, and shared clients.
- `local_eventbridge_runner.py` - creates local SQS clients for polling EventBridge target messages.
- `ecs_worker.py` - creates local SQS, SNS, S3, and DynamoDB clients/resources.
- `e2e_test.py` - creates local clients to validate resources and cleanup state.

## Related

- [Decision: Local AWS Emulation](../../decisions/002-local-aws-emulation.md)
- [Decision: Tech Stack](../../decisions/001-tech-stack.md)
- [Feature: Local Execution And Evidence](../../intent/feature-local-execution-and-evidence.md)

## Status

- **Created**: 2026-05-02
- **Status**: Active

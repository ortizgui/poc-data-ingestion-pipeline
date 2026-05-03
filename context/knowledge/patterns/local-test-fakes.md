# Pattern: Local Test Fakes

## Description

Use small in-memory fake AWS resources to unit test pipeline behavior without requiring MiniStack.

## When to Use

Use this pattern for unit tests that validate business rules, validation, mapping, rejection behavior, and worker message handling.

## Pattern

Define focused fake classes that implement only the AWS methods needed by the unit under test. Store objects and messages in memory, then assert side effects directly.

## Example

```python
class FakeS3:
    def __init__(self, objects):
        self.objects = objects

    def get_object(self, Bucket, Key):
        return {"Body": FakeBody(self.objects[(Bucket, Key)])}

    def put_object(self, Bucket, Key, Body):
        self.objects[(Bucket, Key)] = Body.decode("utf-8")
```

```python
class FakeSns:
    def __init__(self):
        self.messages = []

    def publish(self, TopicArn, Message):
        self.messages.append(json.loads(Message))
```

## Files Using This Pattern

- `tests/test_pipeline.py` - uses fake DynamoDB, S3, SNS, and patching to test critical pipeline behavior.

## Related

- [Decision: Tech Stack](../../decisions/001-tech-stack.md)
- [Decision: Local AWS Emulation](../../decisions/002-local-aws-emulation.md)
- [Feature: Local Execution And Evidence](../../intent/feature-local-execution-and-evidence.md)

## Status

- **Created**: 2026-05-02
- **Status**: Active

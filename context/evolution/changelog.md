# Changelog

## [Documentation] - Agent Rules Restored

### Changed

- Restored AI-first development philosophy, simplicity principles, lightweight architecture guidance, and file-organization rules in `AGENTS.md`.
- Added persistent caveman response style rules to `AGENTS.md`.

---

*Updated: 2026-05-02*

## [Current State] - Context Mesh Added

### Existing Features (documented)

- Event-driven ingestion - product file-ready events start ingestion runs.
- Config-driven product onboarding - product setup and mappings live outside pipeline core.
- Business data lake processing - files move through raw, processed, and curated stages.
- Rejected-record handling - invalid rows are separated and threshold policies can fail runs.
- Downstream event publication - curated, rejected, and failed-file events are published to destinations.
- Operational analytics and audit - runs and enriched steps are written for observability.
- Catalog and query layer - business and analytics datasets are cataloged and queryable.
- Local execution and evidence - local commands and tests validate the POC and write evidence.

### Tech Stack (documented)

- Python 3 scripts
- Boto3 and Botocore
- MiniStack through Docker Compose
- DuckDB
- AWS-style S3, DynamoDB, EventBridge, Step Functions, SQS, SNS, and Glue Data Catalog APIs
- JSON and JSONL data/configuration files
- Python `unittest`

### Patterns Identified

- Boto3 local clients
- ASL resource dispatch
- S3 partitioned keys
- Analytics step emission
- Product mapping validation
- Rejection threshold handling
- Downstream message publishing
- Local test fakes

---

*Context Mesh added: 2026-05-02*
*This changelog documents the state when Context Mesh was added.*
*Future changes will be tracked below.*

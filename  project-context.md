# Project Context

## Goal

POC evolve from business-only ingestion to business + operational analytics.

Keep:

- generic pipeline for N products;
- low product customization;
- support for files with millions of rows;
- simple architecture;
- low cost;
- traceability;
- this file as source of truth.

Business flow still write `raw`, `processed`, `curated`, `rejected`.
Analytics flow now write observability, quality, audit datasets to dedicated S3 analytics bucket.

## Tooling Rules

Mandatory unless user explicitly says otherwise.

- Always use `/caveman` before executing task.
- Apply caveman preprocessing to all user inputs.
- Prefer caveman for token optimization before calling any model.
- Keep responses terse, high-signal, low-token.
- Keep code, commands, paths, schemas exact.
- All edits to this file must be written in caveman style (no articles, fragments OK, short synonyms, technical terms exact).

## Local Execution Policy

- Simulate AWS via MiniStack to maximum extent possible.
- Use `boto3` against `localhost:4566` for all AWS services MiniStack supports (S3, DynamoDB, SQS, SNS, EventBridge, Step Functions, Glue, STS).
- Simulate Athena via DuckDB: load analytics/business lake S3 files into in-memory DuckDB, run SQL queries that mirror `athena_views.sql`.
- Only use local alternatives (scripts, file I/O) when MiniStack lacks the service or the emulation is too incomplete to be useful.
- Document any local-only deviation clearly so real-AWS path remains obvious.

## Target Flow

`EventBridge -> Step Functions(InitExecution -> ValidateEvent -> Landing -> Harmonization -> Enrichment -> S3Notification -> FinalizeExecution) -> DynamoDB -> S3 analytics -> Glue Data Catalog -> Athena -> QuickSight`

Downstream: `S3 ObjectCreated(curated/rejected) -> SQS curated-files -> ECS PublishEvents -> SNS -> SQS destino`

## Main Components

### EventBridge

- receive `file-ready` event;
- start pipeline execution;
- can receive `ingestion_id` from upstream.

### Step Functions

- validate event;
- generate or propagate `ingestion_id`;
- use `execution_id` from execution context;
- derive `anomesdia` from business date;
- pass shared control fields to all jobs;
- orchestrate success/failure path;
- call finalizer;
- ensure failed runs also reach analytics layer.

### Glue Jobs

- process heavy files;
- write business data;
- write analytics facts;
- keep product rules out of pipeline core when possible.

### DynamoDB

- product config;
- fast operational status;
- quick lookup;
- not source for analytics dashboards;
- not place for millions of rejected rows.

### S3 Business Data Lake

- `raw`: source copy;
- `processed`: harmonized domain file;
- `curated`: enriched final file;
- `rejected`: detailed rejected rows and file-failed payloads.

### Downstream (SQS + ECS + SNS)

- S3 notification fires on `curated/*.parquet` and `rejected/*.jsonl`;
- both go to same SQS `curated-files`;
- ECS worker reads SQS, fetches product config from DynamoDB, reads S3 rows;
- publishes `domain_record_ready` to product publish destinations;
- publishes `ingestion.records-rejected` and `ingestion.file-failed` to rejection policy destinations.

### Analytics S3

Suggested bucket:

`s3://poc-data-ingestion-analytics-<env>/`

Purpose:

- observability;
- audit trail;
- quality summaries;
- Athena queries;
- QuickSight dashboards.

### Glue Data Catalog

Databases:

- `poc_data_ingestion_analytics`: 8 operational analytics tables.
- `poc_data_ingestion_business`: 4 business data lake tables (raw, processed, curated, rejected).

Registration:

- `glue_catalog.py` registers all tables during bootstrap.
- analytics tables use partition `anomesdia`; business tables use `year/month/day`.
- S3 prefix points: analytics bucket for observability, data-lake bucket for business.

### Athena

- query partitioned analytics tables;
- views defined in `athena_views.sql`:
  - `vw_ingestion_status_by_day`
  - `vw_ingestion_errors_by_product`
  - `vw_ingestion_duration_by_step`
  - `vw_rejections_by_reason`
  - `vw_data_quality_by_product`
  - `vw_schema_validation_by_product`
  - `vw_file_lineage_timeline`
  - `vw_execution_timeline`
  - `vw_curated_with_run_context` (join business curated + analytics runs)
  - `vw_rejected_with_context` (join business rejected + analytics runs)
  - `vw_error_detail_with_source` (individual errors with source file trace)
  - `vw_ingested_with_source` (curated records with full file lineage)
  - `vw_troubleshooting_dashboard` (unified run status + error/rejection flags)
- views join across both databases for business + operational context.

### QuickSight

- consume Athena datasets;
- build operational dashboards from views.

## Text Diagram

```text
Product/File Ready Event
  -> EventBridge
  -> Step Functions
     -> InitExecution
        -> set ingestion_id
        -> set execution_id
        -> set correlation_id
        -> set anomesdia
        -> write analytics execution_events (ingestion_started)
     -> ValidateEvent
        -> read DynamoDB ProductConfig
        -> validate product exists + config valid
     -> Landing Glue Job
        -> read source from product-lake S3
        -> write /raw
        -> write analytics step/lineage/event
     -> Harmonization Glue Job
        -> read de-para mapping from s3://data-lake/de-para/
        -> read /raw
        -> write /processed
        -> write rejected detail when needed
        -> write analytics step/error/rejection/quality/schema/lineage/event
     -> Enrichment Glue Job
        -> read /processed
        -> write /curated
        -> write rejected detail when needed
        -> write analytics step/error/rejection/quality/lineage/event
     -> CuratedS3Notification
        -> verify S3 notification config
     -> FinalizeExecution
        -> consolidate run status
        -> write analytics_ingestion_runs
        -> write manifest to S3
        -> on failure: also write analytics error + run FAILED
  -> (downstream) S3 ObjectCreated curated/*.parquet + rejected/*.jsonl
     -> SQS curated-files
     -> ECS PublishEvents worker
        -> read DynamoDB product destinations
        -> publish domain_record_ready / records-rejected / file-failed
        -> SNS -> SQS destino
  -> Glue Data Catalog
  -> Athena
  -> QuickSight
```

## Shared Control Fields

All pipeline steps must propagate:

- `ingestion_id`
- `execution_id`
- `correlation_id`
- `product`
- `domain`
- `source_bucket`
- `source_key`
- `file_name`
- `business_date`
- `anomesdia`

Rules:

- `anomesdia` mandatory in all analytics datasets;
- format `yyyyMMdd`;
- example `20260502`.

## Business Data Flow

1. product publishes `file-ready` event.
2. Step Functions starts execution.
3. landing job copies file to `raw`.
4. harmonization job maps product layout to domain layout, writes `processed`.
5. enrichment job writes `curated`.
6. invalid rows go to `rejected`.
7. curated or rejected notifications continue downstream path.

## Analytics Data Flow

1. InitExecution creates shared control context.
2. Each processing step writes step telemetry.
3. Quality and schema checks write summaries.
4. Rejection detail stays in business lake; rejection summary goes to analytics bucket.
5. Finalizer writes consolidated run record.
6. Athena reads analytics bucket.
7. QuickSight reads Athena views.

## Analytics Write Strategy

Keep analytics rules in one place.

Implementation rule:

- shared file `analytics_writer.py` owns analytics path logic, common dimensions, dataset mapping, reusable emit helpers.

Current reusable helpers:

- `analytics_dimensions()`
- `emit_ingestion_run()`
- `emit_ingestion_step()` — enriched: carries error, quality, schema, rejection, lineage fields in single row per step attempt.'

Reason:

- 2 emit functions cover all analytics facts;
- error/quality/schema/rejection/lineage merged into step row — no separate tables;

Guideline:

- new analytics dataset or common rule goes first to `analytics_writer.py`;
- job files should pass only local fields like counts, paths, errors, event type.

## Analytics Bucket Layout

```text
s3://poc-data-ingestion-analytics-<env>/
  observability/
    ingestion_runs/
      anomesdia=20260502/
    ingestion_steps/
      anomesdia=20260502/
```

Meaning:

- `observability/ingestion_runs`: 1 row per ingestion run — status, counts, paths, error summary.
- `observability/ingestion_steps`: 1 row per step attempt — enriched with error, quality, schema, rejection, lineage fields.

## Glue Catalog Tables

Databases:

- `poc_data_ingestion_analytics`: 2 enriched tables (runs + steps).
- `poc_data_ingestion_business`: 4 business tables (raw, processed, curated, rejected).

Tables:

- `analytics_ingestion_runs` — run-level with paths, counts, error summary.
- `analytics_ingestion_steps` — enriched step row with error, quality, schema, rejection, lineage fields.

All analytics tables partition by `anomesdia`. Business tables partition by `year/month/day`.

## Table Purpose + Grain

### analytics_ingestion_runs

- purpose: final run status + file traceability;
- grain: 1 row per `ingestion_id`.

### analytics_ingestion_steps (enriched)

- purpose: rich step telemetry with error, quality, schema, rejection, lineage embedded;
- grain: 1 row per step attempt.
- columns grouped in blocks:
  - **Core**: step_name, step_order, status, timing, record counts, I/O paths.
  - **Error**: error_type, error_code, error_message, error_category, source_bucket, source_key, payload_ref, occurred_at.
  - **Quality**: rule_name, rule_type, rule_result, valid/invalid/warning_records, threshold/measured values.
  - **Schema**: schema_name, version, validation_result, missing/unexpected columns, validation_message.
  - **Rejection**: rejection_reason, rejection_category, rejected_count_summary, rejection_percent, rejected_detail_path, sample_message.
  - **Lineage**: artifact_type, artifact_role, lineage_bucket/key/format, record_count, file_size, parent_bucket/key.

## Partition Strategy

Start simple.

- mandatory partition: `anomesdia`
- query pattern:

```sql
SELECT *
FROM analytics_ingestion_runs
WHERE anomesdia = '20260502';
```

Extra partitions like `product`, `domain`, `status` only when query volume justifies more complexity.

## Athena + QuickSight

Athena queries across both Glue databases:

- `poc_data_ingestion_analytics` for operational observability.
- `poc_data_ingestion_business` for curated enriched data + rejected detail.

Views already defined in `athena_views.sql`:

- `vw_ingestion_status_by_day`
- `vw_ingestion_errors_by_product`
- `vw_ingestion_duration_by_step`
- `vw_rejections_by_reason`
- `vw_data_quality_by_product`
- `vw_schema_validation_by_product`
- `vw_file_lineage_timeline`
- `vw_execution_timeline`
- `vw_curated_with_run_context`
- `vw_rejected_with_context`
- `vw_error_detail_with_source` — individual errors + source file trace
- `vw_ingested_with_source` — curated records + full file lineage
- `vw_troubleshooting_dashboard` — unified run status + error/rejection flags

QuickSight uses Athena datasets from these views for operational dashboards.

## Decisions

### DynamoDB + S3

- DynamoDB for fast operational lookup.
- S3 for cheap historical analytics.
- separation reduces coupling and cost.

### Parquet

- better Athena performance;
- lower scan cost;
- fit for summaries and dashboards.

### Rejection Detail in S3

- high-volume reject detail fits S3;
- analytics tables keep summary only;
- dashboard stays cheap.

### Generic Analytics Model

- same structure for many products;
- low coupling between product rule and pipeline telemetry;
- easier reuse.

## Best Practices

- do not write millions of rejects to DynamoDB;
- write reject detail to S3;
- write reject summary to analytics tables;
- keep `ingestion_id`, `execution_id`, `correlation_id` everywhere;
- avoid many tiny files;
- consolidate writes when possible;
- evolve schema carefully;
- keep idempotency by `ingestion_id` + file reference;
- version DDL and views as code;
- apply lifecycle to analytics bucket.

## Repo Mapping

- `pipeline.py`: bootstrap + event publish.
- `state-machine.asl.json`: local flow.
- `local_sfn_runner.py`: local orchestration.
- `local_eventbridge_runner.py`: consumes EventBridge target queue, starts SFN.
- `glue_landing.py`: landing step.
- `glue_harmonization.py`: harmonization step.
- `enrichment_batch.py`: enrichment step.
- `analytics_writer.py`: shared analytics emit rules.
- `analytics_queries.py`: DuckDB local Athena simulation - loads S3 analytics/business files and runs operational queries.
- `glue_catalog.py`: Glue Data Catalog DDL bootstrap (analytics + business databases/tables).
- `athena_views.sql`: 10 Athena operational views for QuickSight dashboards.
- `ecs_worker.py`: consumes SQS curated-files, reads S3, publishes SNS.
- `evidence.py`: local evidence output.
- `e2e_test.py`: end-to-end test against MiniStack.

## Success

Success means:

- business flow still works (curated + rejected downstream);
- each ingestion writes 2 analytics tables (runs + enriched steps);
- 1 step row tells full story (status + error + quality + schema + rejection + lineage);
- failures also appear in analytics;
- Glue Data Catalog tables registered for analytics + business lake;
- Athena can query by `anomesdia` across both databases;
- troubleshooting views join step fields with run context;
- QuickSight can build operational dashboards;
- new products need low customization.

## Next Steps

1. move local pseudo-Parquet to real Parquet.
2. validate file sizing and compaction.
3. decide whether file keeps leading-space name or gets renamed.

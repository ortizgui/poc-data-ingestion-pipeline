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
- `emit_ingestion_step()`
- `emit_error_event()`
- `emit_rejection_summary()`
- `emit_data_quality_summary()`
- `emit_schema_validation()`
- `emit_file_lineage_event()`
- `emit_execution_event()`

Reason:

- avoid repeating `ingestion_id`, `execution_id`, `correlation_id`, `product`, `domain`, `anomesdia` in every flow;
- keep analytics folder routing in one place;
- reduce code drift between landing, harmonization, enrichment, finalizer;
- simplify future table changes.

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
    ingestion_errors/
      anomesdia=20260502/
    ingestion_rejections/
      anomesdia=20260502/
  quality/
    data_quality_summary/
      anomesdia=20260502/
    schema_validation/
      anomesdia=20260502/
  audit/
    file_lineage/
      anomesdia=20260502/
    execution_events/
      anomesdia=20260502/
```

Meaning:

- `observability/ingestion_runs`: 1 row per ingestion run.
- `observability/ingestion_steps`: 1 row per step attempt.
- `observability/ingestion_errors`: relevant technical/functional errors.
- `observability/ingestion_rejections`: rejection summary, not full rejected detail.
- `quality/data_quality_summary`: rule results and metrics.
- `quality/schema_validation`: schema/contract validation result.
- `audit/file_lineage`: artifact lineage.
- `audit/execution_events`: fine-grained execution trail.

## Glue Catalog Tables

Database:

`poc_data_ingestion_analytics`

Tables:

- `analytics_ingestion_runs`
- `analytics_ingestion_steps`
- `analytics_ingestion_errors`
- `analytics_ingestion_rejections_summary`
- `analytics_data_quality_summary`
- `analytics_schema_validation`
- `analytics_file_lineage`
- `analytics_execution_events`

All tables partition by `anomesdia`.

## Table Purpose + Grain

### analytics_ingestion_runs

- purpose: final run status;
- grain: 1 row per `ingestion_id`.

### analytics_ingestion_steps

- purpose: step telemetry;
- grain: 1 row per `ingestion_id + step_name + attempt`.

### analytics_ingestion_errors

- purpose: error diagnostics;
- grain: 1 row per relevant error.

### analytics_ingestion_rejections_summary

- purpose: rejection dashboard;
- grain: 1 row per `ingestion_id + step_name + rejection_reason`.

### analytics_data_quality_summary

- purpose: quality metrics;
- grain: 1 row per rule evaluation.

### analytics_schema_validation

- purpose: schema/contract validation;
- grain: 1 row per validation execution.

### analytics_file_lineage

- purpose: artifact traceability;
- grain: 1 row per artifact.

### analytics_execution_events

- purpose: technical event history;
- grain: 1 row per execution event.

## Core Schemas

### analytics_ingestion_runs

- `ingestion_id string`
- `execution_id string`
- `correlation_id string`
- `product string`
- `domain string`
- `source_system string`
- `source_bucket string`
- `source_key string`
- `source_file_name string`
- `source_file_etag string`
- `status string`
- `failure_step string`
- `started_at timestamp`
- `finished_at timestamp`
- `duration_seconds bigint`
- `total_records bigint`
- `processed_records bigint`
- `rejected_records bigint`
- `error_records bigint`
- `raw_path string`
- `processed_path string`
- `curated_path string`
- `rejected_path string`
- `error_message string`
- `anomesdia string`

### analytics_ingestion_steps

- `ingestion_id string`
- `execution_id string`
- `correlation_id string`
- `product string`
- `domain string`
- `step_name string`
- `step_order int`
- `attempt int`
- `glue_job_name string`
- `glue_job_run_id string`
- `status string`
- `started_at timestamp`
- `finished_at timestamp`
- `duration_seconds bigint`
- `input_records bigint`
- `output_records bigint`
- `rejected_records bigint`
- `error_records bigint`
- `input_path string`
- `output_path string`
- `error_message string`
- `anomesdia string`

### analytics_ingestion_errors

- `ingestion_id string`
- `execution_id string`
- `correlation_id string`
- `product string`
- `domain string`
- `step_name string`
- `error_type string`
- `error_code string`
- `error_message string`
- `error_category string`
- `is_retryable boolean`
- `glue_job_name string`
- `glue_job_run_id string`
- `source_bucket string`
- `source_key string`
- `payload_ref string`
- `occurred_at timestamp`
- `anomesdia string`

### analytics_ingestion_rejections_summary

- `ingestion_id string`
- `execution_id string`
- `correlation_id string`
- `product string`
- `domain string`
- `step_name string`
- `rejection_reason string`
- `rejection_category string`
- `rejected_count bigint`
- `total_step_records bigint`
- `rejection_percent double`
- `rejected_detail_path string`
- `sample_message string`
- `occurred_at timestamp`
- `anomesdia string`

### analytics_data_quality_summary

- `ingestion_id string`
- `execution_id string`
- `correlation_id string`
- `product string`
- `domain string`
- `step_name string`
- `rule_name string`
- `rule_type string`
- `rule_result string`
- `total_records bigint`
- `valid_records bigint`
- `invalid_records bigint`
- `warning_records bigint`
- `threshold_value string`
- `measured_value string`
- `details string`
- `measured_at timestamp`
- `anomesdia string`

### analytics_schema_validation

- `ingestion_id string`
- `execution_id string`
- `correlation_id string`
- `product string`
- `domain string`
- `step_name string`
- `schema_name string`
- `schema_version string`
- `validation_result string`
- `missing_columns string`
- `unexpected_columns string`
- `invalid_types string`
- `validation_message string`
- `validated_at timestamp`
- `anomesdia string`

### analytics_file_lineage

- `ingestion_id string`
- `execution_id string`
- `correlation_id string`
- `product string`
- `domain string`
- `artifact_type string`
- `artifact_role string`
- `bucket string`
- `s3_key string`
- `format string`
- `record_count bigint`
- `file_size_bytes bigint`
- `parent_bucket string`
- `parent_key string`
- `created_at timestamp`
- `anomesdia string`

### analytics_execution_events

- `ingestion_id string`
- `execution_id string`
- `correlation_id string`
- `product string`
- `domain string`
- `step_name string`
- `event_type string`
- `event_source string`
- `event_message string`
- `event_payload_ref string`
- `event_at timestamp`
- `anomesdia string`

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
- `glue_catalog.py`: Glue Data Catalog DDL bootstrap (analytics + business databases/tables).
- `athena_views.sql`: 10 Athena operational views for QuickSight dashboards.
- `ecs_worker.py`: consumes SQS curated-files, reads S3, publishes SNS.
- `evidence.py`: local evidence output.
- `e2e_test.py`: end-to-end test against MiniStack.

## Success

Success means:

- business flow still works (curated + rejected downstream);
- each ingestion writes analytics facts to all 8 datasets;
- failures also appear in analytics;
- Glue Data Catalog tables registered for analytics + business lake;
- Athena can query by `anomesdia` across both databases;
- 10 operational views join analytics + business data;
- QuickSight can build operational dashboards;
- new products need low customization.

## Next Steps

1. move local pseudo-Parquet to real Parquet.
2. validate file sizing and compaction.
3. decide whether file keeps leading-space name or gets renamed.

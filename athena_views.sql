-- Athena views for operational dashboards
-- Run against poc_data_ingestion_analytics database
-- Pre-requisite: Glue Data Catalog tables registered via glue_catalog.py

-- Daily ingestion status overview
CREATE OR REPLACE VIEW vw_ingestion_status_by_day AS
SELECT
    anomesdia,
    product,
    domain,
    status,
    COUNT(*) AS total_runs,
    COALESCE(SUM(total_records), 0) AS total_records,
    COALESCE(SUM(processed_records), 0) AS processed_records,
    COALESCE(SUM(rejected_records), 0) AS rejected_records,
    COALESCE(AVG(duration_seconds), 0) AS avg_duration_seconds
FROM analytics_ingestion_runs
GROUP BY anomesdia, product, domain, status;

-- Errors grouped by product and step for IT diagnostics
CREATE OR REPLACE VIEW vw_ingestion_errors_by_product AS
SELECT
    anomesdia,
    product,
    step_name,
    error_type,
    error_category,
    COUNT(*) AS error_count,
    MIN(occurred_at) AS first_occurrence,
    MAX(occurred_at) AS last_occurrence
FROM analytics_ingestion_errors
GROUP BY anomesdia, product, step_name, error_type, error_category;

-- Step duration metrics for performance monitoring
CREATE OR REPLACE VIEW vw_ingestion_duration_by_step AS
SELECT
    anomesdia,
    product,
    step_name,
    COUNT(*) AS step_count,
    COALESCE(AVG(duration_seconds), 0) AS avg_duration_seconds,
    COALESCE(MAX(duration_seconds), 0) AS max_duration_seconds,
    COALESCE(SUM(input_records), 0) AS total_input_records
FROM analytics_ingestion_steps
WHERE status = 'SUCCEEDED'
GROUP BY anomesdia, product, step_name;

-- Rejection analysis by reason for business quality reviews
CREATE OR REPLACE VIEW vw_rejections_by_reason AS
SELECT
    anomesdia,
    product,
    step_name,
    rejection_reason,
    rejection_category,
    SUM(rejected_count) AS total_rejected,
    AVG(rejection_percent) AS avg_rejection_percent,
    COUNT(*) AS occurrence_count,
    MIN(rejected_detail_path) AS rejected_detail_path,
    MIN(sample_message) AS sample_message
FROM analytics_ingestion_rejections_summary
GROUP BY anomesdia, product, step_name, rejection_reason, rejection_category;

-- Data quality metrics for dashboard and SLA tracking
CREATE OR REPLACE VIEW vw_data_quality_by_product AS
SELECT
    anomesdia,
    product,
    step_name,
    rule_name,
    rule_type,
    rule_result,
    SUM(total_records) AS total_records,
    SUM(valid_records) AS valid_records,
    SUM(invalid_records) AS invalid_records,
    SUM(warning_records) AS warning_records
FROM analytics_data_quality_summary
GROUP BY anomesdia, product, step_name, rule_name, rule_type, rule_result;

-- Schema validation results for contract monitoring
CREATE OR REPLACE VIEW vw_schema_validation_by_product AS
SELECT
    anomesdia,
    product,
    step_name,
    schema_name,
    validation_result,
    COUNT(*) AS validation_count,
    MIN(validated_at) AS first_validated_at,
    MAX(validated_at) AS last_validated_at
FROM analytics_schema_validation
GROUP BY anomesdia, product, step_name, schema_name, validation_result;

-- File lineage for audit trail
CREATE OR REPLACE VIEW vw_file_lineage_timeline AS
SELECT
    anomesdia,
    product,
    artifact_type,
    s3_key,
    format,
    record_count,
    file_size_bytes,
    parent_key,
    created_at
FROM analytics_file_lineage
WHERE artifact_type IN ('raw', 'processed', 'curated')
ORDER BY anomesdia, product, created_at;

-- Execution event timeline for detailed tracing
CREATE OR REPLACE VIEW vw_execution_timeline AS
SELECT
    anomesdia,
    product,
    step_name,
    event_type,
    event_source,
    event_message,
    event_at
FROM analytics_execution_events
ORDER BY anomesdia, product, event_at;

-- Business curated data combined with ingestion run context
CREATE OR REPLACE VIEW vw_curated_with_run_context AS
SELECT
    c.transaction_id,
    c.customer_id,
    c.amount,
    c.transaction_date,
    c.domain,
    c.product,
    c.business_date,
    c.enriched_at,
    r.status AS run_status,
    r.execution_id,
    r.ingestion_id
FROM poc_data_ingestion_business.business_curated c
LEFT JOIN analytics_ingestion_runs r
    ON c.product = r.product
    AND replace(c.business_date, '-', '') = r.anomesdia
    AND r.status = 'SUCCEEDED';

-- Rejected records with context for operational analysis
CREATE OR REPLACE VIEW vw_rejected_with_context AS
SELECT
    rj.run_id,
    rj.stage,
    rj.product,
    rj.business_date,
    rj.file_name,
    rj.row_number,
    rj.reason,
    rj.raw_row,
    r.status AS run_status,
    r.ingestion_id
FROM poc_data_ingestion_business.business_rejected rj
LEFT JOIN analytics_ingestion_runs r
    ON rj.product = r.product
    AND replace(rj.business_date, '-', '') = r.anomesdia;

-- Error detail with source file for IT diagnostics (non-aggregated)
CREATE OR REPLACE VIEW vw_error_detail_with_source AS
SELECT
    e.anomesdia,
    e.product,
    e.step_name,
    e.error_type,
    e.error_code,
    e.error_message,
    e.error_category,
    e.occurred_at,
    e.source_bucket,
    e.source_key,
    e.payload_ref,
    r.source_file_name,
    r.ingestion_id,
    r.execution_id,
    r.status AS run_status
FROM analytics_ingestion_errors e
LEFT JOIN analytics_ingestion_runs r
    ON e.ingestion_id = r.ingestion_id;

-- Ingested records with full source traceability for business + TI
CREATE OR REPLACE VIEW vw_ingested_with_source AS
SELECT
    c.transaction_id,
    c.customer_id,
    c.amount,
    c.transaction_date,
    c.domain,
    c.product,
    c.business_date,
    r.anomesdia,
    r.source_bucket,
    r.source_key,
    r.source_file_name,
    r.ingestion_id,
    r.execution_id,
    r.status AS run_status,
    r.raw_path,
    r.processed_path,
    r.curated_path
FROM poc_data_ingestion_business.business_curated c
LEFT JOIN analytics_ingestion_runs r
    ON c.product = r.product
    AND replace(c.business_date, '-', '') = r.anomesdia
    AND r.status = 'SUCCEEDED';

-- Unified troubleshooting: runs with errors, rejections, and file lineage in one view
CREATE OR REPLACE VIEW vw_troubleshooting_dashboard AS
SELECT
    r.anomesdia,
    r.product,
    r.domain,
    r.status AS run_status,
    r.source_bucket,
    r.source_key,
    r.source_file_name,
    r.ingestion_id,
    r.execution_id,
    r.total_records,
    r.processed_records,
    r.rejected_records,
    r.error_records,
    r.failure_step,
    r.error_message AS run_error,
    r.started_at,
    r.finished_at,
    r.raw_path,
    r.processed_path,
    r.curated_path,
    r.rejected_path,
    CASE WHEN r.rejected_records > 0 THEN 1 ELSE 0 END AS has_rejections
FROM analytics_ingestion_runs r;

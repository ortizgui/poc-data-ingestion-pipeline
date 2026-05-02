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
    COUNT(*) AS occurrence_count
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
    AND c.business_date = r.anomesdia
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
    AND rj.business_date = r.anomesdia;

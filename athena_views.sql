-- Athena views for operational dashboards
-- Run against poc_data_ingestion_analytics database
-- Uses analytics_ingestion_runs + analytics_ingestion_steps (enriched with all step-level facts)

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

-- Unified troubleshooting: every run with source file, counts, paths, error info
CREATE OR REPLACE VIEW vw_troubleshooting_dashboard AS
SELECT
    anomesdia,
    product,
    domain,
    status AS run_status,
    source_bucket,
    source_key,
    source_file_name,
    ingestion_id,
    execution_id,
    total_records,
    processed_records,
    rejected_records,
    error_records,
    failure_step,
    error_message AS run_error,
    started_at,
    finished_at,
    raw_path,
    processed_path,
    curated_path,
    rejected_path,
    CASE WHEN rejected_records > 0 THEN 1 ELSE 0 END AS has_rejections
FROM analytics_ingestion_runs;

-- Step summary with record flow (core step fields)
CREATE OR REPLACE VIEW vw_ingestion_steps_summary AS
SELECT
    anomesdia, product, step_order, step_name, status,
    COUNT(*) AS attempts,
    SUM(input_records) AS input_records,
    SUM(output_records) AS output_records,
    SUM(rejected_records) AS rejected_records,
    SUM(error_records) AS error_records,
    AVG(duration_seconds) AS avg_duration_seconds
FROM analytics_ingestion_steps
GROUP BY anomesdia, product, step_order, step_name, status;

-- Errors extracted from step rows (where error_message is populated)
CREATE OR REPLACE VIEW vw_errors_with_source AS
SELECT
    s.anomesdia,
    s.product,
    s.step_name,
    s.error_type,
    s.error_code,
    s.error_message,
    s.error_category,
    s.occurred_at,
    s.source_bucket,
    s.source_key,
    r.source_file_name,
    r.ingestion_id,
    r.execution_id,
    r.status AS run_status
FROM analytics_ingestion_steps s
LEFT JOIN analytics_ingestion_runs r ON s.ingestion_id = r.ingestion_id
WHERE s.error_message IS NOT NULL AND s.error_message != '';

-- Quality results extracted from step rows
CREATE OR REPLACE VIEW vw_quality_results AS
SELECT
    anomesdia, product, step_name, rule_name, rule_type, rule_result,
    SUM(valid_records) AS valid_records,
    SUM(invalid_records) AS invalid_records,
    SUM(warning_records) AS warning_records,
    MIN(threshold_value) AS threshold_value,
    MIN(measured_value) AS measured_value
FROM analytics_ingestion_steps
WHERE rule_name IS NOT NULL AND rule_name != ''
GROUP BY anomesdia, product, step_name, rule_name, rule_type, rule_result;

-- Schema validation results extracted from step rows
CREATE OR REPLACE VIEW vw_schema_validation_results AS
SELECT
    anomesdia, product, step_name, schema_name, schema_version,
    validation_result, missing_columns, unexpected_columns,
    validation_message, validated_at
FROM analytics_ingestion_steps
WHERE schema_name IS NOT NULL AND schema_name != '';

-- Rejection summary extracted from step rows
CREATE OR REPLACE VIEW vw_rejections_summary AS
SELECT
    anomesdia, product, step_name, rejection_reason, rejection_category,
    SUM(rejected_count_summary) AS total_rejected,
    AVG(rejection_percent) AS avg_rejection_percent,
    MIN(rejected_detail_path) AS rejected_detail_path,
    MIN(sample_message) AS sample_message
FROM analytics_ingestion_steps
WHERE rejection_reason IS NOT NULL AND rejection_reason != ''
GROUP BY anomesdia, product, step_name, rejection_reason, rejection_category;

-- File lineage extracted from step rows
CREATE OR REPLACE VIEW vw_file_lineage AS
SELECT
    anomesdia, product, step_name, artifact_type, artifact_role,
    lineage_bucket, lineage_key, lineage_format,
    record_count_lineage, file_size_bytes,
    parent_bucket, parent_key, lineage_created_at
FROM analytics_ingestion_steps
WHERE artifact_type IS NOT NULL AND artifact_type != '';

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
    r.ingestion_id,
    r.source_bucket,
    r.source_key,
    r.source_file_name
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
    r.ingestion_id,
    r.source_file_name
FROM poc_data_ingestion_business.business_rejected rj
LEFT JOIN analytics_ingestion_runs r
    ON rj.product = r.product
    AND replace(rj.business_date, '-', '') = r.anomesdia;

-- Curated records with full source traceability
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

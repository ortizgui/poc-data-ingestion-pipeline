from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from botocore.exceptions import ClientError

from aws_local import (
    CONFIG_TABLE,
    CURATED_QUEUE,
    DATA_BUCKET,
    PipelineError,
    RejectedRowsThresholdError,
    clients,
    ensure_queue,
    load_json,
    partition_prefix,
    s3_write_text,
    validate_product_config,
    write_jsonl,
)
from enrichment_batch import run_enrichment
from evidence import Evidence, evidence_table, write_local_report
from glue_harmonization import run_harmonization
from glue_landing import run_landing
from pipeline import ASL_PATH, run_id_for


def get_product_config(product: str, aws: Any) -> dict[str, Any] | None:
    return aws.dynamodb.Table(CONFIG_TABLE).get_item(Key={"product": product}).get("Item")


def validate_event(event: dict[str, Any], aws: Any) -> dict[str, Any]:
    missing = [field for field in ["product", "file_name", "business_date"] if not event.get(field)]
    if missing:
        raise PipelineError(f"event missing fields: {', '.join(missing)}")

    product_config = get_product_config(event["product"], aws)
    if not product_config:
        raise PipelineError(f"product not configured in DynamoDB: {event['product']}")
    validate_product_config(event["product"], product_config)
    return {**event, "product_config": product_config}


def start_step_function_execution(event: dict[str, Any], aws: Any) -> str | None:
    machines = aws.sfn.list_state_machines().get("stateMachines", [])
    machine = next((item for item in machines if item["name"] == "local-ingestion-state-machine"), None)
    if not machine:
        return None
    response = aws.sfn.start_execution(stateMachineArn=machine["stateMachineArn"], input=json.dumps(event))
    return response.get("executionArn")


def send_file_event_to_worker_queue(
    aws: Any,
    event_type: str,
    key: str,
    valid_event: dict[str, Any],
    run_id: str,
    rows: dict[str, int] | None = None,
    error: str | None = None,
) -> None:
    queue_url = ensure_queue(aws.sqs, CURATED_QUEUE)
    message = {
        "event_type": event_type,
        "bucket": DATA_BUCKET,
        "key": key,
        "run_id": run_id,
        "product": valid_event["product"],
        "business_date": valid_event["business_date"],
        "file_name": valid_event["file_name"],
    }
    if rows:
        message["rows"] = rows
    if error:
        message["error"] = error
    aws.sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(message, sort_keys=True))


def write_file_failure(
    aws: Any,
    valid_event: dict[str, Any],
    run_id: str,
    step_name: str,
    error: str,
) -> str:
    file_stem = Path(valid_event["file_name"]).stem
    key = (
        f"rejected/file-failed/{valid_event['product']}/{partition_prefix(valid_event['business_date'])}/"
        f"{run_id}/{file_stem}.jsonl"
    )
    s3_write_text(
        aws.s3,
        DATA_BUCKET,
        key,
        write_jsonl(
            [
                {
                    "run_id": run_id,
                    "stage": step_name,
                    "product": valid_event["product"],
                    "business_date": valid_event["business_date"],
                    "file_name": valid_event["file_name"],
                    "reason": error,
                }
            ]
        ),
    )
    return key


def execute_resource(resource: str, state: dict[str, Any], aws: Any, evidence: Evidence) -> dict[str, Any]:
    if resource == "local:validate_event":
        valid_event = validate_event(state["event"], aws)
        evidence.ok("DynamoDBConfig", "product config found", f"dynamodb://{CONFIG_TABLE}/{valid_event['product']}")
        return {**state, "valid_event": valid_event}

    if resource == "local:glue_landing":
        raw_key, raw_rows = run_landing(state["valid_event"], aws, state["run_id"])
        evidence.ok("LandingGlue", f"copied {raw_rows} rows to raw", f"s3://{DATA_BUCKET}/{raw_key}")
        return {**state, "raw_key": raw_key}

    if resource == "local:glue_harmonization":
        processed_key, processed_rows, mapping_key, rejection_key, rejected_rows = run_harmonization(
            state["raw_key"], state["valid_event"], aws, state["run_id"]
        )
        evidence.ok(
            "HarmonizationGlue",
            f"loaded s3://{DATA_BUCKET}/{mapping_key}; mapped {processed_rows} rows to transaction domain; rejected {rejected_rows} rows",
            f"s3://{DATA_BUCKET}/{processed_key}",
        )
        if rejection_key:
            evidence.ok(
                "RejectedRecords",
                f"wrote {rejected_rows} rejected rows; S3 notification should notify worker queue",
                f"s3://{DATA_BUCKET}/{rejection_key}",
            )
        rejection_keys = [*state.get("rejection_keys", [])]
        if rejection_key:
            rejection_keys.append(rejection_key)
        return {
            **state,
            "processed_key": processed_key,
            "rejection_key": rejection_key,
            "rejection_keys": rejection_keys,
            "rejected_rows": state.get("rejected_rows", 0) + rejected_rows,
        }

    if resource == "local:enrichment_batch":
        curated_key, curated_rows, rejection_key, rejected_rows = run_enrichment(
            state["processed_key"], state["valid_event"], aws
        )
        evidence.ok("EnrichmentBatch", f"enriched {curated_rows} rows; rejected {rejected_rows} rows", f"s3://{DATA_BUCKET}/{curated_key}")
        rejection_keys = [*state.get("rejection_keys", [])]
        if rejection_key:
            rejection_keys.append(rejection_key)
            evidence.ok(
                "RejectedRecords",
                f"wrote {rejected_rows} enrichment rejected rows; S3 notification should notify worker queue",
                f"s3://{DATA_BUCKET}/{rejection_key}",
            )
        return {
            **state,
            "curated_key": curated_key,
            "rejection_key": rejection_key or state.get("rejection_key"),
            "rejection_keys": rejection_keys,
            "rejected_rows": state.get("rejected_rows", 0) + rejected_rows,
        }

    if resource == "local:s3_notification":
        evidence.ok("S3Notification", "curated object should notify SQS", f"s3://{DATA_BUCKET}/curated/ -> sqs://{CURATED_QUEUE}")
        return state

    raise PipelineError(f"unsupported ASL resource: {resource}")


def run_state_machine(event: dict[str, Any], asl_path: Path = ASL_PATH, aws: Any | None = None) -> dict[str, Any]:
    aws = aws or clients()
    run_id = event.get("run_id") or run_id_for(event)
    event = {**event, "run_id": run_id}
    evidence = Evidence(run_id, event.get("product", "unknown"), event.get("file_name", "unknown"), event.get("business_date", "unknown"))
    state = {"event": event, "run_id": run_id}
    current_state_name = "StepFunctions"

    try:
        execution_arn = start_step_function_execution(event, aws)
        evidence.ok("StepFunctions", "ASL execution started by local runner", execution_arn)

        definition = load_json(asl_path)
        current_state_name = definition["StartAt"]
        states = definition["States"]

        while True:
            state_def = states[current_state_name]
            state = execute_resource(state_def["Resource"], state, aws, evidence)
            if state_def.get("End"):
                break
            current_state_name = state_def["Next"]

        valid_event = state["valid_event"]
        manifest = {
            "run_id": run_id,
            "product": valid_event["product"],
            "business_date": valid_event["business_date"],
            "raw": f"s3://{DATA_BUCKET}/{state['raw_key']}",
            "processed": f"s3://{DATA_BUCKET}/{state['processed_key']}",
            "curated": f"s3://{DATA_BUCKET}/{state['curated_key']}",
            "rejected": [f"s3://{DATA_BUCKET}/{key}" for key in state.get("rejection_keys", [])],
            "rejected_rows": state.get("rejected_rows", 0),
            "curated_notification": f"s3://{DATA_BUCKET}/curated/ -> sqs://{CURATED_QUEUE}",
            "step_functions_execution": execution_arn,
        }
        manifest_key = f"manifests/{run_id}.json"
        s3_write_text(aws.s3, DATA_BUCKET, manifest_key, json.dumps(manifest, indent=2, sort_keys=True) + "\n")
        evidence.ok("Manifest", "manifest persisted", f"s3://{DATA_BUCKET}/{manifest_key}")

        report = evidence.as_dict()
        report["manifest"] = manifest
        report["asl_path"] = str(asl_path)
        report_path = write_local_report(report)
        report["report_path"] = str(report_path)
        return {**manifest, "evidence": report}
    except Exception as exc:
        if state.get("valid_event"):
            if isinstance(exc, RejectedRowsThresholdError):
                failure_key = exc.rejection_key
                rows = {"rejected": exc.rejected_rows, "total": exc.total_rows}
            else:
                failure_key = write_file_failure(aws, state["valid_event"], run_id, current_state_name, str(exc))
                rows = None
            send_file_event_to_worker_queue(
                aws,
                "ingestion.file-failed",
                failure_key,
                state["valid_event"],
                run_id,
                rows,
                str(exc),
            )
        if not any(step.status == "FAIL" for step in evidence.steps):
            evidence.fail(current_state_name, str(exc))
        report = evidence.as_dict()
        report["asl_path"] = str(asl_path)
        report_path = write_local_report(report)
        report["report_path"] = str(report_path)
        raise PipelineError(json.dumps(report, sort_keys=True)) from exc


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run local Step Functions ASL against MiniStack")
    parser.add_argument("event", type=Path)
    parser.add_argument("--asl", type=Path, default=ASL_PATH)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    try:
        result = run_state_machine(load_json(args.event), args.asl)
    except (PipelineError, ClientError) as exc:
        print(f"local sfn failed: {exc}", file=sys.stderr)
        return 1

    print(evidence_table(result["evidence"]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

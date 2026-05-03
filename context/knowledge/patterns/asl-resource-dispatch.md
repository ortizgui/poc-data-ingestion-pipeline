# Pattern: ASL Resource Dispatch

## Description

Load the state machine definition from ASL JSON and dispatch each state resource to a local Python implementation.

## When to Use

Use this pattern when local execution should mirror Step Functions sequencing while still running local scripts for each job.

## Pattern

Read `StartAt` and `States`, execute the current state's `Resource`, then follow `Next` until a state declares `End`.

## Example

```python
definition = load_json(asl_path)
current_state_name = definition["StartAt"]
states = definition["States"]

while True:
    state_def = states[current_state_name]
    state = execute_resource(state_def["Resource"], state, aws, evidence)
    if state_def.get("End"):
        break
    current_state_name = state_def["Next"]
```

```python
if resource == "local:glue_harmonization":
    processed_key, processed_rows, mapping_key, rejection_key, rejected_rows = run_harmonization(
        state["raw_key"], state["valid_event"], aws, state["run_id"]
    )
```

## Files Using This Pattern

- `state-machine.asl.json` - declares the local state machine flow.
- `local_sfn_runner.py` - executes ASL states and maps local resources to functions.

## Related

- [Decision: Event Orchestration](../../decisions/003-event-orchestration.md)
- [Feature: Event-Driven Ingestion](../../intent/feature-event-driven-ingestion.md)

## Status

- **Created**: 2026-05-02
- **Status**: Active

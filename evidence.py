from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from aws_local import DEFAULT_REPORTS, utc_now


@dataclass
class StepEvidence:
    name: str
    status: str
    detail: str
    resource: str | None = None
    error: str | None = None
    timestamp: str = field(default_factory=utc_now)


@dataclass
class Evidence:
    run_id: str
    product: str
    file_name: str
    business_date: str
    steps: list[StepEvidence] = field(default_factory=list)

    def ok(self, name: str, detail: str, resource: str | None = None) -> None:
        self.steps.append(StepEvidence(name=name, status="OK", detail=detail, resource=resource))

    def fail(self, name: str, error: str, resource: str | None = None) -> None:
        self.steps.append(StepEvidence(name=name, status="FAIL", detail="failed", resource=resource, error=error))

    def as_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "product": self.product,
            "file_name": self.file_name,
            "business_date": self.business_date,
            "status": "FAIL" if any(step.status == "FAIL" for step in self.steps) else "OK",
            "steps": [step.__dict__ for step in self.steps],
        }


def write_local_report(report: dict[str, Any], reports_dir: Path = DEFAULT_REPORTS) -> Path:
    reports_dir.mkdir(parents=True, exist_ok=True)
    path = reports_dir / f"{report['run_id']}.json"
    path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def evidence_table(report: dict[str, Any]) -> str:
    lines = [
        f"Run: {report['run_id']} | product={report['product']} | file={report['file_name']} | status={report['status']}",
        "",
        "STATUS  STEP                       DETAIL",
        "------  -------------------------  ------------------------------------------------------------",
    ]
    for step in report["steps"]:
        lines.append(f"{step['status']:<6}  {step['name']:<25}  {step.get('detail') or '-'}")
        if step.get("resource"):
            lines.append(f"        resource: {step['resource']}")
        if step.get("error"):
            lines.append(f"        error: {step['error']}")
    if "report_path" in report:
        lines.extend(["", f"Evidence: {report['report_path']}"])
    return "\n".join(lines)

"""Aggregate workflow executions into per-type summaries (kept free of web deps so it is
easy to test against a live Temporal server)."""
from typing import Any, AsyncIterable, Optional

FAILED_STATUSES = {"FAILED", "TIMED_OUT"}


async def summarize_types(executions: AsyncIterable[Any]) -> list[dict[str, Any]]:
    """`executions`: Temporal WorkflowExecution objects, newest first.

    Returns one row per workflow type with instance counts by outcome and the start time
    of the most recent instance.
    """
    types: dict[str, dict[str, Any]] = {}
    async for wf in executions:
        row = types.setdefault(
            wf.workflow_type,
            {
                "type": wf.workflow_type,
                "total": 0,
                "running": 0,
                "completed": 0,
                "failed": 0,
                "last_started": None,
            },
        )
        status: Optional[str] = wf.status.name if wf.status else None
        row["total"] += 1
        if status == "RUNNING":
            row["running"] += 1
        elif status == "COMPLETED":
            row["completed"] += 1
        elif status in FAILED_STATUSES:
            row["failed"] += 1
        started = wf.start_time.isoformat() if wf.start_time else None
        if started and (row["last_started"] is None or started > row["last_started"]):
            row["last_started"] = started
    return sorted(types.values(), key=lambda r: r["type"])

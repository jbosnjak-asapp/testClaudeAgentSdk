"""Turn a workflow's raw event history into Temporal-UI-style "compact" rows.

Related events are grouped into one row: scheduled/started/completed of an activity,
started/fired of a timer. Workflow-task bookkeeping events are hidden. Web-dependency free,
so it can be tested against a Temporal server directly.

Row shape (stable; only add fields):
  {"ids": [event ids], "time": ISO-8601 UTC of the latest event in the row,
   "kind": "Activity" | "Timer" | "Signal received" | "Workflow started" | ...,
   "status": e.g. "Completed" | "Failed" | "Running" | "Fired" | None,
   "details": [{"label": ..., "value": ...}, ...]}
"""
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Optional

from temporalio.api.enums.v1 import EventType

MAX_VALUE_CHARS = 1000
Row = dict[str, Any]


def event_time_iso(ts) -> Optional[str]:
    """Protobuf Timestamp -> timezone-aware ISO string (browsers treat naive ones as local)."""
    if ts is None or (ts.seconds == 0 and ts.nanos == 0):
        return None
    return datetime.fromtimestamp(ts.seconds + ts.nanos / 1e9, tz=timezone.utc).isoformat()


def _truncate(text: str) -> str:
    return text if len(text) <= MAX_VALUE_CHARS else text[:MAX_VALUE_CHARS] + "…"


def payload_text(payloads) -> Optional[str]:
    """Human-readable form of a Payloads message (JSON payloads are already text)."""
    if payloads is None or not payloads.payloads:
        return None
    parts = []
    for p in payloads.payloads:
        encoding = p.metadata.get("encoding", b"").decode("utf-8", "replace")
        if encoding == "binary/null":
            parts.append("null")
            continue
        try:
            parts.append(p.data.decode("utf-8"))
        except UnicodeDecodeError:
            parts.append("<binary>")
    return _truncate(", ".join(parts))


def failure_text(failure) -> Optional[str]:
    if failure is None or not failure.message:
        return None
    messages, cause = [failure.message], failure.cause
    while cause is not None and cause.message and len(messages) < 3:
        messages.append(cause.message)
        cause = cause.cause if cause.HasField("cause") else None
    return _truncate(" ← ".join(messages))


def duration_text(duration) -> str:
    seconds = int(duration.ToTimedelta().total_seconds()) if duration else 0
    for unit, size in (("day", 86400), ("hour", 3600), ("minute", 60)):
        if seconds >= size and seconds % size == 0:
            n = seconds // size
            return f"{n} {unit}{'' if n == 1 else 's'}"
    return f"{seconds}s"


def _name(event) -> str:
    return EventType.Name(event.event_type).removeprefix("EVENT_TYPE_")


def _attrs(event):
    which = event.WhichOneof("attributes")
    return getattr(event, which) if which else None


def _detail(row: Row, label: str, value: Optional[str]) -> None:
    """Set (or replace) a detail chip; empty values are skipped."""
    row["details"] = [d for d in row["details"] if d["label"] != label]
    if value:
        row["details"].append({"label": label, "value": value})


def _new_row(kind: str, event, status: Optional[str] = None) -> Row:
    return {
        "ids": [event.event_id],
        "time": event_time_iso(event.event_time),
        "kind": kind,
        "status": status,
        "details": [],
    }


def _touch(row: Row, event, status: Optional[str] = None) -> None:
    row["ids"].append(event.event_id)
    row["time"] = event_time_iso(event.event_time)
    if status:
        row["status"] = status


def compact_events(events: Iterable[Any]) -> list[Row]:
    """History events (oldest first) -> compact rows, ordered by their first event."""
    rows: list[Row] = []
    groups: dict[int, Row] = {}  # activity: scheduled event id; timer: started event id

    for event in events:
        name = _name(event)
        a = _attrs(event)
        if name.startswith("WORKFLOW_TASK_"):
            continue

        if name == "ACTIVITY_TASK_SCHEDULED":
            row = _new_row("Activity", event, "Scheduled")
            _detail(row, "Activity Type", a.activity_type.name)
            _detail(row, "Input", payload_text(a.input))
            groups[event.event_id] = row
            rows.append(row)
        elif name.startswith("ACTIVITY_TASK_") and a is not None and a.scheduled_event_id in groups:
            row = groups[a.scheduled_event_id]
            if name == "ACTIVITY_TASK_STARTED":
                _touch(row, event, "Running")
                _detail(row, "Attempt", str(a.attempt))
            elif name == "ACTIVITY_TASK_COMPLETED":
                _touch(row, event, "Completed")
                _detail(row, "Result", payload_text(a.result))
            elif name == "ACTIVITY_TASK_FAILED":
                _touch(row, event, "Failed")
                _detail(row, "Failure", failure_text(a.failure))
            elif name == "ACTIVITY_TASK_TIMED_OUT":
                _touch(row, event, "Timed out")
                _detail(row, "Failure", failure_text(a.failure))
            elif name == "ACTIVITY_TASK_CANCELED":
                _touch(row, event, "Canceled")
            else:  # e.g. cancel requested
                _touch(row, event)
        elif name == "TIMER_STARTED":
            row = _new_row("Timer", event, "Waiting")
            _detail(row, "Start To Fire Timeout", duration_text(a.start_to_fire_timeout))
            groups[event.event_id] = row
            rows.append(row)
        elif name in ("TIMER_FIRED", "TIMER_CANCELED") and a.started_event_id in groups:
            _touch(groups[a.started_event_id], event, "Fired" if name == "TIMER_FIRED" else "Canceled")
        elif name == "WORKFLOW_EXECUTION_SIGNALED":
            row = _new_row("Signal received", event)
            _detail(row, "Signal Name", a.signal_name)
            _detail(row, "Input", payload_text(a.input))
            _detail(row, "Identity", a.identity)
            rows.append(row)
        elif name == "WORKFLOW_EXECUTION_STARTED":
            row = _new_row("Workflow started", event)
            _detail(row, "Workflow Type", a.workflow_type.name)
            _detail(row, "Task Queue", a.task_queue.name)
            _detail(row, "Input", payload_text(a.input))
            rows.append(row)
        elif name == "WORKFLOW_EXECUTION_COMPLETED":
            row = _new_row("Workflow completed", event, "Completed")
            _detail(row, "Result", payload_text(a.result))
            rows.append(row)
        elif name == "WORKFLOW_EXECUTION_FAILED":
            row = _new_row("Workflow failed", event, "Failed")
            _detail(row, "Failure", failure_text(a.failure))
            rows.append(row)
        elif name in ("WORKFLOW_EXECUTION_TERMINATED", "WORKFLOW_EXECUTION_TIMED_OUT",
                      "WORKFLOW_EXECUTION_CANCELED"):
            kind = "Workflow " + name.removeprefix("WORKFLOW_EXECUTION_").replace("_", " ").lower()
            row = _new_row(kind, event, "Failed")
            _detail(row, "Reason", getattr(a, "reason", None))
            rows.append(row)
        else:  # anything else: a plain row so nothing silently disappears
            rows.append(_new_row(name.replace("_", " ").capitalize(), event))
    return rows


def pending_activity_rows(pending: Iterable[Any]) -> list[Row]:
    """Activities that are scheduled but haven't completed (they have no event of their own
    until they finish, so they come from DescribeWorkflowExecution)."""
    rows = []
    for p in pending:
        maximum = f"/{p.maximum_attempts}" if p.maximum_attempts else ""
        row: Row = {
            "ids": [],
            "time": event_time_iso(p.scheduled_time),
            "kind": "Pending activity",
            "status": f"Attempt {p.attempt}{maximum}",
            "details": [],
        }
        _detail(row, "Activity Type", p.activity_type.name)
        _detail(row, "Last Failure", failure_text(p.last_failure) if p.HasField("last_failure") else None)
        rows.append(row)
    return rows

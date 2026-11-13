"""HTTP API over Temporal: a read-only part for a generic workflow UI, plus the curl-able
address verification endpoints (address_api.py, protected by X-API-Key).

  uv run uvicorn api:app --port 8000     (binds 127.0.0.1 by default)

GET /health
GET /workflow-types                       workflow types with instance counts
GET /workflows?type=&status=&limit=       list recent workflows
GET /workflows/{id}/events                compact event history (like Temporal UI's Compact view)
GET /workflows/{id}                       lifecycle status + `ui_state` (the UI contract
                                          in workflow_ui.py) or, if the workflow doesn't
                                          implement it, an event `timeline` fallback

/address-verifications/...                start, upload, review and check an address verification
                                          (see address_api.py)

The generic endpoints above have no auth and only read: keep the API on localhost / behind an
authenticated proxy.
"""
import os
import re
from contextlib import asynccontextmanager
from datetime import timedelta
from typing import Any, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from temporalio.api.enums.v1 import EventType
from temporalio.client import Client
from temporalio.service import RPCError, RPCStatusCode

from address_api import router as address_router
from workflow_events import compact_events, event_time_iso, pending_activity_rows
from workflow_types import summarize_types
from workflow_ui import UI_STATE_QUERY

QUERY_TIMEOUT = timedelta(seconds=5)  # a query needs a live worker; don't hang without one
MAX_TIMELINE_EVENTS = 500
MAX_TYPE_SCAN = 1000  # executions scanned to discover workflow types
MAX_HISTORY_EVENTS = 2000  # events returned by /events
# Task-level noise that makes a timeline unreadable.
_NOISE = ("WORKFLOW_TASK_", "WORKFLOW_EXECUTION_SIGNALED_PLACEHOLDER")
_STATUSES = {
    "Running", "Completed", "Failed", "Canceled", "Terminated", "ContinuedAsNew", "TimedOut",
}
_TYPE_RE = re.compile(r"^[A-Za-z0-9_.\-]+$")

_state: dict[str, Client] = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    _state["client"] = app.state.client = await Client.connect(
        os.getenv("TEMPORAL_ADDRESS", "localhost:7233"),
        namespace=os.getenv("TEMPORAL_NAMESPACE", "default"),
    )
    yield


app = FastAPI(title="Workflow API", lifespan=lifespan)

# The UI runs in the browser on another origin (e.g. the AI Console dev server on
# localhost:3005), so allow cross-origin GETs. Default: any localhost port only.
# Override with WORKFLOW_API_CORS_ORIGIN_REGEX for other hosts. Read-only, no cookies.
app.add_middleware(
    CORSMiddleware,
    allow_origin_regex=os.getenv(
        "WORKFLOW_API_CORS_ORIGIN_REGEX", r"^https?://(localhost|127\.0\.0\.1)(:\d+)?$"
    ),
    allow_methods=["GET"],
    allow_headers=["*"],
)


app.include_router(address_router)


def _iso(dt) -> Optional[str]:
    return dt.isoformat() if dt else None


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/workflow-types")
async def list_workflow_types() -> list[dict[str, Any]]:
    """Distinct workflow types seen in the latest executions, with instance counts."""
    return await summarize_types(_state["client"].list_workflows("", limit=MAX_TYPE_SCAN))


@app.get("/workflows")
async def list_workflows(
    type: Optional[str] = None,
    status: Optional[str] = None,
    limit: int = Query(50, ge=1, le=200),
) -> list[dict[str, Any]]:
    # These values are interpolated into a visibility query string: allow-list them.
    clauses = []
    if type:
        if not _TYPE_RE.match(type):
            raise HTTPException(400, "invalid type")
        clauses.append(f"WorkflowType = '{type}'")
    if status:
        if status not in _STATUSES:
            raise HTTPException(400, f"status must be one of {sorted(_STATUSES)}")
        clauses.append(f"ExecutionStatus = '{status}'")
    # No ORDER BY: the dev server rejects it, and Temporal already returns the
    # newest executions first.
    query = " AND ".join(clauses)

    out = []
    async for wf in _state["client"].list_workflows(query, limit=limit):
        out.append(
            {
                "id": wf.id,
                "run_id": wf.run_id,
                "type": wf.workflow_type,
                "status": wf.status.name if wf.status else None,
                "start_time": _iso(wf.start_time),
                "close_time": _iso(wf.close_time),
            }
        )
    return out


async def _timeline(handle) -> list[dict[str, Any]]:
    events = []
    async for ev in handle.fetch_history_events():
        name = EventType.Name(ev.event_type).removeprefix("EVENT_TYPE_")
        if name.startswith(_NOISE):
            continue
        detail = None
        if name.startswith("ACTIVITY_TASK_SCHEDULED"):
            detail = ev.activity_task_scheduled_event_attributes.activity_type.name
        elif name == "WORKFLOW_EXECUTION_SIGNALED":
            detail = ev.workflow_execution_signaled_event_attributes.signal_name
        events.append(
            {
                "id": ev.event_id,
                "time": event_time_iso(ev.event_time),
                "type": name,
                "detail": detail,
            }
        )
        if len(events) >= MAX_TIMELINE_EVENTS:
            break
    return events


@app.get("/workflows/{workflow_id}")
async def get_workflow(workflow_id: str) -> dict[str, Any]:
    handle = _state["client"].get_workflow_handle(workflow_id)
    try:
        desc = await handle.describe()
    except RPCError as e:
        if e.status == RPCStatusCode.NOT_FOUND:
            raise HTTPException(404, "workflow not found") from e
        raise

    ui_state = None
    ui_state_error = None
    try:
        ui_state = await handle.query(UI_STATE_QUERY, rpc_timeout=QUERY_TIMEOUT)
    except Exception as e:  # not implemented, worker down, timeout...
        ui_state_error = f"{type(e).__name__}: {e}"

    return {
        "id": desc.id,
        "run_id": desc.run_id,
        "type": desc.workflow_type,
        "status": desc.status.name if desc.status else None,
        "start_time": _iso(desc.start_time),
        "close_time": _iso(desc.close_time),
        "ui_state": ui_state,
        "ui_state_error": ui_state_error,
        # Fallback for workflows that don't implement the UI contract.
        "timeline": await _timeline(handle) if ui_state is None else None,
    }


@app.get("/workflows/{workflow_id}/events")
async def get_workflow_events(workflow_id: str) -> dict[str, Any]:
    """Event history grouped into compact rows (oldest first) plus pending activities."""
    handle = _state["client"].get_workflow_handle(workflow_id)
    try:
        desc = await handle.describe()
    except RPCError as e:
        if e.status == RPCStatusCode.NOT_FOUND:
            raise HTTPException(404, "workflow not found") from e
        raise

    history = []
    truncated = False
    async for ev in handle.fetch_history_events():
        if len(history) >= MAX_HISTORY_EVENTS:
            truncated = True
            break
        history.append(ev)
    return {
        "status": desc.status.name if desc.status else None,
        "events": compact_events(history),
        "pending": pending_activity_rows(desc.raw_description.pending_activities),
        "total_events": len(history),
        "truncated": truncated,
    }

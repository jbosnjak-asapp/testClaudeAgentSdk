"""REST endpoints for the address verification workflow: the curl-able version of address_cli.py.

  POST /address-verifications                       {"company_marker": "acme", "customer_id": 42}
  GET  /address-verifications/{id}/status
  POST /address-verifications/{id}/address          {"address": "..."}
  POST /address-verifications/{id}/document         multipart file field "file"
  POST /address-verifications/{id}/review           {"approved": true, "reviewer": "me", "notes": ""}

Reading the status is open like the rest of the API. The POSTs change workflows, so they need the
header `X-API-Key: $WORKFLOW_API_KEY`; if that variable isn't set on the API process they are
disabled. (A custom header also stops other web pages from firing requests at your localhost.)
"""
import hmac
import os
import re
import shutil
import uuid
from dataclasses import asdict
from datetime import timedelta
from pathlib import Path
from typing import Any, Optional

from fastapi import APIRouter, Depends, Header, HTTPException, Request, UploadFile
from pydantic import BaseModel, Field
from temporalio.client import Client, WorkflowHandle
from temporalio.exceptions import WorkflowAlreadyStartedError
from temporalio.service import RPCError, RPCStatusCode

from address_activities import MAX_DOCUMENT_BYTES, UPLOAD_ROOT
from address_shared import (
    ADDRESS_TASK_QUEUE,
    ADDRESS_WORKFLOW_NAME,
    AddressVerificationInput,
    ReviewDecision,
    address_workflow_id,
)
from address_workflow import AddressVerificationWorkflow

QUERY_TIMEOUT = timedelta(seconds=5)
# Only ids that follow the convention are accepted: this also keeps them safe as a directory name.
_ID_RE = re.compile(rf"^{re.escape(ADDRESS_WORKFLOW_NAME)}-[A-Za-z0-9_.\-]+-\d+$")
_FILENAME_RE = re.compile(r"[^A-Za-z0-9_.\-]")
_MAX_ADDRESS_CHARS = 500


def require_api_key(x_api_key: Optional[str] = Header(default=None)) -> None:
    expected = os.getenv("WORKFLOW_API_KEY")
    if not expected:
        raise HTTPException(503, "Write endpoints are disabled: set WORKFLOW_API_KEY on the API process")
    if not x_api_key or not hmac.compare_digest(x_api_key, expected):
        raise HTTPException(401, "Missing or wrong X-API-Key header")


router = APIRouter(prefix="/address-verifications", tags=["address verification"])


class StartRequest(BaseModel):
    company_marker: str
    customer_id: int = Field(ge=0)


class AddressRequest(BaseModel):
    address: str = Field(min_length=1, max_length=_MAX_ADDRESS_CHARS)


class ReviewRequest(BaseModel):
    approved: bool
    reviewer: str = Field(min_length=1, max_length=200)
    notes: str = Field(default="", max_length=2000)


def _client(request: Request) -> Client:
    return request.app.state.client


def _handle(request: Request, workflow_id: str) -> WorkflowHandle:
    if not _ID_RE.match(workflow_id):
        raise HTTPException(400, f"id must look like {ADDRESS_WORKFLOW_NAME}-<companyMarker>-<customerId>")
    return _client(request).get_workflow_handle_for(AddressVerificationWorkflow.run, workflow_id)


async def _signal(handle: WorkflowHandle, signal, arg: Any) -> None:
    """Send a signal; Temporal rejects it for unknown or already finished workflows."""
    try:
        await handle.signal(signal, arg)
    except RPCError as e:
        if e.status == RPCStatusCode.NOT_FOUND:
            raise HTTPException(404, "workflow not found or already finished") from e
        raise


@router.post("", status_code=201, dependencies=[Depends(require_api_key)])
async def start_verification(body: StartRequest, request: Request) -> dict[str, str]:
    try:
        workflow_id = address_workflow_id(body.company_marker, body.customer_id)
    except ValueError as e:
        raise HTTPException(422, str(e)) from e
    try:
        await _client(request).start_workflow(
            AddressVerificationWorkflow.run,
            AddressVerificationInput(company_marker=body.company_marker, customer_id=body.customer_id),
            id=workflow_id,
            task_queue=ADDRESS_TASK_QUEUE,
        )
    except WorkflowAlreadyStartedError as e:
        raise HTTPException(409, f"a verification is already running for this customer: {workflow_id}") from e
    return {"workflow_id": workflow_id}


@router.get("/{workflow_id}/status")
async def get_status(workflow_id: str, request: Request) -> dict[str, Any]:
    """Lifecycle status, the workflow's current step, last error, address and Claude's verdict.

    Once the workflow has finished, `result` holds the outcome (approved / rejected / expired)."""
    handle = _handle(request, workflow_id)
    try:
        desc = await handle.describe()
    except RPCError as e:
        if e.status == RPCStatusCode.NOT_FOUND:
            raise HTTPException(404, "workflow not found") from e
        raise

    out: dict[str, Any] = {
        "workflow_id": desc.id,
        "run_id": desc.run_id,
        "lifecycle": desc.status.name if desc.status else None,
        "start_time": desc.start_time.isoformat() if desc.start_time else None,
        "close_time": desc.close_time.isoformat() if desc.close_time else None,
        "status": None,
        "result": None,
    }
    try:  # queries need a live worker
        out["status"] = asdict(await handle.query(AddressVerificationWorkflow.get_status, rpc_timeout=QUERY_TIMEOUT))
    except Exception as e:
        out["status_error"] = f"{type(e).__name__}: {e}"
    if desc.status is not None and desc.status.name == "COMPLETED":
        try:
            out["result"] = asdict(await handle.result())
        except Exception as e:
            out["result_error"] = f"{type(e).__name__}: {e}"
    return out


@router.post("/{workflow_id}/address", status_code=202, dependencies=[Depends(require_api_key)])
async def submit_address(workflow_id: str, body: AddressRequest, request: Request) -> dict[str, str]:
    await _signal(_handle(request, workflow_id), AddressVerificationWorkflow.submit_address, body.address)
    return {"accepted": "address"}


@router.post("/{workflow_id}/document", status_code=202, dependencies=[Depends(require_api_key)])
async def upload_document(workflow_id: str, file: UploadFile, request: Request) -> dict[str, str]:
    handle = _handle(request, workflow_id)
    try:  # fail early, before storing a file nobody will read
        await handle.describe()
    except RPCError as e:
        if e.status == RPCStatusCode.NOT_FOUND:
            raise HTTPException(404, "workflow not found") from e
        raise

    name = _FILENAME_RE.sub("_", Path(file.filename or "document").name).lstrip(".") or "document"
    dest_dir = UPLOAD_ROOT / workflow_id
    dest_dir.mkdir(parents=True, exist_ok=True)
    # Unique name per upload so a retry never overwrites a file the agent may be reading.
    dest = dest_dir / f"{uuid.uuid4().hex[:8]}-{name}"
    size = 0
    try:
        with dest.open("wb") as out:
            while chunk := await file.read(1024 * 1024):
                size += len(chunk)
                if size > MAX_DOCUMENT_BYTES:
                    raise HTTPException(413, f"file is larger than {MAX_DOCUMENT_BYTES // (1024 * 1024)}MB")
                out.write(chunk)
        if size == 0:
            raise HTTPException(400, "file is empty")
        await _signal(handle, AddressVerificationWorkflow.upload_document, str(dest))
    except BaseException:
        dest.unlink(missing_ok=True)
        if dest_dir.exists() and not any(dest_dir.iterdir()):
            shutil.rmtree(dest_dir, ignore_errors=True)
        raise
    return {"accepted": "document", "stored_as": dest.name}


@router.post("/{workflow_id}/review", status_code=202, dependencies=[Depends(require_api_key)])
async def review(workflow_id: str, body: ReviewRequest, request: Request) -> dict[str, str]:
    """Manual override of the Jira decision (the normal path is closing the Jira ticket)."""
    decision = ReviewDecision(approved=body.approved, reviewer=body.reviewer, notes=body.notes)
    await _signal(_handle(request, workflow_id), AddressVerificationWorkflow.review, decision)
    return {"accepted": "review"}

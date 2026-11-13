"""Tests the /address-verifications REST endpoints against a local Temporal dev server with FAKE
activities (no Claude, Jira or network). Needs the `temporal` CLI on PATH.
Run: uv run python test_address_api.py"""
import asyncio
import os
import shutil

import httpx
from temporalio import activity
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

import address_api
from address_activities import UPLOAD_ROOT
from address_shared import ADDRESS_TASK_QUEUE, ApprovalRequest, ApprovalTicket, ReviewDecision
from address_workflow import AddressVerificationWorkflow
import api
from api import app
from test_address_workflow import TICKET, fake_geocode, fake_verify

KEY = {"X-API-Key": "test-key"}
BASE = "/address-verifications"
WF = "AddressVerificationWorkflow-acme-4242"


@activity.defn(name="create_approval_ticket")
async def fake_create(req: ApprovalRequest) -> ApprovalTicket:
    return TICKET


@activity.defn(name="wait_for_ticket_decision")
async def fake_wait(ticket: ApprovalTicket) -> ReviewDecision:
    await asyncio.sleep(3600)  # the "reviewer" never acts in Jira; the test uses the review endpoint
    raise AssertionError("unreachable")


async def wait_for(http, predicate, what):
    for _ in range(100):
        body = (await http.get(f"{BASE}/{WF}/status")).json()
        if predicate(body):
            return body
        await asyncio.sleep(0.1)
    raise AssertionError(f"timed out waiting for {what}: {body}")


async def main() -> None:
    cli = shutil.which("temporal")
    assert cli, "temporal CLI not found"
    shutil.rmtree(UPLOAD_ROOT / WF, ignore_errors=True)
    async with await WorkflowEnvironment.start_local(dev_server_existing_path=cli) as env:
        app.state.client = api._state["client"] = env.client
        transport = httpx.ASGITransport(app=app)
        async with Worker(
            env.client,
            task_queue=ADDRESS_TASK_QUEUE,
            workflows=[AddressVerificationWorkflow],
            activities=[fake_geocode, fake_verify, fake_create, fake_wait],
        ), httpx.AsyncClient(transport=transport, base_url="http://test") as http:
            start = {"company_marker": "acme", "customer_id": 4242}

            # auth: disabled without a key on the server, rejected with a wrong one
            os.environ.pop("WORKFLOW_API_KEY", None)
            assert (await http.post(BASE, json=start, headers=KEY)).status_code == 503
            os.environ["WORKFLOW_API_KEY"] = "test-key"
            assert (await http.post(BASE, json=start)).status_code == 401
            assert (await http.post(BASE, json=start, headers={"X-API-Key": "nope"})).status_code == 401

            # validation and id handling
            assert (await http.post(BASE, json={"company_marker": "a/b", "customer_id": 1}, headers=KEY)).status_code == 422
            assert (await http.post(BASE, json={"company_marker": "acme", "customer_id": -1}, headers=KEY)).status_code == 422
            assert (await http.get(f"{BASE}/not-an-id/status")).status_code == 400
            assert (await http.get(f"{BASE}/AddressVerificationWorkflow-acme-1/status")).status_code == 404
            r = await http.post(f"{BASE}/AddressVerificationWorkflow-acme-1/address", json={"address": "x"}, headers=KEY)
            assert r.status_code == 404, r.text

            # start (+ duplicate refused)
            r = await http.post(BASE, json=start, headers=KEY)
            assert r.status_code == 201 and r.json() == {"workflow_id": WF}, r.text
            assert (await http.post(BASE, json=start, headers=KEY)).status_code == 409

            body = await wait_for(http, lambda b: b["status"] and b["status"]["step"] == "awaiting_address", "awaiting address")
            assert body["lifecycle"] == "RUNNING" and body["result"] is None, body

            # address
            assert (await http.post(f"{BASE}/{WF}/address", json={"address": ""}, headers=KEY)).status_code == 422
            r = await http.post(f"{BASE}/{WF}/address", json={"address": "Helguera 3635"}, headers=KEY)
            assert r.status_code == 202, r.text
            body = await wait_for(http, lambda b: b["status"] and b["status"]["address"], "address validated")
            assert body["status"]["geocode"]["found"] is True

            # document: too big, empty, then ok (with a hostile filename)
            old = address_api.MAX_DOCUMENT_BYTES
            address_api.MAX_DOCUMENT_BYTES = 10
            r = await http.post(f"{BASE}/{WF}/document", files={"file": ("big.pdf", b"x" * 11)}, headers=KEY)
            assert r.status_code == 413, r.text
            address_api.MAX_DOCUMENT_BYTES = old
            r = await http.post(f"{BASE}/{WF}/document", files={"file": ("empty.pdf", b"")}, headers=KEY)
            assert r.status_code == 400, r.text
            assert not (UPLOAD_ROOT / WF).exists(), "rejected uploads must not leave files behind"
            r = await http.post(f"{BASE}/{WF}/document", files={"file": ("../../evil bill.pdf", b"%PDF-1.4 fake")}, headers=KEY)
            assert r.status_code == 202, r.text
            stored = list((UPLOAD_ROOT / WF).iterdir())
            assert len(stored) == 1 and stored[0].name.endswith("-evil_bill.pdf"), stored
            assert stored[0].resolve().is_relative_to(UPLOAD_ROOT)

            body = await wait_for(
                http, lambda b: b["status"] and b["status"]["step"] == "awaiting_approval", "awaiting approval"
            )
            assert body["status"]["verdict"]["is_utility_bill"] is True
            assert body["status"]["document_path"] == str(stored[0])

            # review (manual override) -> finished with a result
            r = await http.post(
                f"{BASE}/{WF}/review", json={"approved": True, "reviewer": "Jane", "notes": "ok"}, headers=KEY
            )
            assert r.status_code == 202, r.text
            body = await wait_for(http, lambda b: b["lifecycle"] == "COMPLETED", "completion")
            assert body["result"]["status"] == "approved" and body["result"]["reviewer"] == "Jane", body
            assert (body["result"]["company_marker"], body["result"]["customer_id"]) == ("acme", 4242)

            # signals to a finished workflow are refused, not silently dropped
            r = await http.post(f"{BASE}/{WF}/address", json={"address": "again"}, headers=KEY)
            assert r.status_code == 404, r.text

            # the generic read-only API is unaffected and needs no key
            assert (await http.get(f"/workflows/{WF}")).status_code == 200
    shutil.rmtree(UPLOAD_ROOT / WF, ignore_errors=True)
    print("address api tests passed")


if __name__ == "__main__":
    asyncio.run(main())

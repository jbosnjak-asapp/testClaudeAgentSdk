"""Runs AddressVerificationWorkflow against a local Temporal dev server with FAKE
activities (no network, no Claude, no Jira). Needs the `temporal` CLI on PATH.
Run: uv run python test_address_workflow.py"""
import asyncio
import shutil
import uuid

from temporalio import activity
from temporalio.client import WorkflowFailureError
from temporalio.exceptions import WorkflowAlreadyStartedError
from temporalio.exceptions import ApplicationError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from address_shared import (
    ApprovalRequest,
    AddressVerificationInput,
    ApprovalTicket,
    DocumentCheckInput,
    DocumentVerdict,
    GeocodeResult,
    ReviewDecision,
    address_workflow_id,
)
from address_workflow import AddressVerificationWorkflow
from workflow_events import compact_events

INPUT = AddressVerificationInput(company_marker="acme", customer_id=42)
TICKET = ApprovalTicket(key="APP-7", url="https://example.atlassian.net/browse/APP-7")


@activity.defn(name="geocode_address")
async def fake_geocode(address: str) -> GeocodeResult:
    return GeocodeResult(found=True, lat="1", lon="2", display_name=f"Canonical {address}")


@activity.defn(name="verify_document_with_claude")
async def fake_verify(input: DocumentCheckInput) -> DocumentVerdict:
    return DocumentVerdict(
        match=True,
        confidence="high",
        matched_text="HELGUERA 3635",
        reason="ok",
        is_utility_bill=True,
        document_type="water bill",
    )


async def never_called(_t):
    raise AssertionError("poll must not start")


def test_ids() -> None:
    assert address_workflow_id("acme", 42) == "AddressVerificationWorkflow-acme-42"
    assert address_workflow_id("mtagnostic1", 0) == "AddressVerificationWorkflow-mtagnostic1-0"
    from address_shared import ADDRESS_WORKFLOW_NAME

    assert ADDRESS_WORKFLOW_NAME == AddressVerificationWorkflow.__name__
    for marker, cid in [("", 1), ("a b", 1), ("a/b", 1), ("../x", 1), ("-lead", 1), ("acme", -1), ("acme", "7"), ("acme", True), ("acme", 1.5)]:
        try:
            address_workflow_id(marker, cid)
            raise AssertionError(f"{marker!r}, {cid!r} should be rejected")
        except ValueError:
            pass


def make_jira(create_returns, wait_impl):
    calls = {"requests": []}

    @activity.defn(name="create_approval_ticket")
    async def create(req: ApprovalRequest):
        calls["requests"].append(req)
        return create_returns

    @activity.defn(name="wait_for_ticket_decision")
    async def wait(ticket: ApprovalTicket) -> ReviewDecision:
        return await wait_impl(ticket)

    return create, wait, calls


async def run(env, create_returns, wait_impl, *, signal=None, check_status=None):
    create, wait, calls = make_jira(create_returns, wait_impl)
    queue = f"test-{uuid.uuid4()}"
    async with Worker(
        env.client,
        task_queue=queue,
        workflows=[AddressVerificationWorkflow],
        activities=[fake_geocode, fake_verify, create, wait],
    ):
        handle = await env.client.start_workflow(
            AddressVerificationWorkflow.run,
            INPUT,
            id=f"wf-{uuid.uuid4()}",
            task_queue=queue,
        )
        await handle.signal(AddressVerificationWorkflow.submit_address, "Helguera 3635")
        await handle.signal(AddressVerificationWorkflow.upload_document, "/tmp/x.pdf")
        # wait until the workflow reaches the approval step
        for _ in range(100):
            state = await handle.query("ui_state")
            if state["current_step"] == "approval" and state["pending_input"]:
                break
            await asyncio.sleep(0.1)
        if check_status:
            check_status(await handle.query("ui_state"))
        if signal:
            await handle.signal(AddressVerificationWorkflow.review, signal)
        result = await asyncio.wait_for(handle.result(), 30)
        calls["events"] = compact_events([e async for e in handle.fetch_history_events()])
        return result, calls, await handle.query("ui_state")


async def main() -> None:
    test_ids()
    cli = shutil.which("temporal")
    assert cli, "temporal CLI not found"
    async with await WorkflowEnvironment.start_local(dev_server_existing_path=cli) as env:
        # A. Jira ticket closed as approved
        async def approved(_t):
            await asyncio.sleep(0.3)
            return ReviewDecision(True, "Jane Doe", "Jira APP-7 closed as 'Done'")

        def check_a(state):
            assert state["data"]["approval_ticket"]["key"] == "APP-7"
            assert "APP-7" in state["pending_input"]["label"]
            assert [x["state"] for x in state["steps"]] == ["done", "done", "done", "active"]
            assert [x["id"] for x in state["steps"]] == ["address", "document", "ticket", "approval"]

        result, calls, ui = await run(env, TICKET, approved, check_status=check_a)
        assert result.status == "approved" and result.reviewer == "Jane Doe", result
        req = calls["requests"][0]
        assert req.address == "Helguera 3635" and req.display_name == "Canonical Helguera 3635"
        assert req.matched_text == "HELGUERA 3635" and req.workflow_id.startswith("wf-")
        assert (req.company_marker, req.customer_id) == ("acme", 42)
        assert req.document_type == "water bill"
        assert (result.company_marker, result.customer_id) == ("acme", 42)
        assert ui["data"]["company_marker"] == "acme" and ui["data"]["customer_id"] == 42
        assert [s["state"] for s in ui["steps"]] == ["done"] * 4, ui
        rows = calls["events"]
        kinds = [r["kind"] for r in rows]
        assert kinds[0] == "Workflow started" and kinds[-1] == "Workflow completed", kinds
        assert rows[0]["ids"] == [1] and rows[0]["time"].endswith("+00:00"), rows[0]
        signals = [d["value"] for r in rows if r["kind"] == "Signal received" for d in r["details"] if d["label"] == "Signal Name"]
        assert signals == ["submit_address", "upload_document"], signals
        activities = {
            next(d["value"] for d in r["details"] if d["label"] == "Activity Type"): r
            for r in rows
            if r["kind"] == "Activity"
        }
        assert list(activities) == [
            "geocode_address",
            "verify_document_with_claude",
            "create_approval_ticket",
            "wait_for_ticket_decision",
        ], list(activities)
        for r in activities.values():
            assert r["status"] == "Completed" and len(r["ids"]) == 3, r  # scheduled, started, completed
        assert not any(k.startswith("Workflow task") for k in kinds)  # bookkeeping hidden
        assert any(r["kind"] == "Timer" for r in rows)
        print("A ok: jira approved; compact events grouped")

        # B. Jira ticket closed as rejected
        async def rejected(_t):
            return ReviewDecision(False, "Jane Doe", "Jira APP-7 closed as 'Won't Do'")

        result, _, ui = await run(env, TICKET, rejected)
        assert result.status == "rejected" and result.reviewer == "Jane Doe"
        assert ui["steps"][3]["state"] == "failed" and ui["error"] == "Rejected by reviewer"
        assert ui["steps"][2]["state"] == "done"  # the ticket was created
        print("B ok: jira rejected")

        # C. ticket creation fails (e.g. Jira not configured) -> the workflow FAILS and the
        #    UI shows the ticket step as failed; nothing is silently approved.
        @activity.defn(name="create_approval_ticket")
        async def create_fails(req: ApprovalRequest):
            raise ApplicationError("Jira is not configured", type="JiraConfig", non_retryable=True)

        @activity.defn(name="wait_for_ticket_decision")
        async def wait_unused(t: ApprovalTicket) -> ReviewDecision:
            raise AssertionError("poll must not start without a ticket")

        queue = f"test-{uuid.uuid4()}"
        async with Worker(
            env.client,
            task_queue=queue,
            workflows=[AddressVerificationWorkflow],
            activities=[fake_geocode, fake_verify, create_fails, wait_unused],
        ):
            handle = await env.client.start_workflow(
                AddressVerificationWorkflow.run,
                INPUT,
                id=f"wf-{uuid.uuid4()}",
                task_queue=queue,
            )
            await handle.signal(AddressVerificationWorkflow.submit_address, "Helguera 3635")
            await handle.signal(AddressVerificationWorkflow.upload_document, "/tmp/x.pdf")
            try:
                await asyncio.wait_for(handle.result(), 30)
                raise AssertionError("workflow should have failed")
            except WorkflowFailureError:
                pass
            ui = await handle.query("ui_state")
        assert [s["state"] for s in ui["steps"]] == ["done", "done", "failed", "pending"], ui
        assert "Could not create the Jira ticket" in ui["error"]
        print("C ok: ticket creation failure fails the workflow visibly")

        # D. manual signal overrides a ticket that stays open
        cancelled = asyncio.Event()

        async def hangs(_t):
            try:
                while True:
                    activity.heartbeat()
                    await asyncio.sleep(0.2)
            except asyncio.CancelledError:
                cancelled.set()
                raise

        result, _, _ = await run(
            env, TICKET, hangs, signal=ReviewDecision(False, "ops", "override")
        )
        assert result.status == "rejected" and result.reviewer == "ops"
        await asyncio.wait_for(cancelled.wait(), 10)
        print("D ok: signal overrides open ticket and cancels the poll")

        # E. Jira polling fails fatally -> falls back to the manual signal
        async def broken(_t):
            raise ApplicationError("Jira returned 401", type="JiraAuth", non_retryable=True)

        def check_e(state):
            assert "Jira polling failed" in state["error"], state

        # the error is set asynchronously; give the workflow a moment
        create, wait, _ = make_jira(TICKET, broken)
        queue = f"test-{uuid.uuid4()}"
        async with Worker(
            env.client,
            task_queue=queue,
            workflows=[AddressVerificationWorkflow],
            activities=[fake_geocode, fake_verify, create, wait],
        ):
            handle = await env.client.start_workflow(
                AddressVerificationWorkflow.run,
                INPUT,
                id=f"wf-{uuid.uuid4()}",
                task_queue=queue,
            )
            await handle.signal(AddressVerificationWorkflow.submit_address, "Helguera 3635")
            await handle.signal(AddressVerificationWorkflow.upload_document, "/tmp/x.pdf")
            for _ in range(100):
                state = await handle.query("ui_state")
                if state["error"] and "Jira polling failed" in state["error"]:
                    break
                await asyncio.sleep(0.1)
            check_e(state)
            await handle.signal(
                AddressVerificationWorkflow.review, ReviewDecision(True, "ops", "fallback")
            )
            result = await asyncio.wait_for(handle.result(), 30)
        assert result.status == "approved" and result.reviewer == "ops"
        print("E ok: falls back to signal when jira polling fails")

        # F. invalid input fails the workflow immediately (nothing else runs)
        for bad in (
            AddressVerificationInput(company_marker="../etc", customer_id=1),
            AddressVerificationInput(company_marker="acme", customer_id=-5),
        ):
            create, wait, calls = make_jira(TICKET, never_called)
            queue = f"test-{uuid.uuid4()}"
            async with Worker(
                env.client,
                task_queue=queue,
                workflows=[AddressVerificationWorkflow],
                activities=[fake_geocode, fake_verify, create, wait],
            ):
                handle = await env.client.start_workflow(
                    AddressVerificationWorkflow.run, bad, id=f"wf-{uuid.uuid4()}", task_queue=queue
                )
                try:
                    await asyncio.wait_for(handle.result(), 30)
                    raise AssertionError("invalid input should fail the workflow")
                except WorkflowFailureError:
                    pass
                ui = await handle.query("ui_state")
            assert ui["steps"][0]["state"] == "failed" and ui["error"], ui
            assert not calls["requests"]
        print("F ok: invalid input fails the workflow")

        # G. deterministic id: a second start while one runs is refused
        wf_id = address_workflow_id("acme", 42)
        assert wf_id == "AddressVerificationWorkflow-acme-42"
        queue = f"test-{uuid.uuid4()}"
        first = await env.client.start_workflow(
            AddressVerificationWorkflow.run, INPUT, id=wf_id, task_queue=queue
        )
        try:
            await env.client.start_workflow(
                AddressVerificationWorkflow.run, INPUT, id=wf_id, task_queue=queue
            )
            raise AssertionError("duplicate start should be refused")
        except WorkflowAlreadyStartedError:
            pass
        await first.terminate("test cleanup")
        print("G ok: one running verification per customer")

        # H. a document that isn't a utility bill is bounced; a second upload is accepted
        verdicts = [
            DocumentVerdict(
                match=False,
                confidence="high",
                matched_text="",
                reason="This is not a utility bill (it looks like: bank statement).",
                is_utility_bill=False,
                document_type="bank statement",
            ),
            DocumentVerdict(
                match=True,
                confidence="high",
                matched_text="HELGUERA 3635",
                reason="ok",
                is_utility_bill=True,
                document_type="water bill",
            ),
        ]

        @activity.defn(name="verify_document_with_claude")
        async def verify_sequence(input: DocumentCheckInput) -> DocumentVerdict:
            return verdicts.pop(0)

        create, wait, calls = make_jira(TICKET, approved)
        queue = f"test-{uuid.uuid4()}"
        async with Worker(
            env.client,
            task_queue=queue,
            workflows=[AddressVerificationWorkflow],
            activities=[fake_geocode, verify_sequence, create, wait],
        ):
            handle = await env.client.start_workflow(
                AddressVerificationWorkflow.run, INPUT, id=f"wf-{uuid.uuid4()}", task_queue=queue
            )
            await handle.signal(AddressVerificationWorkflow.submit_address, "Helguera 3635")
            await handle.signal(AddressVerificationWorkflow.upload_document, "/tmp/statement.pdf")
            for _ in range(100):
                state = await handle.query("ui_state")
                if state["error"] and "not a utility bill" in state["error"]:
                    break
                await asyncio.sleep(0.1)
            assert "not a utility bill" in state["error"], state
            assert state["current_step"] == "document"  # asking for a new upload
            assert state["steps"][1]["state"] == "active" and state["data"]["verdict"]["document_type"] == "bank statement"
            await handle.signal(AddressVerificationWorkflow.upload_document, "/tmp/bill.pdf")
            result = await asyncio.wait_for(handle.result(), 30)
        assert result.status == "approved"
        assert calls["requests"][0].document_type == "water bill"  # ticket uses the accepted bill
        print("H ok: non-bill document bounced, then a real bill accepted")

    print("test_address_workflow: all passed")


if __name__ == "__main__":
    asyncio.run(main())

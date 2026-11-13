import asyncio
from dataclasses import asdict
from datetime import timedelta
from typing import Optional

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError, ApplicationError

with workflow.unsafe.imports_passed_through():
    from address_activities import geocode_address, verify_document_with_claude
    from address_shared import (
        AddressVerificationInput,
        ApprovalRequest,
        validate_verification_input,
        AWAITING_ADDRESS,
        AWAITING_APPROVAL,
        AWAITING_DOCUMENT,
        CREATING_TICKET,
        DONE,
        VALIDATING_ADDRESS,
        VERIFYING_DOCUMENT,
        AddressVerificationResult,
        DocumentCheckInput,
        DocumentVerdict,
        GeocodeResult,
        ReviewDecision,
        WorkflowStatus,
    )
    from jira_activities import create_approval_ticket, wait_for_ticket_decision
    from workflow_ui import UI_STATE_QUERY, UiInput, UiState, UiTracker

MAX_ATTEMPTS = 3  # per customer-facing step
CUSTOMER_TIMEOUT = timedelta(days=1)  # how long we wait for each customer input
APPROVAL_TIMEOUT = timedelta(days=7)  # how long we wait for the human reviewer


@workflow.defn
class AddressVerificationWorkflow:
    """Address validation with proof of address and human approval.

    The "ask" steps are wait points fed by signals: an activity can't talk to
    the customer, so the workflow waits for `submit_address`, `upload_document`
    and `review`. Claude is only used for the judgment call on the document.
    """

    def __init__(self) -> None:
        self._step = AWAITING_ADDRESS
        self._company_marker: Optional[str] = None
        self._customer_id: Optional[int] = None
        self._last_error: Optional[str] = None
        self._address: Optional[str] = None  # pending (unconsumed) input
        self._document: Optional[str] = None  # pending (unconsumed) input
        self._review: Optional[ReviewDecision] = None
        self._confirmed_address: Optional[str] = None
        self._geocode: Optional[GeocodeResult] = None
        self._document_path: Optional[str] = None
        self._verdict: Optional[DocumentVerdict] = None
        self._ui = UiTracker(
            "AddressVerificationWorkflow",
            [
                ("address", "Validate address"),
                ("document", "Verify proof of address"),
                ("ticket", "Create Jira ticket"),
                ("approval", "Approval in Jira"),
            ],
        )

    # -- signals: only store input; all work happens in run() ---------------
    @workflow.signal
    def submit_address(self, address: str) -> None:
        self._address = address

    @workflow.signal
    def upload_document(self, file_path: str) -> None:
        self._document = file_path

    @workflow.signal
    def review(self, decision: ReviewDecision) -> None:
        self._review = decision

    @workflow.query
    def get_status(self) -> WorkflowStatus:
        return WorkflowStatus(
            step=self._step,
            last_error=self._last_error,
            address=self._confirmed_address,
            geocode=self._geocode,
            document_path=self._document_path,
            verdict=self._verdict,
        )

    @workflow.query(name=UI_STATE_QUERY)
    def ui_state(self) -> UiState:
        return self._ui.snapshot()

    def _result(self, status: str, reason: str, decision: Optional[ReviewDecision] = None):
        self._step = DONE
        self._ui.finish(status == "approved", reason)
        return AddressVerificationResult(
            status=status,
            reason=reason,
            address=self._confirmed_address,
            geocode=self._geocode,
            verdict=self._verdict,
            reviewer=decision.reviewer if decision else None,
            notes=decision.notes if decision else "",
            company_marker=self._company_marker,
            customer_id=self._customer_id,
        )

    @workflow.run
    async def run(self, input: AddressVerificationInput) -> AddressVerificationResult:
        self._company_marker, self._customer_id = input.company_marker, input.customer_id
        try:
            validate_verification_input(input.company_marker, input.customer_id)
        except ValueError as e:
            self._ui.begin("address")
            self._ui.finish(False, str(e))
            self._step = DONE
            raise ApplicationError(str(e), type="InvalidInput", non_retryable=True) from e
        self._ui.set_data("company_marker", input.company_marker)
        self._ui.set_data("customer_id", input.customer_id)

        # Step 1: ask for an address and check that it exists.
        for _ in range(MAX_ATTEMPTS):
            self._step = AWAITING_ADDRESS
            self._ui.begin(
                "address",
                UiInput(
                    signal="submit_address",
                    label="Customer address",
                    schema={"type": "string"},
                ),
            )
            try:
                await workflow.wait_condition(
                    lambda: self._address is not None, timeout=CUSTOMER_TIMEOUT
                )
            except asyncio.TimeoutError:
                return self._result("expired", "Customer never submitted an address")
            address, self._address = self._address, None
            self._step = VALIDATING_ADDRESS
            self._ui.waiting_for(None)
            self._ui.set_data("submitted_address", address)
            geo = await workflow.execute_activity(
                geocode_address,
                address,
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=RetryPolicy(maximum_attempts=5, initial_interval=timedelta(seconds=2)),
            )
            if geo.found:
                self._confirmed_address, self._geocode, self._last_error = address, geo, None
                self._ui.set_data("geocode", asdict(geo))
                self._ui.done("address")
                break
            self._last_error = f"Address not found: {address!r}"
            self._ui.set_error(self._last_error)
        else:
            return self._result("rejected", "No valid address after several attempts")

        # Step 2: ask for a service document and have Claude check the address.
        for _ in range(MAX_ATTEMPTS):
            self._step = AWAITING_DOCUMENT
            self._ui.begin(
                "document",
                UiInput(
                    signal="upload_document",
                    label="Upload a service document (e.g. utility bill)",
                    schema={"type": "string", "description": "file path under uploads/"},
                ),
            )
            try:
                await workflow.wait_condition(
                    lambda: self._document is not None, timeout=CUSTOMER_TIMEOUT
                )
            except asyncio.TimeoutError:
                return self._result("expired", "Customer never uploaded a document")
            path, self._document = self._document, None
            self._document_path = path
            self._step = VERIFYING_DOCUMENT
            self._ui.waiting_for(None)
            self._ui.set_data("document_path", path)
            verdict = await workflow.execute_activity(
                verify_document_with_claude,
                DocumentCheckInput(
                    address=self._confirmed_address,
                    display_name=self._geocode.display_name,
                    file_path=path,
                ),
                start_to_close_timeout=timedelta(minutes=10),
                heartbeat_timeout=timedelta(minutes=2),
                retry_policy=RetryPolicy(
                    maximum_attempts=3, initial_interval=timedelta(seconds=5)
                ),
            )
            self._verdict = verdict
            self._ui.set_data("verdict", asdict(verdict))
            if verdict.match:
                self._last_error = None
                self._ui.done("document")
                break
            self._last_error = (
                f"Document does not match the address: {verdict.reason}"
                if verdict.is_utility_bill
                else f"The document is not a utility bill: {verdict.reason}"
            )
            self._ui.set_error(self._last_error)
        else:
            return self._result("rejected", "Document did not match after several attempts")

        # Step 3: create the Jira ticket the reviewer will work from.
        self._step = CREATING_TICKET
        self._ui.begin("ticket")
        try:
            ticket = await workflow.execute_activity(
                create_approval_ticket,
                ApprovalRequest(
                    workflow_id=workflow.info().workflow_id,
                    address=self._confirmed_address,
                    display_name=self._geocode.display_name,
                    confidence=self._verdict.confidence,
                    matched_text=self._verdict.matched_text,
                    reason=self._verdict.reason,
                    company_marker=self._company_marker,
                    customer_id=self._customer_id,
                    document_type=self._verdict.document_type,
                ),
                start_to_close_timeout=timedelta(seconds=60),
                retry_policy=RetryPolicy(
                    maximum_attempts=3, initial_interval=timedelta(seconds=5)
                ),
            )
        except ActivityError as e:
            # Misconfiguration or Jira down: surface it on the workflow (FAILED) and in the UI.
            self._ui.finish(False, f"Could not create the Jira ticket: {e.cause}")
            self._step = DONE
            raise
        self._ui.set_data("approval_ticket", {"key": ticket.key, "url": ticket.url})
        self._ui.done("ticket")

        # Step 4: wait for the decision, made in Jira: closing the ticket with a resolution in
        # JIRA_APPROVED_RESOLUTIONS approves; any other resolution rejects. The `review`
        # signal stays available as a manual override (and if Jira polling breaks).
        self._step = AWAITING_APPROVAL
        self._ui.begin(
            "approval",
            UiInput(
                signal="review",
                label=f"Close Jira ticket {ticket.key}: an approving resolution approves, "
                "any other rejects",
                schema={
                    "type": "object",
                    "properties": {
                        "approved": {"type": "boolean"},
                        "reviewer": {"type": "string"},
                        "notes": {"type": "string"},
                    },
                    "required": ["approved", "reviewer"],
                },
            ),
        )
        poll = workflow.start_activity(
            wait_for_ticket_decision,
            ticket,
            start_to_close_timeout=APPROVAL_TIMEOUT,
            heartbeat_timeout=timedelta(minutes=2),
            retry_policy=RetryPolicy(maximum_attempts=5, initial_interval=timedelta(seconds=10)),
        )

        deadline = workflow.time() + APPROVAL_TIMEOUT.total_seconds()
        decision: Optional[ReviewDecision] = None
        while decision is None:
            try:
                await workflow.wait_condition(
                    lambda: self._review is not None or (poll is not None and poll.done()),
                    timeout=max(0.0, deadline - workflow.time()),
                )
            except asyncio.TimeoutError:
                if poll is not None:
                    poll.cancel()
                return self._result("expired", "No reviewer decision in time")
            if self._review is not None:  # manual override wins
                decision = self._review
                if poll is not None and not poll.done():
                    poll.cancel()
            else:  # the Jira poll finished
                try:
                    decision = poll.result()
                except ActivityError as e:
                    # e.g. bad Jira credentials: fall back to the manual signal.
                    self._ui.set_error(f"Jira polling failed ({e.cause}); use the review signal")
                    poll = None
        self._ui.set_data("review", asdict(decision))
        if decision.approved:
            self._ui.done("approval")
            return self._result("approved", "Approved by reviewer", decision)
        return self._result("rejected", "Rejected by reviewer", decision)

import asyncio
from datetime import timedelta
from typing import Optional

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from address_activities import geocode_address, verify_document_with_claude
    from address_shared import (
        AWAITING_ADDRESS,
        AWAITING_APPROVAL,
        AWAITING_DOCUMENT,
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
        self._last_error: Optional[str] = None
        self._address: Optional[str] = None  # pending (unconsumed) input
        self._document: Optional[str] = None  # pending (unconsumed) input
        self._review: Optional[ReviewDecision] = None
        self._confirmed_address: Optional[str] = None
        self._geocode: Optional[GeocodeResult] = None
        self._document_path: Optional[str] = None
        self._verdict: Optional[DocumentVerdict] = None

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

    def _result(self, status: str, reason: str, decision: Optional[ReviewDecision] = None):
        self._step = DONE
        return AddressVerificationResult(
            status=status,
            reason=reason,
            address=self._confirmed_address,
            geocode=self._geocode,
            verdict=self._verdict,
            reviewer=decision.reviewer if decision else None,
            notes=decision.notes if decision else "",
        )

    @workflow.run
    async def run(self) -> AddressVerificationResult:
        # Step 1: ask for an address and check that it exists.
        for _ in range(MAX_ATTEMPTS):
            self._step = AWAITING_ADDRESS
            try:
                await workflow.wait_condition(
                    lambda: self._address is not None, timeout=CUSTOMER_TIMEOUT
                )
            except asyncio.TimeoutError:
                return self._result("expired", "Customer never submitted an address")
            address, self._address = self._address, None
            self._step = VALIDATING_ADDRESS
            geo = await workflow.execute_activity(
                geocode_address,
                address,
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=RetryPolicy(maximum_attempts=5, initial_interval=timedelta(seconds=2)),
            )
            if geo.found:
                self._confirmed_address, self._geocode, self._last_error = address, geo, None
                break
            self._last_error = f"Address not found: {address!r}"
        else:
            return self._result("rejected", "No valid address after several attempts")

        # Step 2: ask for a service document and have Claude check the address.
        for _ in range(MAX_ATTEMPTS):
            self._step = AWAITING_DOCUMENT
            try:
                await workflow.wait_condition(
                    lambda: self._document is not None, timeout=CUSTOMER_TIMEOUT
                )
            except asyncio.TimeoutError:
                return self._result("expired", "Customer never uploaded a document")
            path, self._document = self._document, None
            self._document_path = path
            self._step = VERIFYING_DOCUMENT
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
            if verdict.match:
                self._last_error = None
                break
            self._last_error = f"Document does not match the address: {verdict.reason}"
        else:
            return self._result("rejected", "Document did not match after several attempts")

        # Step 3: manual approval by a human reviewer.
        self._step = AWAITING_APPROVAL
        try:
            await workflow.wait_condition(
                lambda: self._review is not None, timeout=APPROVAL_TIMEOUT
            )
        except asyncio.TimeoutError:
            return self._result("expired", "No reviewer decision in time")
        decision = self._review
        if decision.approved:
            return self._result("approved", "Approved by reviewer", decision)
        return self._result("rejected", "Rejected by reviewer", decision)

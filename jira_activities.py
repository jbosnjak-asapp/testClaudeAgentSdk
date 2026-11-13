import asyncio
from typing import Optional

import httpx
from temporalio import activity
from temporalio.exceptions import ApplicationError

from address_shared import ApprovalRequest, ApprovalTicket, ReviewDecision
from jira_client import (
    JiraAuthError,
    JiraConfig,
    JiraConfigError,
    create_ticket,
    find_ticket,
    get_decision,
    label_for,
    make_client,
    ticket_summary,
)


def _config() -> Optional[JiraConfig]:
    try:
        return JiraConfig.from_env()
    except JiraConfigError as e:
        raise ApplicationError(str(e), type="JiraConfig", non_retryable=True) from e


@activity.defn
async def create_approval_ticket(req: ApprovalRequest) -> ApprovalTicket:
    """Create the Jira ticket ("Address verification <workflow id>") a human closes to
    decide. Fails (non-retryable) if Jira isn't configured.

    Idempotent: a retry finds the ticket by its workflow-id label instead of creating
    a duplicate."""
    cfg = _config()
    if cfg is None:
        raise ApplicationError(
            "Jira is not configured: set JIRA_BASE_URL, JIRA_EMAIL and JIRA_API_TOKEN "
            "on the worker",
            type="JiraConfig",
            non_retryable=True,
        )
    label = label_for(req.workflow_id)
    lines = [
        f"Company: {req.company_marker}" if req.company_marker else "",
        f"Customer id: {req.customer_id}" if req.customer_id is not None else "",
        f"Customer address: {req.address}",
        f"Verified location (OpenStreetMap): {req.display_name}",
        f"Document type: {req.document_type}" if req.document_type else "",
        f"Claude's document check: match, confidence {req.confidence}",
        f"Matched text in the document: {req.matched_text}",
        f"Reason: {req.reason}",
        f"Temporal workflow: {req.workflow_id}",
        "To decide, close this ticket. Resolution "
        f"{'/'.join(sorted(cfg.approved_resolutions))} approves; any other resolution rejects.",
    ]
    try:
        async with make_client(cfg) as client:
            key = await find_ticket(client, cfg, label) or await create_ticket(
                client, cfg, ticket_summary(req.workflow_id), lines, label
            )
    except JiraAuthError as e:
        raise ApplicationError(str(e), type="JiraAuth", non_retryable=True) from e
    return ApprovalTicket(key=key, url=f"{cfg.base_url}/browse/{key}")


@activity.defn
async def wait_for_ticket_decision(ticket: ApprovalTicket) -> ReviewDecision:
    """Poll the ticket until it is closed, then map its resolution to a decision.

    Heartbeats every poll (so a dead worker is noticed and cancellation is delivered).
    Transient Jira errors are logged and polling continues; auth errors are fatal."""
    cfg = _config()
    if cfg is None:
        raise ApplicationError("Jira is not configured", type="JiraConfig", non_retryable=True)
    async with make_client(cfg) as client:
        while True:
            activity.heartbeat(ticket.key)
            try:
                decision = await get_decision(client, cfg, ticket.key)
            except JiraAuthError as e:
                raise ApplicationError(str(e), type="JiraAuth", non_retryable=True) from e
            except httpx.HTTPError as e:
                activity.logger.warning("Jira poll failed (%s); will retry", e)
                decision = None
            if decision is not None:
                return ReviewDecision(
                    approved=decision.approved,
                    reviewer=decision.decided_by,
                    notes=f"Jira {ticket.key} closed as '{decision.resolution}'",
                )
            await asyncio.sleep(cfg.poll_seconds)

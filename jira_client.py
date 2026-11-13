"""Minimal Jira Cloud REST client (httpx) for the approval ticket.

Configuration (environment, never in the repo):
  JIRA_BASE_URL              https://<your-site>.atlassian.net
  JIRA_EMAIL, JIRA_API_TOKEN account + API token used by the worker (basic auth)
  JIRA_PROJECT_KEY           project that receives approval tickets (default "CB")
  JIRA_ISSUE_TYPE            default "Task"
  JIRA_APPROVED_RESOLUTIONS  comma list, default "Done,Approved,Fixed". A closed ticket
                             with any OTHER resolution (or none) is treated as REJECTED.
  JIRA_POLL_SECONDS          default 30
  JIRA_ASSIGNEE_ACCOUNT_ID   optional: assign the ticket to a reviewer
"""
import os
import re
from urllib.parse import urlparse
from dataclasses import dataclass
from typing import Any, Optional

import httpx

_PROJECT_RE = re.compile(r"^[A-Z][A-Z0-9_]+$")


class JiraConfigError(Exception):
    pass


class JiraAuthError(Exception):
    """401/403/404: retrying will not help."""


@dataclass
class JiraConfig:
    base_url: str
    email: str
    api_token: str
    project_key: str
    issue_type: str = "Task"
    approved_resolutions: frozenset = frozenset({"done", "approved", "fixed"})
    poll_seconds: float = 30.0
    assignee_account_id: Optional[str] = None

    @classmethod
    def from_env(cls) -> Optional["JiraConfig"]:
        """None when Jira isn't configured at all (manual signal approval only)."""
        base = os.getenv("JIRA_BASE_URL", "").strip().rstrip("/")
        if not base:
            return None
        host = urlparse(base).hostname or ""
        if not base.startswith("https://") or host == "atlassian.net" or "." not in host:
            raise JiraConfigError(
                "JIRA_BASE_URL must be your Jira Cloud site, e.g. https://<yourcompany>.atlassian.net "
                f"(got {base!r})"
            )
        missing = [n for n in ("JIRA_EMAIL", "JIRA_API_TOKEN") if not os.getenv(n, "").strip()]
        if missing:
            raise JiraConfigError(f"JIRA_BASE_URL is set but missing: {', '.join(missing)}")
        project = os.getenv("JIRA_PROJECT_KEY", "CB").strip()
        if not _PROJECT_RE.match(project):
            raise JiraConfigError("JIRA_PROJECT_KEY must look like ABC or ABC_1")
        resolutions = os.getenv("JIRA_APPROVED_RESOLUTIONS", "Done,Approved,Fixed")
        return cls(
            base_url=base,
            email=os.environ["JIRA_EMAIL"].strip(),
            api_token=os.environ["JIRA_API_TOKEN"].strip(),
            project_key=project,
            issue_type=os.getenv("JIRA_ISSUE_TYPE", "Task").strip() or "Task",
            approved_resolutions=frozenset(
                r.strip().lower() for r in resolutions.split(",") if r.strip()
            ),
            poll_seconds=float(os.getenv("JIRA_POLL_SECONDS", "30")),
            assignee_account_id=os.getenv("JIRA_ASSIGNEE_ACCOUNT_ID") or None,
        )


def make_client(cfg: JiraConfig, transport: Optional[httpx.AsyncBaseTransport] = None):
    return httpx.AsyncClient(
        base_url=cfg.base_url,
        auth=(cfg.email, cfg.api_token),
        headers={"Accept": "application/json"},
        timeout=20,
        transport=transport,
    )


def _check(response: httpx.Response) -> None:
    if 300 <= response.status_code < 400:
        # Jira's REST API never redirects: this means JIRA_BASE_URL points somewhere else.
        raise JiraAuthError(
            f"Jira redirected to {response.headers.get('location', '?')!r}: check JIRA_BASE_URL"
        )
    if response.status_code in (401, 403, 404):
        raise JiraAuthError(f"Jira returned {response.status_code} for {response.request.url.path}")
    response.raise_for_status()


def ticket_summary(workflow_id: str) -> str:
    return f"Address verification {workflow_id}"


def label_for(workflow_id: str) -> str:
    """Jira labels can't contain spaces; used to find the ticket again on retries."""
    return re.sub(r"[^A-Za-z0-9_.\-]", "-", workflow_id)


def adf(lines: list[str]) -> dict[str, Any]:
    """Atlassian Document Format (Jira Cloud v3 requires it for descriptions)."""
    return {
        "type": "doc",
        "version": 1,
        "content": [
            {"type": "paragraph", "content": [{"type": "text", "text": line}]}
            for line in lines
            if line
        ],
    }


async def find_ticket(client: httpx.AsyncClient, cfg: JiraConfig, label: str) -> Optional[str]:
    response = await client.get(
        "/rest/api/3/search/jql",
        params={
            "jql": f'project = "{cfg.project_key}" AND labels = "{label}"',
            "fields": "key",
            "maxResults": 1,
        },
    )
    _check(response)
    issues = response.json().get("issues", [])
    return issues[0]["key"] if issues else None


async def create_ticket(
    client: httpx.AsyncClient, cfg: JiraConfig, summary: str, lines: list[str], label: str
) -> str:
    fields: dict[str, Any] = {
        "project": {"key": cfg.project_key},
        "issuetype": {"name": cfg.issue_type},
        "summary": summary[:250],
        "description": adf(lines),
        "labels": ["address-verification", label],
    }
    if cfg.assignee_account_id:
        fields["assignee"] = {"accountId": cfg.assignee_account_id}
    response = await client.post("/rest/api/3/issue", json={"fields": fields})
    _check(response)
    return response.json()["key"]


@dataclass
class TicketDecision:
    approved: bool
    resolution: str
    decided_by: str


async def get_decision(
    client: httpx.AsyncClient, cfg: JiraConfig, key: str
) -> Optional[TicketDecision]:
    """None while the ticket is still open. Fails closed: only an explicitly approved
    resolution counts as approval."""
    response = await client.get(
        f"/rest/api/3/issue/{key}",
        params={"fields": "status,resolution", "expand": "changelog"},
    )
    _check(response)
    body = response.json()
    fields = body.get("fields", {})
    category = ((fields.get("status") or {}).get("statusCategory") or {}).get("key")
    if category != "done":
        return None
    resolution = (fields.get("resolution") or {}).get("name") or ""

    decided_by = "jira"
    for history in reversed((body.get("changelog") or {}).get("histories", [])):
        if any(i.get("field") in ("status", "resolution") for i in history.get("items", [])):
            decided_by = (history.get("author") or {}).get("displayName") or decided_by
            break
    return TicketDecision(
        approved=resolution.lower() in cfg.approved_resolutions,
        resolution=resolution or "(none)",
        decided_by=decided_by,
    )

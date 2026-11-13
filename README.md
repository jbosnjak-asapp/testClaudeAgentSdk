# testClaudeAgentSdk

Temporal workflows (Python) where some steps run the Claude Agent SDK, plus a small read-only API that feeds the
"Long Horizon Agent" tab of the AI Console so you can watch the workflows.

| Workflow | What it does |
|---|---|
| `ClaudeAgentWorkflow` | Minimal example: runs a Claude agent on a question, then summarizes the result. |
| `AddressVerificationWorkflow` | Validates an address (OpenStreetMap Nominatim), has Claude check that an uploaded document is a **utility bill** showing that address, creates a **Jira ticket** and waits for someone to close it: the approval happens in Jira. |

## Prerequisites

- [uv](https://docs.astral.sh/uv/) (installs the right Python for you; the Claude Agent SDK needs Python 3.10+, `.python-version` pins 3.12)
- [Temporal CLI](https://docs.temporal.io/cli) for a local dev server (also used by the workflow tests)
- Access to Claude: an Anthropic API key, or a LiteLLM proxy URL and token
- For address verification: a Jira Cloud site, an [API token](https://id.atlassian.com/manage-profile/security/api-tokens), and a project to receive tickets (default `CB`)

## Setup

```bash
cd testClaudeAgentSdk
uv python install 3.12
uv sync
```

## Configuration

Set these in the terminal that runs the **worker** (the agents and the Jira calls run inside the worker process).
Never commit tokens; `.env` is git-ignored.

```bash
# Claude: option A, Anthropic API directly
export ANTHROPIC_API_KEY=...
# Claude: option B, LiteLLM proxy (use instead of A, do not set both)
export ANTHROPIC_BASE_URL=https://<your-litellm-host>
export ANTHROPIC_AUTH_TOKEN=...
# export ANTHROPIC_MODEL=<model name your proxy serves>   # if the default model isn't mapped

# Address workflow: Nominatim requires an identifying User-Agent
export NOMINATIM_USER_AGENT="MyApp/1.0 (you@example.com)"

# Address workflow: Jira (see "Approval through a Jira ticket")
export JIRA_BASE_URL=https://<your-site>.atlassian.net     # the site, not just atlassian.net
export JIRA_EMAIL=...            # account that owns the API token
export JIRA_API_TOKEN=...
```

Optional: `TEMPORAL_ADDRESS` (default `localhost:7233`) and `TEMPORAL_NAMESPACE` (default `default`), read by the worker, the CLIs and the API.

## Run

Use separate terminals, all in this folder:

```bash
# 1. Local Temporal server (UI at http://localhost:8233)
temporal server start-dev

# 2. Worker: serves both workflows (needs the Claude and Jira env vars above)
uv run python worker.py

# 3. (optional) Workflow API for the console tab, see "Workflow API and console"
uv run uvicorn api:app --port 8000

# 4. Start a workflow (see below)
```

Restart the worker after changing workflow or activity code, and the API after changing `api.py` (it does not auto-reload).

### Example workflow

```bash
uv run python starter.py "What files are in this directory and what do they do?"
```

### Address verification

Start one for a company and customer; the workflow id is `AddressVerificationWorkflow-<companyMarker>-<customerId>`.
There is at most **one running verification per customer** (a second start is refused until the first finishes; after that
the same id can be used again). The company marker may contain letters, digits, `_`, `.` and `-`; the customer id is a
non-negative number.

```bash
uv run python address_cli.py start mtagnostic1 12345        # prints the workflow id
uv run python address_cli.py address <id> "Av. Corrientes 1000, Buenos Aires, Argentina"
uv run python address_cli.py status  <id>                   # current step, errors, Claude's verdict
uv run python address_cli.py upload  <id> ./bill.pdf        # proof of address (PDF/image, max 10MB)
uv run python address_cli.py result  <id>                   # waits for and prints the final result
uv run python address_cli.py approve <id> <your-name> [notes]   # manual override, or: reject
```

Steps (as shown in the console):

1. **Validate address**: waits for an address and checks it exists on OpenStreetMap.
2. **Verify proof of address**: waits for an uploaded document. The Claude agent first classifies it: it must be a
   **utility bill** (electricity, gas, water, internet, phone or cable). A bank statement, ID, letter or any other document is
   bounced with "not a utility bill", and `match` is forced to false in code, whatever the model says. Only then does it check
   that the bill shows the address.
3. **Create Jira ticket**: see below.
4. **Approval in Jira**: see below.

An unknown address, a non-matching document or a non-bill can be retried up to 3 times; then the workflow is `rejected`.
The customer steps expire after 1 day and the approval after 7 days (`expired`). The final status is `approved`,
`rejected` or `expired`.

### Approval through a Jira ticket

The manual part happens in Jira:

1. **Create Jira ticket.** A ticket titled `Address verification <workflow id>` is created in project `CB` (override with
   `JIRA_PROJECT_KEY`) with the company, customer id, address, verified location, document type, Claude's verdict and the
   matching text. A retry finds the existing ticket by a workflow-id label instead of creating a duplicate. The ticket is
   required: if Jira isn't configured or the creation fails, the workflow **fails** (FAILED, with the ticket step marked
   failed in the console).
2. **Approval in Jira.** The workflow polls the ticket. **Closing it decides**: a resolution in `JIRA_APPROVED_RESOLUTIONS`
   (default `Done,Approved,Fixed`) approves; any other resolution, or none, rejects (fails closed). The reviewer name comes
   from the ticket's changelog. The `review` signal (`address_cli.py approve|reject`) remains as a manual override and as the
   fallback if Jira polling fails (e.g. bad credentials).

The ticket key and link appear in the workflow's details in the console. Optional settings: `JIRA_PROJECT_KEY` (default
`CB`), `JIRA_ISSUE_TYPE` (default `Task`), `JIRA_APPROVED_RESOLUTIONS`, `JIRA_POLL_SECONDS` (30), `JIRA_ASSIGNEE_ACCOUNT_ID`.
The token's account needs permission to create and browse issues in the project. The ticket contains the customer's
address, so pick a project with suitable access. The bill itself is not attached. Your Jira workflow's close transition must
set a resolution that is in the approved list.

Workflows started before a change to the workflow code may not be replayable by the new code; terminate them and start new ones.

## Workflow API and console

`api.py` is a read-only HTTP API over Temporal for a generic workflow UI: the **Long Horizon Agent** tab on the Beta
Configurations page of the AI Console (`ai-console/ai-console-app-ga-beta-configurations`, internal users only).

```bash
uv run uvicorn api:app --port 8000        # binds 127.0.0.1; no auth, keep it local or behind an authenticated proxy
```

Open `/company/<partnerId>/generative-agent-beta-configurations?tab=long-horizon-agent`. The tab has three levels, each
deep-linkable (`&type=<type>`, `&id=<workflow id>`):

1. **Workflow types** with instance counts.
2. A **table of instances** of a type: workflow name, id, start date, status, end date, with status filters.
3. **One workflow**: its steps, current state, errors, what it's waiting for and its data. The **View events** button opens a
   popup with the event history in Temporal's compact style (activities, timers and signals grouped, newest first).

The tab's "Workflow API" line (collapsed) sets the API URL; the default is `http://localhost:8000`, stored in the browser.
The tab calls the API directly from the browser, so the API enables CORS for `localhost` origins only (override with
`WORKFLOW_API_CORS_ORIGIN_REGEX`).

| Endpoint | Returns |
|---|---|
| `GET /health` | `{"status": "ok"}` |
| `GET /workflow-types` | Workflow types with instance counts (from the latest 1000 executions) |
| `GET /workflows?type=&status=&limit=` | Recent workflows (limit up to 200) |
| `GET /workflows/{id}` | Lifecycle status plus `ui_state`; if the workflow doesn't implement the contract, an event `timeline` instead |
| `GET /workflows/{id}/events` | The event history grouped into compact rows, plus pending activities |

**To make any workflow show up with named steps**, follow the contract in `workflow_ui.py`: create a `UiTracker` with the
ordered steps, call `begin` / `done` / `set_error` / `finish` at each transition, and expose it with
`@workflow.query(name=UI_STATE_QUERY)`. See `address_workflow.py` for an example. Only ever add fields to the contract.
`ui_state` is a query, so it needs a running worker; the API gives up after 5s and falls back to the timeline.

## Tests

No Jira, Claude or network needed. The workflow test runs the real workflow against a local Temporal dev server (started
through the `temporal` CLI) with fake activities.

```bash
uv run python test_jira.py              # Jira client against mocked HTTP
uv run python test_verdict.py           # utility-bill / verdict rules
uv run python test_address_workflow.py  # full workflow scenarios, id convention, compact events
```

## Layout

| File | Purpose |
|---|---|
| `worker.py` | Registers workflows and activities, runs the worker |
| `workflows.py`, `activities.py`, `shared.py`, `starter.py` | Example workflow |
| `address_workflow.py`, `address_activities.py`, `address_shared.py`, `address_cli.py` | Address verification: workflow, activities (geocode, document check), types and id convention, CLI |
| `jira_client.py`, `jira_activities.py` | Jira approval ticket: create it, poll until closed |
| `workflow_ui.py` | UI contract: steps, current step, pending input, data |
| `workflow_events.py`, `workflow_types.py` | Compact event history and per-type summaries for the API |
| `api.py` | The read-only Workflow API |
| `agent_options.py` | Locked-down Claude agent options shared by all agent steps |
| `test_*.py` | Tests (see above) |

## Notes

- **The agent is sandboxed on purpose.** It only gets `Read`, `Grep` and `Glob`, and does not load your Claude Code settings, plugins, hooks or MCP servers. Keep it that way for an unattended worker.
- **Uploads stay local.** `address_cli.py upload` copies the file to `uploads/<workflow_id>/` and signals the path (Temporal payloads are limited to ~2MB), so the CLI and worker must share a filesystem. Use object storage for anything beyond local testing. The activity refuses paths outside `uploads/` and files over 10MB.
- **Customer data.** The address, company marker and customer id appear in the Temporal history, the console and the Jira ticket. Pick access levels accordingly.
- **No authentication yet.** The Workflow API has none, and the reviewer name on the `review` signal is whatever the CLI is given. Put an authenticated service in front before real use.
- **Troubleshooting:**
  - The worker logs the Claude CLI's stderr (`claude stderr: ...`). "Connection error" or "Slow first byte" means the model endpoint isn't reachable: check the base URL, VPN and token in the worker's terminal. The "claude.ai connectors are disabled" warning is harmless.
  - Jira "redirected" or "JIRA_BASE_URL must be your Jira Cloud site": use `https://<yourcompany>.atlassian.net`. A 401/403 means the email or token is wrong or lacks permission on the project.
  - The console tab shows an error or nothing: check the API is running (`http://localhost:8000/workflows`) and the "Workflow API" URL. A "route not found" message means the API process predates an update: restart it.

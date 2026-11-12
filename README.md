# testClaudeAgentSdk

Temporal workflows (Python) where one step runs the Claude Agent SDK.

| Workflow | What it does |
|---|---|
| `ClaudeAgentWorkflow` | Minimal example: runs a Claude agent on a question, then summarizes the result. |
| `AddressVerificationWorkflow` | Validates an address (OpenStreetMap Nominatim), has Claude check an uploaded service document contains that address, then waits for a human approval. |

## Prerequisites

- [uv](https://docs.astral.sh/uv/) (it installs the right Python for you; the Claude Agent SDK needs Python 3.10+, `.python-version` pins 3.12)
- [Temporal CLI](https://docs.temporal.io/cli) for a local dev server
- Access to Claude: either an Anthropic API key, or a LiteLLM proxy URL and token

## Setup

```bash
cd testClaudeAgentSdk
uv python install 3.12
uv sync
```

## Configuration

Set these in the terminal that runs the **worker** (the agent runs inside the worker process):

```bash
# Option A: Anthropic API directly
export ANTHROPIC_API_KEY=...

# Option B: LiteLLM proxy (use instead of A, do not set both)
export ANTHROPIC_BASE_URL=https://<your-litellm-host>
export ANTHROPIC_AUTH_TOKEN=...
# export ANTHROPIC_MODEL=<model name your proxy serves>   # if the default model isn't mapped

# Recommended for the address workflow: Nominatim requires an identifying User-Agent
export NOMINATIM_USER_AGENT="MyApp/1.0 (you@example.com)"
```

Optional: `TEMPORAL_ADDRESS` (default `localhost:7233`) and `TEMPORAL_NAMESPACE` (default `default`), used by the worker and the CLIs.

Never commit tokens. `.env` is git-ignored.

## Run

Use three terminals, all in this folder.

```bash
# 1. Local Temporal server (UI at http://localhost:8233)
temporal server start-dev

# 2. Worker (serves both workflows; needs the Claude env vars above)
uv run python worker.py

# 3. Start a workflow (see below)
```

### Example workflow

```bash
uv run python starter.py "What files are in this directory and what do they do?"
```

### Address verification

Steps are driven with signals through `address_cli.py`:

```bash
uv run python address_cli.py start                      # prints the workflow id
uv run python address_cli.py address <id> "Av. Corrientes 1000, Buenos Aires, Argentina"
uv run python address_cli.py status  <id>               # current step, errors, Claude's verdict
uv run python address_cli.py upload  <id> ./bill.pdf    # proof of address (PDF/image, max 10MB)
uv run python address_cli.py approve <id> <your-name> [notes]   # or: reject
uv run python address_cli.py result  <id>
```

Flow: `awaiting_address` → `validating_address` → `awaiting_document` → `verifying_document` → `awaiting_approval` → `done`.

- An unknown address or a non-matching document can be retried up to 3 times; then the workflow is `rejected`.
- Customer steps expire after 1 day, the approval after 7 days (`expired`).
- Final status is `approved`, `rejected` or `expired`.

## Layout

| File | Purpose |
|---|---|
| `worker.py` | Registers workflows and activities, runs the worker |
| `workflows.py`, `activities.py`, `shared.py`, `starter.py` | Example workflow |
| `address_workflow.py`, `address_activities.py`, `address_shared.py`, `address_cli.py` | Address verification |
| `agent_options.py` | Locked-down Claude agent options shared by all agent steps |

## Notes

- **The agent is sandboxed on purpose.** It only gets `Read`, `Grep` and `Glob`, and does not load your Claude Code settings, plugins, hooks or MCP servers. Keep it that way for an unattended worker.
- **Uploads stay local.** `address_cli.py upload` copies the file to `uploads/<workflow_id>/` and signals the path (Temporal payloads are limited to ~2MB), so the CLI and worker must share a filesystem. Use object storage for anything beyond local testing. The activity refuses paths outside `uploads/`.
- **Reviewer identity is not verified.** The name passed to `approve`/`reject` is whatever the CLI is given; put an authenticated service in front before real use.
- **Troubleshooting:** the worker logs the Claude CLI's stderr (`claude stderr: ...`). "Connection error" or "Slow first byte" means the model endpoint isn't reachable: check the base URL, VPN and token in the worker's terminal. The "claude.ai connectors are disabled" warning is harmless.

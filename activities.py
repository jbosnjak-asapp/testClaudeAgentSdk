import asyncio

from claude_agent_sdk import ClaudeAgentOptions, ResultMessage, query
from temporalio import activity
from temporalio.exceptions import ApplicationError

from agent_options import readonly_agent_options
from shared import AgentInput, AgentResult


@activity.defn
async def run_claude_agent(input: AgentInput) -> AgentResult:
    """Run a Claude agent to completion via the Claude Agent SDK.

    Heartbeats on every streamed message so Temporal can detect a hung agent
    (and deliver cancellation). Requires ANTHROPIC_API_KEY in the worker env.
    """
    options = readonly_agent_options(cwd=input.cwd, max_turns=input.max_turns)

    # Heartbeat on a timer too: a slow model call can be silent for a while,
    # and we don't want that mistaken for a hung worker.
    async def _heartbeat_loop() -> None:
        while True:
            activity.heartbeat()
            await asyncio.sleep(10)

    hb = asyncio.create_task(_heartbeat_loop())
    try:
        return await _run(input, options)
    finally:
        hb.cancel()


async def _run(input: AgentInput, options: ClaudeAgentOptions) -> AgentResult:
    count = 0
    async for message in query(prompt=input.prompt, options=options):
        count += 1
        activity.logger.info("claude message %d: %r", count, message)
        if isinstance(message, ResultMessage):
            if message.is_error or message.subtype != "success":
                # Max turns / execution errors won't fix themselves on retry.
                raise ApplicationError(
                    f"Claude agent ended with {message.subtype}",
                    type="AgentError",
                    non_retryable=True,
                )
            return AgentResult(
                result=message.result or "",
                num_turns=message.num_turns,
                total_cost_usd=message.total_cost_usd or 0.0,
                session_id=message.session_id,
            )
    raise ApplicationError("Claude agent stream ended without a result", type="NoResult")


@activity.defn
async def summarize(agent: AgentResult) -> str:
    """Trivial deterministic step to show the agent as one step among others."""
    return f"{agent.num_turns} turns, ${agent.total_cost_usd:.4f}: {agent.result[:200]}"

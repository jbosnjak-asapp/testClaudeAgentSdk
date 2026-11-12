from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from activities import run_claude_agent, summarize
    from shared import AgentInput, WorkflowInput, WorkflowOutput


@workflow.defn
class ClaudeAgentWorkflow:
    @workflow.run
    async def run(self, input: WorkflowInput) -> WorkflowOutput:
        # Step 1: run the Claude agent. Long-running and non-deterministic:
        # generous timeout, heartbeats to detect hangs, limited retries.
        agent = await workflow.execute_activity(
            run_claude_agent,
            AgentInput(
                prompt=f"Research the following topic in the current directory "
                f"and answer concisely: {input.topic}",
                cwd=input.cwd,
            ),
            start_to_close_timeout=timedelta(minutes=15),
            heartbeat_timeout=timedelta(minutes=1),
            retry_policy=RetryPolicy(
                maximum_attempts=3,
                initial_interval=timedelta(seconds=5),
                backoff_coefficient=2.0,
            ),
        )

        # Step 2: downstream processing of the agent output
        summary = await workflow.execute_activity(
            summarize, agent, start_to_close_timeout=timedelta(seconds=30)
        )
        return WorkflowOutput(topic=input.topic, agent=agent, summary=summary)

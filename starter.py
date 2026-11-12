import asyncio
import os
import sys
import time

from temporalio.client import Client

from shared import TASK_QUEUE, WorkflowInput
from workflows import ClaudeAgentWorkflow


async def main() -> None:
    client = await Client.connect(
        os.getenv("TEMPORAL_ADDRESS", "localhost:7233"),
        namespace=os.getenv("TEMPORAL_NAMESPACE", "default"),
    )
    topic = sys.argv[1] if len(sys.argv) > 1 else "What files are in this directory and what do they do?"
    result = await client.execute_workflow(
        ClaudeAgentWorkflow.run,
        WorkflowInput(topic=topic, cwd=os.getcwd()),
        id=f"claude-agent-{int(time.time())}",
        task_queue=TASK_QUEUE,
    )
    print(result)


if __name__ == "__main__":
    asyncio.run(main())

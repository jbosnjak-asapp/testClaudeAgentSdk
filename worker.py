import asyncio
import logging
import os

from temporalio.client import Client
from temporalio.worker import Worker

from activities import run_claude_agent, summarize
from address_activities import geocode_address, verify_document_with_claude
from address_workflow import AddressVerificationWorkflow
from jira_activities import create_approval_ticket, wait_for_ticket_decision
from shared import TASK_QUEUE
from workflows import ClaudeAgentWorkflow


async def main() -> None:
    logging.basicConfig(level=logging.INFO)
    client = await Client.connect(
        os.getenv("TEMPORAL_ADDRESS", "localhost:7233"),
        namespace=os.getenv("TEMPORAL_NAMESPACE", "default"),
    )
    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[ClaudeAgentWorkflow, AddressVerificationWorkflow],
        activities=[
            run_claude_agent,
            summarize,
            geocode_address,
            verify_document_with_claude,
            create_approval_ticket,
            wait_for_ticket_decision,
        ],
    )
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())

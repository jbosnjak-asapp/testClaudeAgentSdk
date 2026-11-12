"""CLI for the address verification workflow.

  uv run python address_cli.py start                       # prints workflow id
  uv run python address_cli.py address <id> "Av. Corrientes 1000, Buenos Aires"
  uv run python address_cli.py upload  <id> ./bill.pdf
  uv run python address_cli.py status  <id>
  uv run python address_cli.py approve <id> <your-name> [notes]
  uv run python address_cli.py reject  <id> <your-name> [notes]
  uv run python address_cli.py result  <id>
"""
import asyncio
import os
import shutil
import sys
import time
from pathlib import Path

from temporalio.client import Client

from address_activities import UPLOAD_ROOT
from address_shared import ADDRESS_TASK_QUEUE, ReviewDecision
from address_workflow import AddressVerificationWorkflow


async def main() -> None:
    if len(sys.argv) < 2:
        sys.exit(__doc__)
    cmd, args = sys.argv[1], sys.argv[2:]
    client = await Client.connect(
        os.getenv("TEMPORAL_ADDRESS", "localhost:7233"),
        namespace=os.getenv("TEMPORAL_NAMESPACE", "default"),
    )

    if cmd == "start":
        wf_id = f"address-verification-{int(time.time())}"
        await client.start_workflow(
            AddressVerificationWorkflow.run, id=wf_id, task_queue=ADDRESS_TASK_QUEUE
        )
        print(wf_id)
        return

    handle = client.get_workflow_handle_for(AddressVerificationWorkflow.run, args[0])
    if cmd == "address":
        await handle.signal(AddressVerificationWorkflow.submit_address, args[1])
    elif cmd == "upload":
        # Signals carry a path, not the file (Temporal payloads are ~2MB max).
        # The worker must see the same filesystem as this CLI.
        src = Path(args[1])
        dest_dir = UPLOAD_ROOT / args[0]
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest = dest_dir / src.name
        shutil.copy(src, dest)
        await handle.signal(AddressVerificationWorkflow.upload_document, str(dest))
    elif cmd in ("approve", "reject"):
        decision = ReviewDecision(
            approved=cmd == "approve",
            reviewer=args[1],
            notes=" ".join(args[2:]),
        )
        await handle.signal(AddressVerificationWorkflow.review, decision)
    elif cmd == "status":
        print(await handle.query(AddressVerificationWorkflow.get_status))
    elif cmd == "result":
        print(await handle.result())
    else:
        sys.exit(__doc__)


if __name__ == "__main__":
    asyncio.run(main())

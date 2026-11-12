from dataclasses import dataclass
from typing import Optional

TASK_QUEUE = "claude-agent-sdk"


@dataclass
class AgentInput:
    prompt: str
    cwd: Optional[str] = None
    max_turns: int = 10


@dataclass
class AgentResult:
    result: str
    num_turns: int
    total_cost_usd: float
    session_id: str


@dataclass
class WorkflowInput:
    topic: str
    cwd: Optional[str] = None


@dataclass
class WorkflowOutput:
    topic: str
    agent: AgentResult
    summary: str

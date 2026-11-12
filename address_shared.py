from dataclasses import dataclass, field
from typing import Optional

ADDRESS_TASK_QUEUE = "claude-agent-sdk"  # same queue/worker as the first workflow

# Workflow steps (returned by the get_status query)
AWAITING_ADDRESS = "awaiting_address"
VALIDATING_ADDRESS = "validating_address"
AWAITING_DOCUMENT = "awaiting_document"
VERIFYING_DOCUMENT = "verifying_document"
AWAITING_APPROVAL = "awaiting_approval"
DONE = "done"


@dataclass
class GeocodeResult:
    found: bool
    lat: Optional[str] = None
    lon: Optional[str] = None
    display_name: Optional[str] = None


@dataclass
class DocumentCheckInput:
    address: str  # what the customer typed
    display_name: str  # canonical form returned by Nominatim
    file_path: str  # absolute path under uploads/<workflow_id>/


@dataclass
class DocumentVerdict:
    match: bool
    confidence: str  # "high" | "medium" | "low"
    matched_text: str
    reason: str


@dataclass
class ReviewDecision:
    approved: bool
    reviewer: str
    notes: str = ""


@dataclass
class WorkflowStatus:
    step: str
    last_error: Optional[str] = None
    address: Optional[str] = None
    geocode: Optional[GeocodeResult] = None
    document_path: Optional[str] = None
    verdict: Optional[DocumentVerdict] = None


@dataclass
class AddressVerificationResult:
    status: str  # "approved" | "rejected" | "expired"
    reason: str
    address: Optional[str] = None
    geocode: Optional[GeocodeResult] = None
    verdict: Optional[DocumentVerdict] = None
    reviewer: Optional[str] = None
    notes: str = ""
    extra: dict = field(default_factory=dict)

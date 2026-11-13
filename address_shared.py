import re
from dataclasses import dataclass, field
from typing import Optional

ADDRESS_TASK_QUEUE = "claude-agent-sdk"  # same queue/worker as the first workflow

ADDRESS_WORKFLOW_NAME = "AddressVerificationWorkflow"  # must equal the workflow class name

# The company marker ends up in the workflow id, a Jira label and an uploads/ directory name,
# so it is restricted to characters that are safe in all three.
_COMPANY_MARKER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.\-]*$")


def validate_verification_input(company_marker: str, customer_id: int) -> None:
    """Raises ValueError if the input can't identify a verification."""
    if not isinstance(company_marker, str) or not _COMPANY_MARKER_RE.match(company_marker):
        raise ValueError(
            "company_marker must be a string of letters, digits, '_', '.' or '-' "
            f"(got {company_marker!r})"
        )
    if isinstance(customer_id, bool) or not isinstance(customer_id, int) or customer_id < 0:
        raise ValueError(f"customer_id must be a non-negative integer (got {customer_id!r})")


def address_workflow_id(company_marker: str, customer_id: int) -> str:
    """Workflow id convention: <workflow name>-<companyMarker>-<customerId>.

    Deterministic, so there is at most one running verification per customer."""
    validate_verification_input(company_marker, customer_id)
    return f"{ADDRESS_WORKFLOW_NAME}-{company_marker}-{customer_id}"


@dataclass
class AddressVerificationInput:
    company_marker: str
    customer_id: int


# Workflow steps (returned by the get_status query)
AWAITING_ADDRESS = "awaiting_address"
VALIDATING_ADDRESS = "validating_address"
AWAITING_DOCUMENT = "awaiting_document"
VERIFYING_DOCUMENT = "verifying_document"
CREATING_TICKET = "creating_ticket"
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
    match: bool  # True only if it is a utility bill AND shows this address
    confidence: str  # "high" | "medium" | "low"
    matched_text: str
    reason: str
    is_utility_bill: bool = True
    document_type: str = ""  # what the document is, e.g. "electricity bill", "bank statement"


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
    company_marker: Optional[str] = None
    customer_id: Optional[int] = None


@dataclass
class ApprovalRequest:
    """What a human reviewer needs to decide (used to build the Jira ticket)."""

    workflow_id: str
    address: str  # what the customer typed
    display_name: str  # canonical form returned by Nominatim
    confidence: str
    matched_text: str
    reason: str
    company_marker: Optional[str] = None
    customer_id: Optional[int] = None
    document_type: Optional[str] = None


@dataclass
class ApprovalTicket:
    key: str
    url: str

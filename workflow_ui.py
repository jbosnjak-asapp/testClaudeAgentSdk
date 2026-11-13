"""Standard "UI contract" so one UI can visualize any workflow.

A workflow that wants a friendly view:
  1. creates a `UiTracker` with its ordered steps,
  2. updates it at each transition (`begin` / `done` / `set_error` / `finish`),
  3. exposes it through a query named `ui_state` (see UI_STATE_QUERY).

The API returns the snapshot as plain JSON; the UI renders a stepper from
`steps`, highlights `current_step`, shows `pending_input` as "waiting for ..."
and lists `data` as key/values. Only ever ADD fields to these dataclasses (never
rename/remove) so older running workflows keep working.
"""
from dataclasses import dataclass, field
from typing import Any, Optional

UI_STATE_QUERY = "ui_state"

PENDING, ACTIVE, DONE, FAILED, SKIPPED = "pending", "active", "done", "failed", "skipped"


@dataclass
class UiStep:
    id: str
    label: str
    state: str = PENDING


@dataclass
class UiInput:
    """What the workflow is currently waiting for (informational for the UI)."""

    signal: str
    label: str
    schema: dict = field(default_factory=dict)  # JSON schema of the payload


@dataclass
class UiState:
    workflow_type: str
    steps: list[UiStep]
    current_step: Optional[str] = None
    error: Optional[str] = None
    pending_input: Optional[UiInput] = None
    data: dict[str, Any] = field(default_factory=dict)  # JSON-serializable values only


class UiTracker:
    """Mutable holder for a workflow's UiState. Deterministic; safe in workflows."""

    def __init__(self, workflow_type: str, steps: list[tuple[str, str]]) -> None:
        self._state = UiState(
            workflow_type=workflow_type,
            steps=[UiStep(id=i, label=label) for i, label in steps],
        )

    def _step(self, step_id: str) -> UiStep:
        for s in self._state.steps:
            if s.id == step_id:
                return s
        raise KeyError(f"unknown step {step_id!r}")

    def begin(self, step_id: str, pending_input: Optional[UiInput] = None) -> None:
        self._step(step_id).state = ACTIVE
        self._state.current_step = step_id
        self._state.pending_input = pending_input

    def waiting_for(self, pending_input: Optional[UiInput]) -> None:
        self._state.pending_input = pending_input

    def set_error(self, error: Optional[str]) -> None:
        self._state.error = error

    def done(self, step_id: str) -> None:
        self._step(step_id).state = DONE
        self._state.pending_input = None
        self._state.error = None

    def set_data(self, key: str, value: Any) -> None:
        self._state.data[key] = value

    def finish(self, success: bool, reason: str) -> None:
        """Close the workflow: on failure, mark the active step failed."""
        self._state.pending_input = None
        if success:
            self._state.current_step = None
            self._state.error = None
            return
        if self._state.current_step:
            self._step(self._state.current_step).state = FAILED
        self._state.error = reason

    def snapshot(self) -> UiState:
        return self._state

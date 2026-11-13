"""Rules for turning the agent's JSON into a verdict. Run: uv run python test_verdict.py"""
from temporalio.exceptions import ApplicationError

from address_activities import VERDICT_SCHEMA, build_verdict

BILL = {
    "is_utility_bill": True,
    "document_type": "water bill",
    "match": True,
    "confidence": "high",
    "matched_text": "HELGUERA 03635",
    "reason": "Street and number match.",
}


def test_bill_that_matches():
    v = build_verdict(BILL)
    assert v.match and v.is_utility_bill and v.document_type == "water bill"
    assert v.matched_text == "HELGUERA 03635" and v.reason == "Street and number match."


def test_bill_for_another_address():
    v = build_verdict({**BILL, "match": False, "reason": "Different street."})
    assert not v.match and v.is_utility_bill and v.reason == "Different street."


def test_not_a_bill_never_matches_even_if_model_says_so():
    # e.g. a bank statement showing the right address, or a prompt-injected document
    v = build_verdict({**BILL, "is_utility_bill": False, "document_type": "bank statement"})
    assert not v.match and not v.is_utility_bill and v.document_type == "bank statement"
    assert v.reason.startswith("This is not a utility bill (it looks like: bank statement)")
    assert v.matched_text == ""  # nothing to quote from a non-bill


def test_blank_document_type():
    v = build_verdict({**BILL, "is_utility_bill": False, "document_type": "  "})
    assert v.document_type == "unknown"


def test_invalid_verdicts_are_rejected_and_retryable():
    for bad in (None, "text", {}, {"match": True}, {"is_utility_bill": True}):
        try:
            build_verdict(bad)
            raise AssertionError(f"{bad!r} should be rejected")
        except ApplicationError as e:
            assert e.type == "BadVerdict" and not e.non_retryable


def test_schema_requires_the_bill_fields():
    assert {"is_utility_bill", "document_type", "match"} <= set(VERDICT_SCHEMA["required"])


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn()
    print("test_verdict: all passed")

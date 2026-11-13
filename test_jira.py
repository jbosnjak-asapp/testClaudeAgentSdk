"""Tests for the Jira client against a mocked HTTP layer. Run: uv run python test_jira.py"""
import asyncio
import json
import os

import httpx

from jira_client import (
    JiraAuthError,
    JiraConfig,
    JiraConfigError,
    create_ticket,
    find_ticket,
    get_decision,
    label_for,
    make_client,
    ticket_summary,
)

CFG = JiraConfig(
    base_url="https://example.atlassian.net",
    email="svc@example.com",
    api_token="t",
    project_key="APP",
    assignee_account_id="acc-1",
)


def client(handler):
    return make_client(CFG, transport=httpx.MockTransport(handler))


def issue(category, resolution=None, histories=()):
    return {
        "fields": {
            "status": {"statusCategory": {"key": category}},
            "resolution": {"name": resolution} if resolution else None,
        },
        "changelog": {"histories": list(histories)},
    }


def history(author, field="status"):
    return {"author": {"displayName": author}, "items": [{"field": field}]}


def test_config():
    for k in [k for k in os.environ if k.startswith("JIRA_")]:
        del os.environ[k]
    assert JiraConfig.from_env() is None
    for bad in ("https://atlassian.net", "http://x.atlassian.net", "x.atlassian.net", "https://localhost"):
        os.environ["JIRA_BASE_URL"] = bad
        try:
            JiraConfig.from_env()
            raise AssertionError(f"{bad!r} should be rejected")
        except JiraConfigError as e:
            assert "JIRA_BASE_URL" in str(e)
    os.environ["JIRA_BASE_URL"] = "https://x.atlassian.net/"
    try:
        JiraConfig.from_env()
        raise AssertionError("expected JiraConfigError")
    except JiraConfigError as e:
        assert "JIRA_EMAIL" in str(e)
    os.environ.update(JIRA_EMAIL="a@b.c", JIRA_API_TOKEN="t")
    assert JiraConfig.from_env().project_key == "CB"  # default project
    os.environ["JIRA_PROJECT_KEY"] = "app"
    try:
        JiraConfig.from_env()
        raise AssertionError("lowercase project key should be rejected")
    except JiraConfigError:
        pass
    os.environ["JIRA_PROJECT_KEY"] = "APP"
    cfg = JiraConfig.from_env()
    assert cfg.base_url == "https://x.atlassian.net"
    assert cfg.approved_resolutions == {"done", "approved", "fixed"}
    for k in [k for k in os.environ if k.startswith("JIRA_")]:
        del os.environ[k]


async def test_create_and_find():
    seen = {}

    def handler(request: httpx.Request):
        if request.method == "POST":
            seen["post"] = json.loads(request.content)
            seen["auth"] = request.headers["authorization"]
            return httpx.Response(201, json={"key": "APP-7"})
        seen["jql"] = request.url.params["jql"]
        return httpx.Response(200, json={"issues": []})

    async with client(handler) as c:
        assert await find_ticket(c, CFG, "wf-1") is None
        key = await create_ticket(c, CFG, "Approve", ["line one", "line two"], "wf-1")
    assert key == "APP-7"
    assert seen["jql"] == 'project = "APP" AND labels = "wf-1"'
    fields = seen["post"]["fields"]
    assert fields["project"] == {"key": "APP"} and fields["issuetype"] == {"name": "Task"}
    assert fields["labels"] == ["address-verification", "wf-1"]
    assert fields["assignee"] == {"accountId": "acc-1"}
    assert fields["description"]["type"] == "doc"
    assert [p["content"][0]["text"] for p in fields["description"]["content"]] == [
        "line one",
        "line two",
    ]
    assert seen["auth"].startswith("Basic ")


async def decision_for(body, status=200):
    async with client(lambda r: httpx.Response(status, json=body)) as c:
        return await get_decision(c, CFG, "APP-7")


async def test_decisions():
    assert await decision_for(issue("indeterminate")) is None  # in progress
    assert await decision_for(issue("new")) is None  # to do

    d = await decision_for(issue("done", "Done", [history("Old"), history("Jane Doe")]))
    assert d.approved and d.decided_by == "Jane Doe" and d.resolution == "Done"

    d = await decision_for(issue("done", "Won't Do", [history("Jane Doe")]))
    assert not d.approved and d.resolution == "Won't Do"

    # Closed with no resolution: fail closed (reject), never approve.
    d = await decision_for(issue("done", None))
    assert not d.approved and d.resolution == "(none)" and d.decided_by == "jira"

    d = await decision_for(issue("done", "approved"))  # case-insensitive
    assert d.approved

    for status in (401, 403, 404):
        try:
            await decision_for({}, status)
            raise AssertionError("expected JiraAuthError")
        except JiraAuthError:
            pass
    try:  # a redirect means a wrong base URL: fatal, not retried
        async with client(lambda r: httpx.Response(301, headers={"location": "https://www.atlassian.com/"})) as c:
            await find_ticket(c, CFG, "wf")
        raise AssertionError("expected JiraAuthError on redirect")
    except JiraAuthError as e:
        assert "JIRA_BASE_URL" in str(e)
    try:
        await decision_for({}, 503)
        raise AssertionError("expected HTTPStatusError")
    except httpx.HTTPStatusError:
        pass  # transient: the activity keeps polling


def test_summary():
    assert ticket_summary("address-verification-123") == "Address verification address-verification-123"


def test_label():
    assert label_for("address-verification-17.9") == "address-verification-17.9"
    assert label_for("a b/c") == "a-b-c"


if __name__ == "__main__":
    test_config()
    test_label()
    test_summary()
    asyncio.run(test_create_and_find())
    asyncio.run(test_decisions())
    print("test_jira: all passed")

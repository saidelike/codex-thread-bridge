import sys

import pytest
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client


async def test_archive_and_source_filters_discover_headers_without_reading_bodies(
    fake_server, tmp_path
):
    fake, socket = fake_server
    fake.threads = {
        "active": {"id": "active", "source": "appServer", "turns": []},
        "archived": {"id": "archived", "archived": True, "source": "appServer", "turns": []},
        "archived-cli": {"id": "archived-cli", "archived": True, "source": "cli", "turns": []},
    }
    params = StdioServerParameters(
        command=sys.executable,
        args=[
            "-m",
            "codex_thread_bridge.server",
            "--socket",
            str(socket),
            "--state-dir",
            str(tmp_path / "state"),
        ],
    )
    async with stdio_client(params) as (read, write), ClientSession(read, write) as session:
        await session.initialize()
        result = await session.call_tool(
            "list_threads", {"archived": True, "source_kinds": ["appServer"]}
        )
        assert not result.isError, result.content
        assert [thread["id"] for thread in result.structuredContent["data"]] == ["archived"]
        assert result.structuredContent["discovery"]["stateDbOnly"] is True
    assert [method for method, _ in fake.calls] == ["initialize", "initialized", "thread/list"]
    assert fake.calls[-1][1]["archived"] is True
    assert fake.calls[-1][1]["sourceKinds"] == ["appServer"]
    assert fake.calls[-1][1]["useStateDbOnly"] is True


async def test_direct_and_recursive_descendants_exclude_similar_unrelated_threads(
    fake_server, tmp_path
):
    fake, socket = fake_server
    source = {
        "subAgent": {
            "thread_spawn": {"parent_thread_id": "root", "depth": 1, "agent_path": "/root/child"}
        }
    }
    fake.threads = {
        "root": {"id": "root", "source": "appServer", "turns": []},
        "child": {"id": "child", "parentThreadId": "root", "source": source, "turns": []},
        "grandchild": {
            "id": "grandchild",
            "parentThreadId": "child",
            "source": {"subAgent": {"other": "legacy-child"}},
            "turns": [],
        },
        "unrelated": {
            "id": "unrelated",
            "name": "child",
            "createdAt": 1,
            "cwd": str(tmp_path),
            "source": {
                "subAgent": {
                    "thread_spawn": {
                        "parent_thread_id": "unrelated-root",
                        "depth": 2,
                        "agent_path": "/root/child/other",
                    }
                }
            },
            "turns": [],
        },
    }
    params = StdioServerParameters(
        command=sys.executable,
        args=[
            "-m",
            "codex_thread_bridge.server",
            "--socket",
            str(socket),
            "--state-dir",
            str(tmp_path / "state"),
        ],
    )
    async with stdio_client(params) as (read, write), ClientSession(read, write) as session:
        await session.initialize()
        source_kinds = ["subAgentThreadSpawn", "subAgentOther"]
        direct = await session.call_tool(
            "list_threads", {"parent_thread_id": "root", "source_kinds": source_kinds}
        )
        assert not direct.isError, direct.content
        assert [t["id"] for t in direct.structuredContent["data"]] == ["child"]
        first = await session.call_tool(
            "list_threads", {"ancestor_thread_id": "root", "limit": 1, "source_kinds": source_kinds}
        )
        assert not first.isError, first.content
        page = first.structuredContent
        assert page["data"][0]["source"] == source
        assert page["discovery"]["relationships"][0] == {
            "threadId": "child",
            "parentEvidence": [
                {"field": "parentThreadId", "threadId": "root"},
                {"field": "source.subAgent.thread_spawn.parent_thread_id", "threadId": "root"},
            ],
            "ancestryStatus": "supported",
            "forkedFromId": None,
            "sessionId": None,
        }
        cursor = page["nextCursor"]
        second = await session.call_tool(
            "list_threads",
            {
                "ancestor_thread_id": "root",
                "limit": 1,
                "cursor": cursor,
                "source_kinds": source_kinds,
            },
        )
        assert [t["id"] for t in second.structuredContent["data"]] == ["grandchild"]
        assert second.structuredContent["nextCursor"] is None
        assert second.structuredContent["discovery"]["relationships"][0]["parentEvidence"] == [
            {"field": "parentThreadId", "threadId": "child"}
        ]
    assert all(method in {"initialize", "initialized", "thread/list"} for method, _ in fake.calls)
    assert [p["cursor"] for m, p in fake.calls if m == "thread/list" and "cursor" in p] == [cursor]


@pytest.mark.parametrize(
    "native_page",
    [
        {"data": [{"id": "candidate", "source": "unknown"}]},
        {"data": ["future-header-format"], "nextCursor": None},
        {"data": [{"source": "unknown"}], "nextCursor": None},
        {"data": [], "nextCursor": {"future": "cursor-object"}},
    ],
)
async def test_unknown_discovery_schema_cannot_claim_completed_pagination(
    fake_server, tmp_path, native_page
):
    fake, socket = fake_server
    fake.list_response = native_page
    params = StdioServerParameters(
        command=sys.executable,
        args=[
            "-m",
            "codex_thread_bridge.server",
            "--socket",
            str(socket),
            "--state-dir",
            str(tmp_path / "state"),
        ],
    )
    async with stdio_client(params) as (read, write), ClientSession(read, write) as session:
        await session.initialize()
        result = await session.call_tool("list_threads", {})
        assert not result.isError, result.content
        page = result.structuredContent
        assert page["data"] == native_page["data"]
        assert page["discovery"]["status"] == "unsupported_schema"
        assert page["discovery"]["pageComplete"] is False
        assert page["discovery"]["gap"]
    assert fake.count("thread/list") == 1


@pytest.mark.parametrize(
    "native_error, expected_status",
    [
        ({"code": -32601, "message": "thread/list unavailable"}, "unsupported"),
        ({"code": -32602, "message": "ancestorThreadId is unsupported"}, "unsupported"),
        ({"code": -32602, "message": "invalid cursor"}, "rejected"),
        ({"code": -32000, "message": "state database unavailable"}, "unavailable"),
    ],
)
async def test_unsupported_discovery_is_an_explicit_gap_without_unfiltered_retry(
    fake_server,
    tmp_path,
    native_error,
    expected_status,
):
    fake, socket = fake_server
    fake.reject["thread/list"] = native_error
    params = StdioServerParameters(
        command=sys.executable,
        args=[
            "-m",
            "codex_thread_bridge.server",
            "--socket",
            str(socket),
            "--state-dir",
            str(tmp_path / "state"),
        ],
    )
    async with stdio_client(params) as (read, write), ClientSession(read, write) as session:
        await session.initialize()
        result = await session.call_tool(
            "list_threads", {"ancestor_thread_id": "root", "archived": True}
        )
        assert not result.isError, result.content
        assert result.structuredContent["data"] is None
        gap = result.structuredContent["discovery"]
        assert gap["status"] == expected_status
        assert gap["pageComplete"] is False
        assert gap["rpcError"] == native_error
        caps = await session.call_tool("get_capabilities", {})
        discovery = caps.structuredContent["capabilities"]["threadDiscovery"]
        assert discovery["filters"] == [
            "archived",
            "sourceKinds",
            "parentThreadId",
            "ancestorThreadId",
        ]
        assert discovery["continuationMapping"] == "unsupported"
    assert fake.count("thread/list") == 1
    assert all(method in {"initialize", "initialized", "thread/list"} for method, _ in fake.calls)


@pytest.mark.parametrize(
    "filters",
    [
        {"parent_thread_id": "root", "ancestor_thread_id": "root"},
        {"parent_thread_id": ""},
        {"source_kinds": ["not-a-native-source-kind"]},
    ],
)
async def test_invalid_discovery_filters_are_rejected_before_request(
    fake_server, tmp_path, filters
):
    fake, socket = fake_server
    params = StdioServerParameters(
        command=sys.executable,
        args=[
            "-m",
            "codex_thread_bridge.server",
            "--socket",
            str(socket),
            "--state-dir",
            str(tmp_path / "state"),
        ],
    )
    async with stdio_client(params) as (read, write), ClientSession(read, write) as session:
        await session.initialize()
        result = await session.call_tool("list_threads", filters)
        assert result.isError
    assert fake.count("thread/list") == 0


async def test_relationship_evidence_preserves_conflicts_cycles_and_unknown_continuity(
    fake_server, tmp_path
):
    fake, socket = fake_server
    raw_source = {"subAgent": {"thread_spawn": {"parent_thread_id": "different-root", "depth": 1}}}
    fake.threads = {
        "conflict": {"id": "conflict", "parentThreadId": "root", "source": raw_source, "turns": []},
        "cycle-a": {"id": "cycle-a", "parentThreadId": "cycle-b", "turns": []},
        "cycle-b": {"id": "cycle-b", "parentThreadId": "cycle-a", "turns": []},
        "self-cycle": {"id": "self-cycle", "parentThreadId": "self-cycle", "turns": []},
        "fork": {"id": "fork", "forkedFromId": "root", "sessionId": "shared-session", "turns": []},
        "unknown": {"id": "unknown", "source": {"future": {"parent": "root"}}, "turns": []},
    }
    params = StdioServerParameters(
        command=sys.executable,
        args=[
            "-m",
            "codex_thread_bridge.server",
            "--socket",
            str(socket),
            "--state-dir",
            str(tmp_path / "state"),
        ],
    )
    async with stdio_client(params) as (read, write), ClientSession(read, write) as session:
        await session.initialize()
        result = await session.call_tool("list_threads", {})
        assert not result.isError, result.content
        page = result.structuredContent
        evidence = {row["threadId"]: row for row in page["discovery"]["relationships"]}
        assert evidence["conflict"]["ancestryStatus"] == "conflicting"
        assert [row["threadId"] for row in evidence["conflict"]["parentEvidence"]] == [
            "root",
            "different-root",
        ]
        assert evidence["cycle-a"]["parentEvidence"][0]["threadId"] == "cycle-b"
        assert evidence["cycle-b"]["parentEvidence"][0]["threadId"] == "cycle-a"
        assert evidence["self-cycle"]["ancestryStatus"] == "cyclic"
        assert evidence["unknown"]["ancestryStatus"] == "unknown"
        assert evidence["fork"]["ancestryStatus"] == "unknown"
        assert evidence["fork"]["forkedFromId"] == "root"
        assert evidence["fork"]["sessionId"] == "shared-session"
        assert page["data"][0]["source"] == raw_source
        assert page["discovery"]["continuationMapping"] == "unsupported"
        assert page["discovery"]["coverageLimits"] == [
            "state_database_only",
            "archive_and_source_partition",
            "per_record_ancestry",
            "continuation_mapping_unsupported",
        ]
    assert all(method in {"initialize", "initialized", "thread/list"} for method, _ in fake.calls)

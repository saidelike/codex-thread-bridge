"""Read-only persisted-record access through the MCP boundary."""

import json
import sys
from contextlib import asynccontextmanager

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client


@asynccontextmanager
async def record_session(socket, tmp_path):
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
        yield session


def seed_history(fake):
    secret = 'SYNTHETIC_TOKEN="fixture-secret-123"'
    tail = "output-prefix " * 800 + " /tmp/unique-tail-artifact " + secret
    fake.threads["thread-record"] = {
        "id": "thread-record",
        "cwd": "/tmp/synthetic-cwd",
        "preview": secret,
        "futureMetadata": {"opaque": "preserve-me"},
        "status": {"type": "idle"},
        "turns": [
            {"id": "turn-early", "status": "completed", "itemsView": "full", "items": []},
            {
                "id": "turn-late",
                "status": "completed",
                "itemsView": "full",
                "futureTurn": {"flag": True},
                "items": [
                    {
                        "id": "item-command",
                        "type": "commandExecution",
                        "aggregatedOutput": tail,
                        "nested": {"outputs": [{"arbitraryField": secret}]},
                    }
                ],
            },
        ],
    }
    return secret, tail


async def test_full_reads_preserve_native_records_and_opaque_cursor(fake_server, tmp_path):
    fake, socket = fake_server
    secret, tail = seed_history(fake)
    fake.cursor = lambda offset: json.dumps({"offset": offset, "token": {"scope": "turns"}})
    fake.offset = lambda cursor: 0 if cursor is None else json.loads(cursor)["offset"]
    async with record_session(socket, tmp_path) as session:
        tools = (await session.list_tools()).tools
        for tool in tools:
            if tool.name in {
                "read_thread_metadata",
                "read_thread_turns",
                "read_thread_items",
                "read_thread_chunk",
                "reconstruct_thread_record",
            }:
                assert tool.annotations.readOnlyHint is True
                assert tool.annotations.destructiveHint is False
        capabilities = await session.call_tool("get_capabilities", {})
        assert capabilities.structuredContent["capabilities"]["faithfulPersistedReads"][
            "nativeMethods"
        ] == ["thread/read", "thread/turns/list", "thread/items/list"]
        metadata = await session.call_tool("read_thread_metadata", {"thread_id": "thread-record"})
        assert not metadata.isError, metadata.content
        assert metadata.structuredContent["record"]["thread"]["preview"] == secret
        assert metadata.structuredContent["record"]["thread"]["futureMetadata"] == {
            "opaque": "preserve-me"
        }
        args = {"thread_id": "thread-record", "limit": 1}
        first = await session.call_tool("read_thread_turns", args)
        assert not first.isError, first.content
        result = first.structuredContent
        assert result["record"]["data"][0]["items"][0]["aggregatedOutput"] == tail
        assert result["coverage"]["transportComplete"] is True
        assert result["coverage"]["hasMore"] is True
        cursor = result["record"]["nextCursor"]
        second = await session.call_tool("read_thread_turns", {**args, "cursor": cursor})
        assert not second.isError, second.content
        assert second.structuredContent["record"]["data"][0]["id"] == "turn-early"
        assert second.structuredContent["coverage"]["startsAtBeginning"] is False
        assert second.structuredContent["coverage"]["hasMore"] is False
    assert (
        "thread/turns/list",
        {
            "threadId": "thread-record",
            "limit": 1,
            "itemsView": "full",
            "cursor": cursor,
        },
    ) in fake.calls
    assert all(
        method in {"initialize", "initialized", "thread/read", "thread/turns/list"}
        for method, _ in fake.calls
    )


async def test_item_pages_preserve_nested_outputs_and_turn_filter(fake_server, tmp_path):
    fake, socket = fake_server
    secret, tail = seed_history(fake)
    fake.threads["thread-record"]["turns"][0]["items"] = [
        {"id": "item-old", "type": "agentMessage", "text": "old"}
    ]
    async with record_session(socket, tmp_path) as session:
        result = await session.call_tool(
            "read_thread_items",
            {
                "thread_id": "thread-record",
                "turn_id": "turn-late",
                "limit": 1,
            },
        )
        assert not result.isError, result.content
        envelope = result.structuredContent
        assert envelope["record"]["data"] == [
            {
                "turnId": "turn-late",
                "item": {
                    "id": "item-command",
                    "type": "commandExecution",
                    "aggregatedOutput": tail,
                    "nested": {"outputs": [{"arbitraryField": secret}]},
                },
            }
        ]
        assert envelope["coverage"]["scope"] == "item-page"
    assert (
        "thread/items/list",
        {
            "threadId": "thread-record",
            "turnId": "turn-late",
            "limit": 1,
        },
    ) in fake.calls
    assert all(
        method in {"initialize", "initialized", "thread/items/list"} for method, _ in fake.calls
    )


async def test_chunks_reconstruct_long_raw_record_and_reject_missing_parts(fake_server, tmp_path):
    fake, socket = fake_server
    secret, tail = seed_history(fake)
    async with record_session(socket, tmp_path) as session:
        args = {
            "source_kind": "turns",
            "thread_id": "thread-record",
            "limit": 1,
            "max_chars": 1800,
        }
        chunks = []
        result = await session.call_tool("read_thread_chunk", args)
        assert not result.isError, result.content
        chunks.append(result.structuredContent)
        while chunks[-1]["nextOffset"] is not None:
            result = await session.call_tool(
                "read_thread_chunk",
                {
                    **args,
                    "offset": chunks[-1]["nextOffset"],
                    "expected_version": chunks[0]["sourceVersion"],
                },
            )
            assert not result.isError, result.content
            chunks.append(result.structuredContent)
        assert chunks[0]["start"] == 0
        assert chunks[-1]["end"] == chunks[0]["totalLength"]
        assert chunks[0]["coverage"]["transportComplete"] is False
        recovered = await session.call_tool("reconstruct_thread_record", {"chunks": chunks})
        assert not recovered.isError, recovered.content
        record = recovered.structuredContent["record"]
        item = record["data"][0]["items"][0]
        assert item["aggregatedOutput"] == tail
        assert item["nested"]["outputs"][0]["arbitraryField"] == secret
        assert recovered.structuredContent["coverage"]["transportComplete"] is True
        missing = await session.call_tool("reconstruct_thread_record", {"chunks": chunks[1:]})
        assert missing.isError
        assert "missing" in missing.content[0].text.lower()
        mutated = [{**chunk} for chunk in chunks]
        mutated[-1]["text"] = "!" + mutated[-1]["text"][1:]
        tampered = await session.call_tool("reconstruct_thread_record", {"chunks": mutated})
        assert tampered.isError
    assert all(
        method in {"initialize", "initialized", "thread/turns/list"} for method, _ in fake.calls
    )


async def test_chunk_continuation_refuses_changed_source_and_unversioned_reads(
    fake_server, tmp_path
):
    fake, socket = fake_server
    seed_history(fake)
    async with record_session(socket, tmp_path) as session:
        args = {"source_kind": "items", "thread_id": "thread-record", "max_chars": 300}
        first = await session.call_tool("read_thread_chunk", args)
        assert not first.isError, first.content
        chunk = first.structuredContent
        unbound = await session.call_tool("read_thread_chunk", {**args, "offset": chunk["end"]})
        assert unbound.isError
        assert "expected_version" in unbound.content[0].text
        fake.threads["thread-record"]["turns"][-1]["items"][0]["aggregatedOutput"] += " CHANGED"
        changed = await session.call_tool(
            "read_thread_chunk",
            {
                **args,
                "offset": chunk["end"],
                "expected_version": chunk["sourceVersion"],
            },
        )
        assert changed.isError
        assert "source changed" in changed.content[0].text.lower()


async def test_compact_read_discloses_exact_clipped_fields_and_page_scope(fake_server, tmp_path):
    fake, socket = fake_server
    seed_history(fake)
    async with record_session(socket, tmp_path) as session:
        result = await session.call_tool(
            "read_thread",
            {
                "thread_id": "thread-record",
                "limit": 1,
                "max_text_chars": 100,
            },
        )
        assert not result.isError, result.content
        compact = result.structuredContent
        assert "truncated" in compact["turnsPage"]["data"][0]["items"][0]["aggregatedOutput"]
        assert compact["coverage"]["transportComplete"] is False
        assert compact["coverage"]["hasMore"] is True
        assert compact["coverage"]["clippedFields"] == [
            {
                "path": "/turnsPage/data/0/items/0/aggregatedOutput",
                "originalLength": 11263,
                "retainedPrefixLength": 100,
            }
        ]
        assert compact["coverage"]["rawRecovery"]["turnsPage"]["tool"] == "read_thread_turns"


async def test_upstream_gaps_remain_explicit_after_faithful_retrieval(fake_server, tmp_path):
    fake, socket = fake_server
    seed_history(fake)
    fake.threads["thread-record"]["turns"][-1]["itemsView"] = "summary"
    fake.offset = lambda cursor: 0
    fake.response_overrides = {"thread/turns/list": {"nextCursor": "repeat-cursor"}}
    async with record_session(socket, tmp_path) as session:
        result = await session.call_tool(
            "read_thread_turns",
            {
                "thread_id": "thread-record",
                "cursor": "repeat-cursor",
            },
        )
        assert not result.isError, result.content
        coverage = result.structuredContent["coverage"]
        assert coverage["transportComplete"] is True
        assert any("repeats" in gap for gap in coverage["gaps"])
        assert any("itemsView is not full" in gap for gap in coverage["gaps"])
        assert any("discarded history" in limitation for limitation in coverage["limitations"])
        fake.reject["thread/items/list"] = {"code": -32601, "message": "method unsupported"}
        unsupported = await session.call_tool("read_thread_items", {"thread_id": "thread-record"})
        assert unsupported.isError
        assert "method unsupported" in unsupported.content[0].text
    assert all(
        method in {"initialize", "initialized", "thread/turns/list", "thread/items/list"}
        for method, _ in fake.calls
    )


async def test_metadata_identity_mismatch_stays_visible_in_compact_reads(fake_server, tmp_path):
    fake, socket = fake_server
    seed_history(fake)
    fake.response_overrides["thread/read"] = {"thread": {"id": "different-thread"}}
    async with record_session(socket, tmp_path) as session:
        result = await session.call_tool("read_thread_metadata", {"thread_id": "thread-record"})
        assert not result.isError, result.content
        assert any("identity" in gap for gap in result.structuredContent["coverage"]["gaps"])
        compact = await session.call_tool("read_thread", {"thread_id": "thread-record"})
        assert not compact.isError, compact.content
        assert any("identity" in gap for gap in compact.structuredContent["coverage"]["gaps"])

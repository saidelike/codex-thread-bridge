import json
import sys

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client


async def test_mcp_forwards_json_shaped_pagination_cursors(fake_server, tmp_path):
    fake, socket = fake_server
    fake.cursor = lambda offset: json.dumps({"offset": offset, "scope": {"kind": "turns"}})
    fake.offset = lambda cursor: 0 if cursor is None else json.loads(cursor)["offset"]
    fake.threads = {
        f"thread-{i}": {
            "id": f"thread-{i}",
            "cwd": str(tmp_path),
            "status": {"type": "idle"},
            "turns": [{"id": f"turn-{j}", "status": "completed", "items": []} for j in range(2)],
        }
        for i in range(2)
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
        for tool, args, page_key, expected_id in (
            ("read_thread", {"thread_id": "thread-0", "limit": 1}, "turnsPage", "turn-0"),
            ("list_threads", {"limit": 1}, None, "thread-1"),
        ):
            first = await session.call_tool(tool, args)
            assert not first.isError
            page = first.structuredContent[page_key] if page_key else first.structuredContent
            cursor = page["nextCursor"]
            second = await session.call_tool(tool, {**args, "cursor": cursor})
            assert not second.isError, second.content
            page = second.structuredContent[page_key] if page_key else second.structuredContent
            assert [item["id"] for item in page["data"]] == [expected_id]
            method = "thread/turns/list" if page_key else "thread/list"
            assert [p["cursor"] for m, p in fake.calls if m == method and "cursor" in p] == [cursor]
    assert not any(
        method in {"thread/start", "thread/resume", "turn/start"} for method, _ in fake.calls
    )


async def test_real_mcp_stdio_discovery_create_read_and_dedup(fake_server, tmp_path):
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
        tools = (await session.list_tools()).tools
        assert {tool.name for tool in tools} == {
            "get_capabilities",
            "create_thread",
            "create_worktree_thread",
            "send_message_to_thread",
            "steer_thread",
            "read_thread",
            "read_thread_metadata",
            "read_thread_turns",
            "read_thread_items",
            "read_thread_chunk",
            "reconstruct_thread_record",
            "list_threads",
            "wait_thread",
            "get_goal",
            "get_operation",
            "update_thread_permissions",
        }
        caps = await session.call_tool("get_capabilities", {})
        assert not caps.isError
        assert caps.structuredContent["capabilities"]["desktopManagedWorktrees"] is False
        assert caps.structuredContent["capabilities"]["steerActiveTurn"] is True
        create_schema = next(tool for tool in tools if tool.name == "create_thread").inputSchema
        assert "reasoning_effort" in create_schema["properties"]
        args = {
            "request_id": "mcp-create",
            "cwd": str(tmp_path),
            "prompt": "READY",
            "model": "gpt-5.6-sol",
            "reasoning_effort": "high",
        }
        result = await session.call_tool("create_thread", args)
        assert not result.isError
        receipt = result.structuredContent
        assert receipt["status"] == "accepted"
        assert receipt["creation"]["reasoningEffort"] == "high"
        repeated = await session.call_tool("create_thread", args)
        assert repeated.structuredContent["replayed"]
        followup = await session.call_tool(
            "send_message_to_thread",
            {
                "request_id": "mcp-send",
                "thread_id": receipt["threadId"],
                "message": "FOLLOWUP",
            },
        )
        sent = followup.structuredContent
        assert sent["status"] == "accepted"
        fake.threads[receipt["threadId"]]["status"] = {"type": "active"}
        fake.threads[receipt["threadId"]]["turns"][-1]["status"] = "inProgress"
        steer = await session.call_tool(
            "steer_thread",
            {
                "request_id": "mcp-steer",
                "thread_id": receipt["threadId"],
                "expected_turn_id": sent["turnId"],
                "message": "STEER",
            },
        )
        assert steer.structuredContent["status"] == "accepted"
        assert steer.structuredContent["turnId"] == sent["turnId"]
        waited = await session.call_tool(
            "wait_thread",
            {
                "thread_id": receipt["threadId"],
                "turn_id": sent["turnId"],
                "timeout_seconds": 0,
            },
        )
        assert waited.structuredContent["turn"]["items"][0]["text"] == "FOLLOWUP"
        history = await session.call_tool("read_thread", {"thread_id": receipt["threadId"]})
        assert len(history.structuredContent["turnsPage"]["data"]) == 2
        goal = await session.call_tool("get_goal", {"thread_id": receipt["threadId"]})
        assert goal.structuredContent["goal"] is None
        invalid = await session.call_tool("create_thread", {**args, "sandbox": "invalid"})
        assert invalid.isError
    assert fake.count("thread/start") == 1 and fake.count("turn/start") == 2
    assert fake.count("turn/steer") == 1


async def test_mcp_socket_alias_restart_does_not_repeat_creation(fake_server, tmp_path):
    fake, socket = fake_server
    alias = socket.parent / "alias.sock"
    alias.symlink_to(socket)
    receipts = []
    for path in [alias, socket]:
        params = StdioServerParameters(
            command=sys.executable,
            args=[
                "-m",
                "codex_thread_bridge.server",
                "--socket",
                str(path),
                "--state-dir",
                str(tmp_path / "state"),
            ],
        )
        async with stdio_client(params) as (read, write), ClientSession(read, write) as session:
            await session.initialize()
            result = await session.call_tool(
                "create_thread",
                {
                    "request_id": "stable-create",
                    "cwd": str(tmp_path),
                    "prompt": "READY",
                },
            )
            assert not result.isError
            receipts.append(result.structuredContent)
    assert receipts[0]["threadId"] == receipts[1]["threadId"]
    assert receipts[1]["replayed"]
    assert fake.count("thread/start") == 1 and fake.count("turn/start") == 1

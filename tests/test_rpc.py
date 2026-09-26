import asyncio
import json
import tempfile
from pathlib import Path

import pytest
from websockets.asyncio.client import unix_connect
from websockets.asyncio.server import unix_serve

from codex_thread_bridge.rpc import AppServer, RpcError, TransportError


async def test_rpc_multiplexes_interleaved_notifications(fake_server):
    fake, path = fake_server
    client = AppServer(path)
    try:
        results = await asyncio.gather(
            *[client.call("thread/goal/get", {"threadId": f"thread-{i}"}) for i in range(10)]
        )
        assert results == [{"goal": None}] * 10
        assert fake.handshake_extensions == [None]
    finally:
        await client.close()


@pytest.mark.parametrize(
    "client_action",
    ["item/tool/call", "item/commandExecution/requestApproval", "item/tool/requestUserInput"],
)
async def test_client_actions_leave_shared_callback_for_subscribed_client(client_action):
    replies = []
    subscribers = {}

    async def handler(ws):
        async for raw in ws:
            message = json.loads(raw)
            method = message.get("method")
            if method is None:
                replies.append(message)
            elif method == "initialize":
                subscribers[message["params"]["clientInfo"]["name"]] = ws
                await ws.send(json.dumps({"id": message["id"], "result": {}}))
            elif method == "thread/read":
                # Server and client request IDs occupy independent namespaces.
                request = {"id": message["id"], "method": client_action, "params": {}}
                for subscriber in subscribers.values():
                    await subscriber.send(json.dumps(request))
                await ws.send(json.dumps({"id": message["id"], "result": {"thread": {}}}))
            elif method == "barrier":
                # WebSocket ordering guarantees any stolen reply arrives before this request.
                await ws.send(json.dumps({"id": message["id"], "result": {}}))

    with tempfile.TemporaryDirectory(prefix="ctb-owner-") as directory:
        path = Path(directory) / "app.sock"
        async with unix_serve(handler, str(path)):
            client = AppServer(path)
            try:
                async with unix_connect(str(path), compression=None) as desktop:
                    await desktop.send(
                        json.dumps(
                            {
                                "id": "desktop-init",
                                "method": "initialize",
                                "params": {"clientInfo": {"name": "desktop"}},
                            }
                        )
                    )
                    await desktop.recv()
                    assert await client.call("thread/read", {"threadId": "idle"}) == {"thread": {}}
                    request = json.loads(await desktop.recv())
                    await client.call("barrier", {})
                    assert replies == []
                    # The other subscriber can still resolve the shared callback.
                    response = {"id": request["id"], "result": {"handled": True}}
                    await desktop.send(json.dumps(response))
                    await desktop.send(json.dumps({"id": "done", "method": "barrier"}))
                    await desktop.recv()
                    assert replies == [response]
            finally:
                await client.close()


async def test_rpc_preserves_api_error_and_reconnects_only_for_new_requests(fake_server):
    fake, path = fake_server
    client = AppServer(path)
    try:
        fake.reject["thread/goal/get"] = {"code": -32601, "message": "unsupported"}
        with pytest.raises(RpcError, match="unsupported"):
            await client.call("thread/goal/get", {"threadId": "missing"})
        fake.reject.clear()
        fake.drop_after = "thread/goal/get"
        with pytest.raises(TransportError):
            await client.call("thread/goal/get", {"threadId": "dropped"})
        assert fake.count("thread/goal/get") == 2
        fake.drop_after = None
        assert await client.call("thread/goal/get", {"threadId": "new"}) == {"goal": None}
        assert fake.count("thread/goal/get") == 3
    finally:
        await client.close()


async def test_timeout_does_not_retry_request():
    calls = []
    stop = asyncio.Event()

    async def handler(ws):
        async for raw in ws:
            message = json.loads(raw)
            method = message["method"]
            calls.append(method)
            if method == "initialize":
                await ws.send(json.dumps({"id": message["id"], "result": {}}))
            elif method == "thread/start":
                await stop.wait()

    with tempfile.TemporaryDirectory(prefix="ctb-timeout-") as directory:
        path = Path(directory) / "app.sock"
        async with unix_serve(handler, str(path)):
            client = AppServer(path, timeout=0.05)
            try:
                with pytest.raises(TransportError, match="do not resend"):
                    await client.call("thread/start", {})
                assert calls.count("thread/start") == 1
            finally:
                stop.set()
                await client.close()

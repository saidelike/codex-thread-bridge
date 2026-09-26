import asyncio
import json
import os
import sys
from pathlib import Path

import pytest
from websockets.asyncio.server import unix_serve
from websockets.exceptions import InvalidMessage

from codex_thread_bridge import proxy_transport
from codex_thread_bridge.bridge import Bridge
from codex_thread_bridge.ledger import Ledger
from codex_thread_bridge.proxy_transport import ProxyWebSocket, resolve_codex_binary
from codex_thread_bridge.rpc import AppServer

RELAY = """
import socket, sys, threading
sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
sock.connect(sys.argv[1])
def upstream():
    while data := sys.stdin.buffer.read1(65536):
        sock.sendall(data)
    sock.shutdown(socket.SHUT_WR)
threading.Thread(target=upstream, daemon=True).start()
while data := sock.recv(65536):
    sys.stdout.buffer.write(data)
    sys.stdout.buffer.flush()
"""


@pytest.fixture
def relay_processes(monkeypatch):
    actual_spawn = asyncio.create_subprocess_exec
    processes = []

    async def spawn(*args, **kwargs):
        assert args[1:4] == ("app-server", "proxy", "--sock")
        process = await actual_spawn(sys.executable, "-c", RELAY, args[4], **kwargs)
        processes.append(process)
        return process

    monkeypatch.setattr(asyncio, "create_subprocess_exec", spawn)
    return processes


@pytest.mark.skipif(os.name == "nt", reason="fake upstream uses Unix socket")
async def test_proxy_preserves_websocket_fragments_and_ping(tmp_path, relay_processes):
    sock = tmp_path / "rpc.sock"

    async def handler(ws):
        assert await ws.recv() == "request"
        pong = await ws.ping(b"alive")
        await asyncio.wait_for(pong, 2)
        await ws.send(["frag", "mented"])
        await ws.wait_closed()

    async with unix_serve(handler, str(sock), compression=None):
        relay = await ProxyWebSocket.open(sock, 2, "fake-codex")
        try:
            await relay.send("request")
            assert await anext(aiter(relay)) == "fragmented"
        finally:
            await relay.close()
        assert relay_processes[0].returncode is not None
        assert all(task.done() for task in relay.pumps)
        assert all(endpoint.fileno() == -1 for endpoint in relay.sockets)


@pytest.mark.skipif(os.name == "nt", reason="fake upstream uses Unix socket")
async def test_proxy_failed_connect_reaps_child(tmp_path, relay_processes):
    with pytest.raises((InvalidMessage, ConnectionError, OSError)):
        await ProxyWebSocket.open(tmp_path / "missing.sock", 1, "fake-codex")
    assert relay_processes[0].returncode is not None


@pytest.mark.skipif(os.name == "nt", reason="fake upstream uses Unix socket")
async def test_cancel_during_handshake_reaps_child(tmp_path, relay_processes):
    entered = asyncio.Event()

    async def handler(reader, writer):
        entered.set()
        await reader.read()
        writer.close()
        await writer.wait_closed()

    sock = tmp_path / "silent.sock"
    async with await asyncio.start_unix_server(handler, str(sock)):
        pending = asyncio.create_task(ProxyWebSocket.open(sock, 30, "fake-codex"))
        await asyncio.wait_for(entered.wait(), 2)
        pending.cancel()
        with pytest.raises(asyncio.CancelledError):
            await pending
        assert relay_processes[0].returncode is not None


@pytest.mark.skipif(os.name == "nt", reason="child deliberately ignores POSIX SIGTERM")
async def test_cancel_during_close_still_reaps_uncooperative_child(monkeypatch):
    relay = ProxyWebSocket()
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-u",
        "-c",
        "import signal, time; signal.signal(signal.SIGTERM, signal.SIG_IGN); "
        "print('ready'); time.sleep(60)",
        stdout=asyncio.subprocess.PIPE,
    )
    relay.process = process
    assert process.stdout is not None
    terminated = asyncio.Event()
    terminate = process.terminate

    def record_terminate():
        terminate()
        terminated.set()

    monkeypatch.setattr(process, "terminate", record_terminate)
    try:
        assert await asyncio.wait_for(process.stdout.readline(), 5) == b"ready\n"
        closing = asyncio.create_task(relay.close())
        await asyncio.wait_for(terminated.wait(), 5)
        closing.cancel()
        with pytest.raises(asyncio.CancelledError):
            await closing
        # A second cancellation cannot abandon the shared cleanup either.
        another_close = asyncio.create_task(relay.close())
        await asyncio.sleep(0)
        another_close.cancel()
        with pytest.raises(asyncio.CancelledError):
            await another_close
        await asyncio.wait_for(relay.close(), 5)
        assert process.returncode is not None
        await relay.close()
    finally:
        if process.returncode is None:
            process.kill()
            await process.wait()


@pytest.mark.skipif(os.name == "nt", reason="fake upstream uses Unix socket")
async def test_proxy_lost_mutation_response_is_not_retried(fake_server, tmp_path, relay_processes):
    fake, sock = fake_server
    rpc = AppServer(sock, timeout=2, transport="proxy", codex_binary="fake-codex")
    ledger = Ledger(tmp_path / "operations.sqlite3")
    bridge = Bridge(rpc, ledger)
    try:
        fake.drop_after = "turn/start"
        result = await bridge.create_thread("lost-response", str(tmp_path), prompt="inspect")
        assert result["status"] == "outcome_unknown"
        assert result["threadId"] in fake.threads
        assert fake.count("turn/start") == 1
        replay = await bridge.create_thread("lost-response", str(tmp_path), prompt="inspect")
        assert replay == {**result, "replayed": True}
        assert fake.count("thread/start") == 1
        assert fake.count("turn/start") == 1
    finally:
        await rpc.close()
        ledger.close()
    assert all(process.returncode is not None for process in relay_processes)


@pytest.mark.skipif(os.name == "nt", reason="fake upstream uses Unix socket")
async def test_appserver_proxy_initialize(tmp_path, relay_processes, monkeypatch):
    monkeypatch.setenv("CODEX_THREAD_BRIDGE_TRANSPORT", "proxy")
    sock = tmp_path / "rpc.sock"

    async def handler(ws):
        initialize = json.loads(await ws.recv())
        assert initialize["method"] == "initialize"
        await ws.send(json.dumps({"id": initialize["id"], "result": {"version": "test"}}))
        assert json.loads(await ws.recv())["method"] == "initialized"
        request = json.loads(await ws.recv())
        await ws.send(json.dumps({"id": request["id"], "result": {"data": []}}))
        await ws.wait_closed()

    async with unix_serve(handler, str(sock), compression=None):
        rpc = AppServer(sock, timeout=2, codex_binary="fake-codex")
        try:
            assert await rpc.call("thread/list", {}) == {"data": []}
        finally:
            await rpc.close()
    assert relay_processes[0].returncode is not None


def test_explicit_binary_is_not_shell_split(monkeypatch):
    monkeypatch.setenv("CODEX_THREAD_BRIDGE_CODEX", "C:/Program Files/Codex/codex.exe")
    assert resolve_codex_binary() == "C:/Program Files/Codex/codex.exe"
    assert resolve_codex_binary("/explicit/codex") == "/explicit/codex"


def test_standalone_binary_detection(tmp_path, monkeypatch):
    monkeypatch.delenv("CODEX_THREAD_BRIDGE_CODEX", raising=False)
    monkeypatch.setenv("CODEX_HOME", str(tmp_path))
    suffix = ".exe" if sys.platform == "win32" else ""
    binary = tmp_path / f"packages/standalone/current/bin/codex{suffix}"
    binary.parent.mkdir(parents=True)
    binary.touch()
    assert resolve_codex_binary() == str(binary)


def test_missing_binary_has_actionable_error(tmp_path, monkeypatch):
    monkeypatch.delenv("CODEX_THREAD_BRIDGE_CODEX", raising=False)
    monkeypatch.setenv("CODEX_HOME", str(tmp_path))
    monkeypatch.delenv("LOCALAPPDATA", raising=False)
    monkeypatch.setattr(proxy_transport.shutil, "which", lambda _: None)
    with pytest.raises(FileNotFoundError, match="CODEX_THREAD_BRIDGE_CODEX"):
        resolve_codex_binary()


def test_invalid_transport_rejected(monkeypatch):
    monkeypatch.setenv("CODEX_THREAD_BRIDGE_TRANSPORT", "unexpected")
    with pytest.raises(ValueError, match="transport"):
        AppServer(Path("server.sock"))

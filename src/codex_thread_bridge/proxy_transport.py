"""WebSocket transport through Codex's cross-platform, raw stdio socket relay."""

import asyncio
import contextlib
import os
import shutil
import socket
import sys
from pathlib import Path

from websockets.asyncio.client import ClientConnection, connect


def resolve_codex_binary(explicit: str | None = None) -> str:
    configured = explicit or os.environ.get("CODEX_THREAD_BRIDGE_CODEX")
    if configured:
        return configured
    home = Path(os.environ.get("CODEX_HOME", Path.home() / ".codex"))
    suffix = ".exe" if sys.platform == "win32" else ""
    for relative in (f"bin/codex{suffix}", f"codex{suffix}"):
        candidate = home / "packages/standalone/current" / relative
        if candidate.is_file():
            return str(candidate)
    if sys.platform == "win32":
        local = os.environ.get("LOCALAPPDATA")
        if local:
            candidates = list((Path(local) / "OpenAI/Codex/bin").glob("*/codex.exe"))
            if candidates:
                return str(max(candidates, key=lambda path: path.stat().st_mtime))
    executable = shutil.which(f"codex{suffix}")
    if executable:
        return executable
    raise FileNotFoundError(
        "Codex executable not found; set CODEX_THREAD_BRIDGE_CODEX to the Codex binary"
    )


class ProxyWebSocket:
    """Own one CLI relay, its socket pair, and a standard websockets client."""

    def __init__(self):
        self.process: asyncio.subprocess.Process | None = None
        self.websocket: ClientConnection | None = None
        self.sockets: tuple[socket.socket, socket.socket] | None = None
        self.pumps: list[asyncio.Task[None]] = []
        self._close_task: asyncio.Task[None] | None = None

    @classmethod
    async def open(cls, socket_path: Path, open_timeout: float, codex_binary: str | None = None):
        relay = cls()
        try:
            relay.process = await asyncio.create_subprocess_exec(
                resolve_codex_binary(codex_binary),
                "app-server",
                "proxy",
                "--sock",
                str(socket_path),
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL,
            )
            relay.sockets = socket.socketpair()
            for endpoint in relay.sockets:
                endpoint.setblocking(False)
            relay.pumps = [
                asyncio.create_task(relay._to_proxy()),
                asyncio.create_task(relay._from_proxy()),
            ]
            # Windows socketpair uses loopback TCP internally, while the Codex
            # child handles AF_UNIX. The WebSocket implementation stays unchanged.
            relay.websocket = await connect(
                "ws://localhost/",
                sock=relay.sockets[1],
                proxy=None,
                compression=None,
                open_timeout=open_timeout,
                close_timeout=2,
                max_size=16 * 1024 * 1024,
            )
            return relay
        except BaseException:
            await relay.close()
            raise

    def _disconnect_socket(self):
        if self.sockets:
            with contextlib.suppress(OSError):
                self.sockets[0].shutdown(socket.SHUT_RDWR)

    async def _to_proxy(self):
        assert self.sockets and self.process and self.process.stdin
        try:
            while data := await asyncio.get_running_loop().sock_recv(self.sockets[0], 65536):
                self.process.stdin.write(data)
                await self.process.stdin.drain()
        finally:
            self.process.stdin.close()
            self._disconnect_socket()

    async def _from_proxy(self):
        assert self.sockets and self.process and self.process.stdout
        try:
            while data := await self.process.stdout.read(65536):
                await asyncio.get_running_loop().sock_sendall(self.sockets[0], data)
        finally:
            self._disconnect_socket()

    def __aiter__(self):
        assert self.websocket is not None
        return self.websocket.__aiter__()

    async def send(self, message: str):
        assert self.websocket is not None
        await self.websocket.send(message)

    async def close(self):
        # A cancelled caller must not cancel reaping the child. Every close
        # joins the same cleanup, including calls after an earlier cancellation.
        if self._close_task is None:
            self._close_task = asyncio.create_task(self._close())
        await asyncio.shield(self._close_task)

    async def _close(self):
        try:
            if self.websocket is not None:
                await self.websocket.close()
        finally:
            for task in self.pumps:
                task.cancel()
            for task in self.pumps:
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await task
            if self.sockets:
                for endpoint in self.sockets:
                    endpoint.close()
            if self.process is not None and self.process.returncode is None:
                with contextlib.suppress(ProcessLookupError):
                    self.process.terminate()
                try:
                    await asyncio.wait_for(self.process.wait(), 2)
                except TimeoutError:
                    with contextlib.suppress(ProcessLookupError):
                        self.process.kill()
                    await self.process.wait()

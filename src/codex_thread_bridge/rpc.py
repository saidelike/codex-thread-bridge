"""A multiplexed JSON-RPC client over the documented Unix WebSocket transport."""

import asyncio
import contextlib
import json
from pathlib import Path
from typing import Any

from websockets.asyncio.client import unix_connect

from . import __version__


class RpcError(Exception):
    def __init__(self, method: str, error: dict[str, Any]):
        self.method = method
        self.error = error
        super().__init__(f"{method}: {error.get('message', error)}")


class TransportError(Exception):
    """A request may have reached the server. Mutations must not be retried."""


class AppServer:
    def __init__(self, socket_path: Path, timeout: float = 20):
        self.socket_path = socket_path
        self.timeout = timeout
        self._ws = None
        self._reader = None
        self._pending: dict[int, asyncio.Future] = {}
        self._counter = 0
        self._connect_lock = asyncio.Lock()
        self.info: dict[str, Any] = {}

    async def connect(self):
        async with self._connect_lock:
            if self._reader is not None and not self._reader.done():
                return
            await self.close()
            try:
                self._ws = await unix_connect(
                    str(self.socket_path),
                    uri="ws://localhost/",
                    open_timeout=self.timeout,
                    close_timeout=2,
                    max_size=16 * 1024 * 1024,
                    # Codex 0.153.4 closes Unix handshakes offering permessage-deflate.
                    compression=None,
                )
                self._reader = asyncio.create_task(self._receive())
                self.info = await self._request(
                    "initialize",
                    {
                        "clientInfo": {"name": "codex_thread_bridge", "version": __version__},
                        "capabilities": {"experimentalApi": True},
                    },
                )
                await self._ws.send(json.dumps({"method": "initialized", "params": {}}))
            except BaseException:
                await self.close()
                raise

    async def _receive(self):
        failure = "App Server disconnected"
        ws = self._ws
        assert ws is not None
        try:
            async for raw in ws:
                message = json.loads(raw)
                if "method" in message:
                    # App Server shares server-request callbacks across subscribers;
                    # the first reply (even an error) consumes the callback. Leave
                    # these requests for a capable client subscribed to the thread.
                    # Silence does not guarantee another client is present.
                    # Reads and waits query the server; no unbounded event history.
                    continue
                future = self._pending.get(message.get("id"))
                if future is not None and not future.done():
                    future.set_result(message)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            failure = f"App Server transport failed: {type(error).__name__}: {error}"
        finally:
            for future in list(self._pending.values()):
                if not future.done():
                    future.set_exception(TransportError(failure))

    async def _request(self, method: str, params: dict[str, Any] | None):
        ws = self._ws
        if ws is None:
            raise TransportError("App Server is not connected")
        self._counter += 1
        ident = self._counter
        future = asyncio.get_running_loop().create_future()
        self._pending[ident] = future
        try:
            await ws.send(json.dumps({"id": ident, "method": method, "params": params}))
            message = await asyncio.wait_for(future, self.timeout)
        except (OSError, TimeoutError) as error:
            raise TransportError(f"{method}: response unavailable; do not resend") from error
        finally:
            self._pending.pop(ident, None)
            if not future.done():
                future.cancel()
        if "error" in message:
            raise RpcError(method, message["error"])
        if "result" not in message:
            raise TransportError(f"{method}: invalid response; outcome unknown")
        return message["result"]

    async def call(self, method: str, params: dict[str, Any] | None):
        await self.connect()
        # Reconnect before a new request, never retry an already-sent request.
        return await self._request(method, params)

    async def close(self):
        if self._ws is not None:
            await self._ws.close()
            self._ws = None
        if self._reader is not None:
            self._reader.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._reader
            self._reader = None

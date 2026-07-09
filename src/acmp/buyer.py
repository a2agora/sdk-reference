"""ACMP Buyer: sends ``acmp/invoke`` and awaits the matching result.

Implements the buyer side of Layer 1 §3.1–§3.7. A background reader task
dispatches incoming *responses* to whichever ``invoke()`` call is waiting on
that JSON-RPC request id, and incoming *notifications*
(``acmp/streamChunk``, ``acmp/heartbeat``) to the callbacks registered for
their ``task_id`` — so multiple invocations can be in flight at once.
"""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Callable
from typing import Any

from .errors import AcmpError, ErrorCode
from .messages import Payload, Result, Task, make_notification, make_request, new_request_id
from .transport import Transport, TransportClosed

ChunkCallback = Callable[[Payload, int, bool], Any]
"""``on_chunk(chunk, seq, final)`` — sync or async.

Callbacks run on the buyer's reader loop: keep them fast, and never await
another ``invoke()`` from inside one (that would deadlock the loop that has
to deliver its response).
"""

HeartbeatCallback = Callable[[float | None, str | None], Any]
"""``on_heartbeat(progress, detail)`` — sync or async. Same caveat as above."""


class Buyer:
    """Invokes tasks against a single provider over a Transport."""

    def __init__(self, transport: Transport) -> None:
        self._transport = transport
        self._pending: dict[str, asyncio.Future[dict]] = {}
        self._reader_task: asyncio.Task | None = None
        # task_id -> {"on_chunk": cb | None, "on_heartbeat": cb | None}
        self._listeners: dict[str, dict[str, Any]] = {}
        self._input_seq: dict[str, int] = {}

    async def __aenter__(self) -> "Buyer":
        self._reader_task = asyncio.create_task(self._read_loop())
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.close()

    async def close(self) -> None:
        """Stop the reader loop and close the transport.

        Closing the buyer's transport end puts a closure sentinel on the
        provider's inbound queue, which is what lets a provider's
        ``serve_forever()`` return instead of blocking on ``receive()``
        forever. Callers that own a provider task should ``await`` it after
        calling this.
        """
        if self._reader_task is not None:
            self._reader_task.cancel()
            try:
                await self._reader_task
            except (asyncio.CancelledError, TransportClosed):
                pass
            self._reader_task = None
        await self._transport.close()

    async def _read_loop(self) -> None:
        try:
            while True:
                message = await self._transport.receive()
                if "id" not in message and "method" in message:
                    await self._dispatch_notification(message)
                    continue
                req_id = message.get("id")
                future = self._pending.pop(req_id, None) if req_id else None
                if future is not None and not future.done():
                    future.set_result(message)
        except TransportClosed:
            for future in self._pending.values():
                if not future.done():
                    future.set_exception(TransportClosed("channel closed"))

    async def _dispatch_notification(self, message: dict) -> None:
        params = message.get("params", {})
        listeners = self._listeners.get(params.get("task_id", ""))
        if listeners is None:
            return  # no one is listening for this task — best-effort drop
        method = message["method"]
        if method == "acmp/streamChunk" and listeners.get("on_chunk"):
            outcome = listeners["on_chunk"](
                Payload.from_dict(params["chunk"]), params["seq"], params.get("final", False)
            )
            if inspect.isawaitable(outcome):
                await outcome
        elif method == "acmp/heartbeat" and listeners.get("on_heartbeat"):
            outcome = listeners["on_heartbeat"](params.get("progress"), params.get("detail"))
            if inspect.isawaitable(outcome):
                await outcome

    async def invoke(
        self,
        task: Task,
        *,
        on_chunk: ChunkCallback | None = None,
        on_heartbeat: HeartbeatCallback | None = None,
    ) -> Result:
        """Send ``acmp/invoke`` for ``task`` and await its ``acmp/result``.

        - ``on_chunk`` receives every ``acmp/streamChunk`` for this task
          (requires ``task.stream=True`` and a provider that advertised
          output streaming).
        - ``on_heartbeat`` receives every ``acmp/heartbeat`` for this task.
        - ``task.timeout_ms`` is enforced as the Layer 1 hard deadline: on
          expiry the buyer sends ``acmp/cancel`` and raises
          :class:`AcmpError` with ``TIMEOUT`` (-33003).

        Raises :class:`AcmpError` if the provider responds with ``acmp/error``.
        """
        if on_chunk is not None or on_heartbeat is not None:
            self._listeners[task.task_id] = {
                "on_chunk": on_chunk,
                "on_heartbeat": on_heartbeat,
            }
        try:
            result = await self.request(
                "acmp/invoke", task.to_params(), timeout_ms=task.timeout_ms
            )
        except AcmpError as err:
            if err.code == ErrorCode.TIMEOUT:
                # Layer 1 §3.7: tell the provider to stop working on it.
                await self.cancel(task.task_id, reason="timeout_ms exceeded")
            raise
        finally:
            self._listeners.pop(task.task_id, None)
        return Result.from_dict(result)

    async def request(
        self, method: str, params: dict, *, timeout_ms: int | None = None
    ) -> dict:
        """Send a JSON-RPC request and return its ``result`` payload.

        Shared by :meth:`invoke` and other request/response methods built on
        top of this connection (e.g. Layer 6 negotiation in
        :class:`acmp.negotiation.Negotiator`). Raises :class:`AcmpError` if
        the peer responds with a JSON-RPC error, or with ``TIMEOUT`` if no
        response arrives within ``timeout_ms``.
        """
        req_id = new_request_id()
        future: asyncio.Future[dict] = asyncio.get_event_loop().create_future()
        self._pending[req_id] = future

        try:
            await self._transport.send(make_request(method, params, req_id))
            if timeout_ms is None:
                response = await future
            else:
                try:
                    response = await asyncio.wait_for(future, timeout=timeout_ms / 1000)
                except asyncio.TimeoutError:
                    raise AcmpError(
                        ErrorCode.TIMEOUT,
                        data={"method": method, "timeout_ms": timeout_ms},
                    ) from None
        finally:
            self._pending.pop(req_id, None)

        if "error" in response:
            raise AcmpError.from_jsonrpc(response["error"])
        return response["result"]

    async def send_input_chunk(
        self, task_id: str, chunk: Payload, *, final: bool = False
    ) -> None:
        """Send one ``acmp/inputChunk`` for a running input-streaming task.

        Sequence numbers are assigned automatically per ``task_id``,
        starting at 0 (Layer 1 §3.4). Requires the task to have been invoked
        with ``input_stream=True`` against a provider advertising it.
        """
        seq = self._input_seq.get(task_id, 0)
        self._input_seq[task_id] = seq + 1
        await self._transport.send(
            make_notification(
                "acmp/inputChunk",
                {"task_id": task_id, "seq": seq, "chunk": chunk.to_dict(), "final": final},
            )
        )
        if final:
            self._input_seq.pop(task_id, None)

    async def cancel(self, task_id: str, reason: str | None = None) -> None:
        """Send ``acmp/cancel`` (Layer 1 §3.7).

        Fire-and-forget: the provider answers the original invoke with
        -33004 (``cancelled``) as soon as it stops.
        """
        params: dict[str, Any] = {"task_id": task_id}
        if reason is not None:
            params["reason"] = reason
        await self._transport.send(make_notification("acmp/cancel", params))

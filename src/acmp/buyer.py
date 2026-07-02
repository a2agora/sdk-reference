"""ACMP Buyer: sends ``acmp/invoke`` and awaits the matching result.

Implements the buyer side of Layer 1 §3.1–§3.3. A background reader task
dispatches incoming responses to whichever ``invoke()`` call is waiting on
that JSON-RPC request id, so multiple invocations can be in flight at once.
"""

from __future__ import annotations

import asyncio

from .errors import AcmpError
from .messages import Result, Task, make_request, new_request_id
from .transport import Transport, TransportClosed


class Buyer:
    """Invokes tasks against a single provider over a Transport."""

    def __init__(self, transport: Transport) -> None:
        self._transport = transport
        self._pending: dict[str, asyncio.Future[dict]] = {}
        self._reader_task: asyncio.Task | None = None

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
                req_id = message.get("id")
                future = self._pending.pop(req_id, None) if req_id else None
                if future is not None and not future.done():
                    future.set_result(message)
        except TransportClosed:
            for future in self._pending.values():
                if not future.done():
                    future.set_exception(TransportClosed("channel closed"))

    async def invoke(self, task: Task) -> Result:
        """Send ``acmp/invoke`` for ``task`` and await its ``acmp/result``.

        Raises :class:`AcmpError` if the provider responds with ``acmp/error``.
        """
        req_id = new_request_id()
        future: asyncio.Future[dict] = asyncio.get_event_loop().create_future()
        self._pending[req_id] = future

        request = make_request("acmp/invoke", task.to_params(), req_id)
        await self._transport.send(request)

        response = await future
        if "error" in response:
            raise AcmpError.from_jsonrpc(response["error"])
        return Result.from_dict(response["result"])

"""WebSocket transport for ACMP.

Layer 1 §1 names WebSocket as an example real transport beyond the in-memory
one used by the other examples: "Real transports (WebSocket, Streamable
HTTP) can implement the same interface" as :class:`~acmp.transport.Transport`.

Optional: requires the ``websockets`` package (``pip install acmp[ws]``). No
module in the ACMP core (``src/acmp/__init__.py`` included) imports this one
— the dependency stays opt-in, keeping the rest of the SDK dependency-light.
"""

from __future__ import annotations

import json
from collections.abc import Awaitable, Callable
from typing import Any
from urllib.parse import parse_qs, urlsplit

try:
    from websockets.asyncio.client import ClientConnection
    from websockets.asyncio.client import connect as _ws_connect
    from websockets.asyncio.server import Server, ServerConnection
    from websockets.asyncio.server import serve as _ws_serve
    from websockets.exceptions import ConnectionClosed
except ImportError as exc:  # pragma: no cover - exercised only without the extra
    raise ImportError(
        "acmp.ws_transport requires the 'websockets' package: pip install acmp[ws]"
    ) from exc

from .transport import Transport, TransportClosed

PARTY_ID_QUERY_KEY = "party_id"


class WebSocketTransport(Transport):
    """A :class:`~acmp.transport.Transport` backed by one open ``websockets``
    connection (either side: client or server)."""

    def __init__(self, connection: "ClientConnection | ServerConnection") -> None:
        self._connection = connection

    async def send(self, message: dict[str, Any]) -> None:
        try:
            await self._connection.send(json.dumps(message))
        except ConnectionClosed as exc:
            raise TransportClosed("websocket connection closed") from exc

    async def receive(self) -> dict[str, Any]:
        try:
            raw = await self._connection.recv()
        except ConnectionClosed as exc:
            raise TransportClosed("websocket connection closed") from exc
        return json.loads(raw)

    async def close(self) -> None:
        await self._connection.close()


def party_id_from_path(path: str) -> str | None:
    """Extract ``?party_id=...`` from a server connection's request path.

    Layer 4 §1: "``payee_id`` itself is self-reported at this layer" —
    verifiable identity binding is Layer 7's job. This is deliberately just a
    URL query parameter, not a handshake.
    """
    query = urlsplit(path).query
    values = parse_qs(query).get(PARTY_ID_QUERY_KEY)
    return values[0] if values else None


async def serve(
    on_connection: Callable[[WebSocketTransport, str | None], Awaitable[None]],
    host: str,
    port: int,
) -> "Server":
    """Start a WebSocket server; ``on_connection(transport, party_id)`` is
    awaited for every incoming connection, ``party_id`` read from the
    ``?party_id=`` query parameter of the connection URL (``None`` if
    absent).

    Returns the running server; call ``.close()`` then ``await
    .wait_closed()`` to stop.
    """

    async def _handler(connection: ServerConnection) -> None:
        party_id = party_id_from_path(connection.request.path)
        await on_connection(WebSocketTransport(connection), party_id)

    return await _ws_serve(_handler, host, port)


async def connect(uri: str) -> WebSocketTransport:
    """Open a client connection and wrap it as a :class:`WebSocketTransport`.

    Pass ``party_id`` as a query parameter on ``uri`` (e.g.
    ``ws://host:port/?party_id=agent:buyer:local``) for the server side to
    read via :func:`party_id_from_path`.
    """
    connection = await _ws_connect(uri)
    return WebSocketTransport(connection)

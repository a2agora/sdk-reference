"""Tests for the optional WebSocket transport (Layer 1 §1, `acmp[ws]`)."""

from __future__ import annotations

import asyncio

import pytest

from acmp.transport import TransportClosed
from acmp.ws_transport import WebSocketTransport, connect, party_id_from_path, serve


def test_party_id_from_path_reads_query_param():
    assert party_id_from_path("/?party_id=agent:buyer:local") == "agent:buyer:local"


def test_party_id_from_path_missing_returns_none():
    assert party_id_from_path("/") is None


def test_party_id_from_path_ignores_other_params():
    assert party_id_from_path("/?foo=bar&party_id=agent:x:y&baz=1") == "agent:x:y"


async def _start_echo_server() -> tuple:
    """A server that echoes every received JSON message back, capturing the
    party_id of each connection. Returns ``(server, port, seen_party_ids)``.
    """
    seen_party_ids: list[str | None] = []

    async def on_connection(transport: WebSocketTransport, party_id: str | None) -> None:
        seen_party_ids.append(party_id)
        try:
            while True:
                message = await transport.receive()
                await transport.send(message)
        except TransportClosed:
            return

    server = await serve(on_connection, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    return server, port, seen_party_ids


@pytest.mark.asyncio
async def test_send_receive_roundtrip_over_real_socket():
    server, port, _seen = await _start_echo_server()
    try:
        client = await connect(f"ws://127.0.0.1:{port}/")
        await client.send({"jsonrpc": "2.0", "method": "ping", "params": {}})
        echoed = await client.receive()
        assert echoed == {"jsonrpc": "2.0", "method": "ping", "params": {}}
        await client.close()
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_party_id_read_from_connection_url():
    server, port, seen_party_ids = await _start_echo_server()
    try:
        client = await connect(f"ws://127.0.0.1:{port}/?party_id=agent:buyer:local")
        await client.send({"jsonrpc": "2.0", "method": "x", "params": {}})
        await client.receive()  # let the server-side handler register the connection
        await client.close()
        await asyncio.sleep(0.05)  # let the server task observe the close
    finally:
        server.close()
        await server.wait_closed()

    assert seen_party_ids == ["agent:buyer:local"]


@pytest.mark.asyncio
async def test_receive_raises_transport_closed_after_peer_closes():
    server, port, _seen = await _start_echo_server()
    try:
        client = await connect(f"ws://127.0.0.1:{port}/")
        await client.close()

        with pytest.raises(TransportClosed):
            await client.receive()
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_send_raises_transport_closed_after_close():
    server, port, _seen = await _start_echo_server()
    try:
        client = await connect(f"ws://127.0.0.1:{port}/")
        await client.close()
        await asyncio.sleep(0.05)

        with pytest.raises(TransportClosed):
            await client.send({"jsonrpc": "2.0", "method": "x", "params": {}})
    finally:
        server.close()
        await server.wait_closed()

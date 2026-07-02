"""Transport abstraction for ACMP.

Layer 1 requires a *bidirectional* transport (server can push notifications to
the client). The abstract :class:`Transport` models one endpoint of such a
channel: you can ``send`` a JSON-RPC message and ``receive`` the next one.

:class:`InMemoryTransport` provides a paired, in-process implementation built on
two crossed asyncio queues — ideal for demos and tests without any network.
Real transports (WebSocket, Streamable HTTP) can implement the same interface.
"""

from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from typing import Any


class Transport(ABC):
    """One endpoint of a bidirectional JSON-RPC message channel."""

    @abstractmethod
    async def send(self, message: dict[str, Any]) -> None:
        """Send one JSON-RPC message to the peer."""

    @abstractmethod
    async def receive(self) -> dict[str, Any]:
        """Await and return the next JSON-RPC message from the peer.

        Raises :class:`TransportClosed` when the channel is closed.
        """

    @abstractmethod
    async def close(self) -> None:
        """Close this endpoint."""


class TransportClosed(Exception):
    """Raised by :meth:`Transport.receive` when the channel has been closed."""


class _QueueEndpoint(Transport):
    """A single endpoint backed by an inbound and outbound asyncio queue."""

    def __init__(
        self,
        inbound: asyncio.Queue[dict[str, Any] | None],
        outbound: asyncio.Queue[dict[str, Any] | None],
    ) -> None:
        self._inbound = inbound
        self._outbound = outbound
        self._closed = False

    async def send(self, message: dict[str, Any]) -> None:
        if self._closed:
            raise TransportClosed("endpoint is closed")
        await self._outbound.put(message)

    async def receive(self) -> dict[str, Any]:
        message = await self._inbound.get()
        if message is None:  # sentinel signalling closure
            raise TransportClosed("channel closed by peer")
        return message

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        # Wake the peer's receive() with a closure sentinel.
        await self._outbound.put(None)


class InMemoryTransport:
    """Factory for a connected pair of in-process transport endpoints."""

    @staticmethod
    def create_pair() -> tuple[Transport, Transport]:
        """Return ``(buyer_endpoint, provider_endpoint)`` wired together."""
        a_to_b: asyncio.Queue[dict[str, Any] | None] = asyncio.Queue()
        b_to_a: asyncio.Queue[dict[str, Any] | None] = asyncio.Queue()
        buyer = _QueueEndpoint(inbound=b_to_a, outbound=a_to_b)
        provider = _QueueEndpoint(inbound=a_to_b, outbound=b_to_a)
        return buyer, provider

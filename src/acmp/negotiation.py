"""Layer 6 negotiation: offer request -> offer -> accept, ahead of invoke.

Mirrors the message flow in spec/layers/06-negotiation-protocol.md:

    BUYER -> PROVIDER: offer request
    PROVIDER -> BUYER: offer { price, latency_sla, proof_method, valid_until_ms }
    BUYER -> PROVIDER: accept (+ escrow_id)
    PROVIDER -> BUYER: ack (task begins)

The spec's RFQ example is registry-directed (it flows through Layer 5/ARD
first and carries registry-oriented fields like ``escrow_id`` and
``offer_valid_ms`` on the RFQ itself). Layer 5 discovery is out of scope for
this SDK, so :class:`OfferRequest` goes straight to a known provider and
carries only the requirement fields from that RFQ (capability, budget,
latency SLA, proof requirement) — the fields relevant to the
"BUYER -> PROVIDER: offer request" step of the flow above.

Layer 6 is still `discussion` status in the spec and does not yet define
formal JSON-RPC method or error code names. This module picks concrete
ones (the ``acmp/offerRequest`` / ``acmp/accept`` methods, and the
-34xxx error range) as an SDK-level extension — kept clearly separate
from the Layer 1 §3.3 error codes in :mod:`acmp.errors`.
"""

from __future__ import annotations

import secrets
import time
from dataclasses import dataclass
from enum import IntEnum
from typing import Any

from .buyer import Buyer
from .messages import put_if_set

DEFAULT_OFFER_VALID_MS = 5000


class NegotiationErrorCode(IntEnum):
    """SDK-level negotiation errors (not part of the Layer 1 §3.3 table)."""

    OFFER_NOT_FOUND = -34001
    OFFER_EXPIRED = -34002
    ALREADY_ACCEPTED = -34003


def now_ms() -> float:
    """Current wall-clock time in milliseconds, for offer expiry checks."""
    return time.time() * 1000


def new_offer_id() -> str:
    return f"offer_{secrets.token_hex(6)}"


@dataclass
class OfferRequest:
    """The "BUYER -> PROVIDER: offer request" step (Layer 6)."""

    capability: str
    max_price_cu: float | None = None
    max_latency_ms: int | None = None
    proof_method: str | None = None
    input_tokens_est: int | None = None
    offer_valid_ms: int | None = None
    """Buyer's requested offer validity window; the provider may honour it,
    clamp it, or fall back to :data:`DEFAULT_OFFER_VALID_MS`."""

    def to_params(self) -> dict[str, Any]:
        params: dict[str, Any] = {"capability": self.capability}
        put_if_set(
            params,
            self,
            "max_price_cu",
            "max_latency_ms",
            "proof_method",
            "input_tokens_est",
            "offer_valid_ms",
        )
        return params

    @classmethod
    def from_params(cls, params: dict[str, Any]) -> "OfferRequest":
        return cls(
            capability=params["capability"],
            max_price_cu=params.get("max_price_cu"),
            max_latency_ms=params.get("max_latency_ms"),
            proof_method=params.get("proof_method"),
            input_tokens_est=params.get("input_tokens_est"),
            offer_valid_ms=params.get("offer_valid_ms"),
        )


@dataclass
class Offer:
    """The "PROVIDER -> BUYER: offer" step (Layer 6)."""

    offer_id: str
    capability: str
    price_cu: float
    valid_until_ms: float
    latency_sla_ms: int | None = None
    proof_method: str | None = None

    def is_expired(self, at_ms: float | None = None) -> bool:
        return (at_ms if at_ms is not None else now_ms()) > self.valid_until_ms

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {
            "offer_id": self.offer_id,
            "capability": self.capability,
            "price_cu": self.price_cu,
            "valid_until_ms": self.valid_until_ms,
        }
        if self.latency_sla_ms is not None:
            d["latency_sla_ms"] = self.latency_sla_ms
        if self.proof_method is not None:
            d["proof_method"] = self.proof_method
        return d

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "Offer":
        return cls(
            offer_id=d["offer_id"],
            capability=d["capability"],
            price_cu=d["price_cu"],
            valid_until_ms=d["valid_until_ms"],
            latency_sla_ms=d.get("latency_sla_ms"),
            proof_method=d.get("proof_method"),
        )


@dataclass
class AcceptedOffer:
    """The "PROVIDER -> BUYER: ack (task begins)" step (Layer 6)."""

    offer_id: str
    escrow_id: str
    price_cu: float

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "AcceptedOffer":
        return cls(offer_id=d["offer_id"], escrow_id=d["escrow_id"], price_cu=d["price_cu"])


class Negotiator:
    """Buyer-side Layer 6 negotiation, built on top of a Stage 1 :class:`Buyer`.

    Kept as a separate class rather than adding methods to :class:`Buyer` so
    the SDK's class boundaries mirror the spec's layering: :class:`Buyer` is
    Layer 1 only, :class:`Negotiator` is the Layer 6 exchange that happens
    before an ``acmp/invoke``.
    """

    def __init__(self, buyer: Buyer) -> None:
        self._buyer = buyer

    async def request_offer(self, offer_request: OfferRequest) -> Offer:
        """Send ``acmp/offerRequest`` and return the provider's :class:`Offer`."""
        result = await self._buyer.request("acmp/offerRequest", offer_request.to_params())
        return Offer.from_dict(result)

    async def accept(self, offer: Offer) -> AcceptedOffer:
        """Send ``acmp/accept`` for a still-valid offer and return the ack.

        Raises :class:`acmp.errors.AcmpError` with a :class:`NegotiationErrorCode`
        if the offer has expired, is unknown to the provider, or was already
        accepted.
        """
        result = await self._buyer.request("acmp/accept", {"offer_id": offer.offer_id})
        return AcceptedOffer.from_dict(result)

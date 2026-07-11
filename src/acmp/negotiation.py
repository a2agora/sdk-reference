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

The Layer 6 draft formalized this flow after the SDK proved it: the
``acmp/offerRequest`` / ``acmp/accept`` methods and the -34xxx error codes
below are wire-compatible with the spec (the spec adopted the SDK's de-facto
numbers). Per the spec, the *buyer* locks escrow (Layer 4) and supplies the
``escrow_id`` at accept; the ack echoes it. Offers MAY carry the negotiated
``challenge_window_ms`` term and SHOULD carry a Layer 7 ``sig`` envelope —
this SDK transports both fields but does not sign (no key infrastructure).
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
    preferred_tier: str | None = None
    proof_method: str | None = None
    input_tokens_est: int | None = None
    challenge_window_ms: int | None = None
    """Proposed claim challenge window (Layer 4 §4.5), negotiated as a term."""
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
            "preferred_tier",
            "proof_method",
            "input_tokens_est",
            "challenge_window_ms",
            "offer_valid_ms",
        )
        return params

    @classmethod
    def from_params(cls, params: dict[str, Any]) -> "OfferRequest":
        return cls(
            capability=params["capability"],
            max_price_cu=params.get("max_price_cu"),
            max_latency_ms=params.get("max_latency_ms"),
            preferred_tier=params.get("preferred_tier"),
            proof_method=params.get("proof_method"),
            input_tokens_est=params.get("input_tokens_est"),
            challenge_window_ms=params.get("challenge_window_ms"),
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
    challenge_window_ms: int | None = None
    """Confirmed challenge-window term; the buyer carries it to Layer 4 bind."""
    sig: dict[str, Any] | None = None
    """Layer 7 signature envelope over the offer (SHOULD per spec). This SDK
    transports the field but does not produce signatures itself."""

    def is_expired(self, at_ms: float | None = None) -> bool:
        return (at_ms if at_ms is not None else now_ms()) > self.valid_until_ms

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {
            "offer_id": self.offer_id,
            "capability": self.capability,
            "price_cu": self.price_cu,
            "valid_until_ms": self.valid_until_ms,
        }
        put_if_set(d, self, "latency_sla_ms", "proof_method", "challenge_window_ms", "sig")
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
            challenge_window_ms=d.get("challenge_window_ms"),
            sig=d.get("sig"),
        )


@dataclass
class AcceptedOffer:
    """The "PROVIDER -> BUYER: ack" step (Layer 6 §2.2).

    ``escrow_id`` echoes what the buyer supplied at accept; ``None`` means
    escrow-less direct settlement (Layer 1, "Relationship to Negotiation").
    """

    offer_id: str
    price_cu: float
    escrow_id: str | None = None

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "AcceptedOffer":
        return cls(
            offer_id=d["offer_id"], price_cu=d["price_cu"], escrow_id=d.get("escrow_id")
        )


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

    async def accept(self, offer: Offer, *, escrow_id: str | None = None) -> AcceptedOffer:
        """Send ``acmp/accept`` for a still-valid offer and return the ack.

        Per the Layer 6 draft (§2.2), the **buyer** locks the escrow (Layer 4)
        beforehand and passes its ``escrow_id`` here; omitting it signals
        escrow-less direct settlement.

        Raises :class:`acmp.errors.AcmpError` with a :class:`NegotiationErrorCode`
        if the offer has expired, is unknown to the provider, or was already
        accepted.
        """
        params: dict[str, Any] = {"offer_id": offer.offer_id}
        if escrow_id is not None:
            params["escrow_id"] = escrow_id
        result = await self._buyer.request("acmp/accept", params)
        return AcceptedOffer.from_dict(result)

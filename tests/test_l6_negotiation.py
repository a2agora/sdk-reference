"""Tests for Stage 2: Layer 6 negotiation (offer request -> offer -> accept)
followed by a Layer 1 invoke using the negotiated escrow_id.
"""

from __future__ import annotations

import asyncio

import pytest

from acmp import (
    AcmpError,
    Buyer,
    ErrorCode,
    EscrowStub,
    InMemoryTransport,
    NegotiationErrorCode,
    Negotiator,
    OfferRequest,
    Payload,
    Provider,
    Task,
)


async def echo_capability(task: Task) -> Payload:
    return Payload(type="json", data={"echo": task.input.data})


def make_pair(price_cu: float = 0.003, latency_sla_ms: int | None = 800):
    buyer_t, provider_t = InMemoryTransport.create_pair()
    escrow = EscrowStub()
    provider = Provider(provider_t, provider_id="agent:test-provider:local", escrow=escrow)
    provider.register(
        "echo", echo_capability, price_cu=price_cu, tokens_per_call=10, latency_sla_ms=latency_sla_ms
    )
    return buyer_t, provider_t, provider, escrow


@pytest.mark.asyncio
async def test_full_negotiation_then_invoke():
    buyer_t, provider_t, provider, escrow = make_pair(price_cu=0.003)
    serve_task = asyncio.create_task(provider.serve_forever())

    async with Buyer(buyer_t) as buyer:
        negotiator = Negotiator(buyer)

        offer = await negotiator.request_offer(
            OfferRequest(
                capability="echo",
                max_price_cu=0.005,
                proof_method="result-hash",
                challenge_window_ms=86400000,
            )
        )
        assert offer.price_cu == 0.003
        assert offer.latency_sla_ms == 800
        assert offer.challenge_window_ms == 86400000  # term echoed (Layer 6 §2.1)
        assert not offer.is_expired()

        # Layer 6 §2.2: the buyer locks (Layer 4) and supplies the escrow_id.
        escrow_id = escrow.lock(offer.price_cu)
        accepted = await negotiator.accept(offer, escrow_id=escrow_id)
        assert accepted.price_cu == 0.003
        assert accepted.escrow_id == escrow_id

        task = Task(
            capability="echo",
            input=Payload(type="text", data="hi"),
            max_price_cu=accepted.price_cu,
            escrow_id=accepted.escrow_id,
            proof_method="result-hash",
        )
        result = await buyer.invoke(task)

    assert result.output.data == {"echo": "hi"}
    assert result.cost_cu == 0.003
    assert result.proof["method"] == "result-hash"

    await serve_task


@pytest.mark.asyncio
async def test_offer_request_exceeding_budget_raises():
    buyer_t, provider_t, provider, _escrow = make_pair(price_cu=0.01)
    serve_task = asyncio.create_task(provider.serve_forever())

    async with Buyer(buyer_t) as buyer:
        negotiator = Negotiator(buyer)
        with pytest.raises(AcmpError) as exc_info:
            await negotiator.request_offer(
                OfferRequest(capability="echo", max_price_cu=0.001)
            )

    assert exc_info.value.code == ErrorCode.BUDGET_EXCEEDED

    await serve_task


@pytest.mark.asyncio
async def test_offer_request_unknown_capability_raises():
    buyer_t, provider_t, provider, _escrow = make_pair()
    serve_task = asyncio.create_task(provider.serve_forever())

    async with Buyer(buyer_t) as buyer:
        negotiator = Negotiator(buyer)
        with pytest.raises(AcmpError) as exc_info:
            await negotiator.request_offer(OfferRequest(capability="does-not-exist"))

    assert exc_info.value.code == ErrorCode.CAPABILITY_NOT_FOUND

    await serve_task


@pytest.mark.asyncio
async def test_accept_unknown_offer_id_raises():
    buyer_t, provider_t, provider, _escrow = make_pair()
    serve_task = asyncio.create_task(provider.serve_forever())

    async with Buyer(buyer_t) as buyer:
        with pytest.raises(AcmpError) as exc_info:
            await buyer.request("acmp/accept", {"offer_id": "offer_does_not_exist"})

    assert exc_info.value.code == NegotiationErrorCode.OFFER_NOT_FOUND

    await serve_task


@pytest.mark.asyncio
async def test_offer_request_with_unsupported_proof_raises():
    """Layer 6 §2.1: quoting an undeliverable proof method MUST fail (-33006)."""
    buyer_t, provider_t, provider, _escrow = make_pair()
    serve_task = asyncio.create_task(provider.serve_forever())

    async with Buyer(buyer_t) as buyer:
        negotiator = Negotiator(buyer)
        with pytest.raises(AcmpError) as exc_info:
            await negotiator.request_offer(
                OfferRequest(capability="echo", proof_method="execution-trace")
            )

    assert exc_info.value.code == ErrorCode.PROOF_UNSUPPORTED

    await serve_task


@pytest.mark.asyncio
async def test_accept_without_escrow_is_direct_mode():
    buyer_t, provider_t, provider, _escrow = make_pair()
    serve_task = asyncio.create_task(provider.serve_forever())

    async with Buyer(buyer_t) as buyer:
        negotiator = Negotiator(buyer)
        offer = await negotiator.request_offer(OfferRequest(capability="echo"))
        accepted = await negotiator.accept(offer)  # no escrow_id

    assert accepted.escrow_id is None  # direct settlement (Layer 1 §Relationship)

    await serve_task


@pytest.mark.asyncio
async def test_double_accept_raises_already_accepted():
    buyer_t, provider_t, provider, _escrow = make_pair()
    serve_task = asyncio.create_task(provider.serve_forever())

    async with Buyer(buyer_t) as buyer:
        negotiator = Negotiator(buyer)
        offer = await negotiator.request_offer(OfferRequest(capability="echo"))
        await negotiator.accept(offer)

        with pytest.raises(AcmpError) as exc_info:
            await negotiator.accept(offer)

    assert exc_info.value.code == NegotiationErrorCode.ALREADY_ACCEPTED

    await serve_task


@pytest.mark.asyncio
async def test_accept_after_expiry_raises_offer_expired():
    buyer_t, provider_t, provider, _escrow = make_pair()
    serve_task = asyncio.create_task(provider.serve_forever())

    async with Buyer(buyer_t) as buyer:
        negotiator = Negotiator(buyer)
        offer = await negotiator.request_offer(
            OfferRequest(capability="echo", offer_valid_ms=1)
        )
        await asyncio.sleep(0.05)  # let the 1ms validity window lapse

        with pytest.raises(AcmpError) as exc_info:
            await negotiator.accept(offer)

    assert exc_info.value.code == NegotiationErrorCode.OFFER_EXPIRED

    await serve_task


@pytest.mark.asyncio
async def test_invoke_with_invalid_escrow_id_raises():
    buyer_t, provider_t, provider, _escrow = make_pair(price_cu=0.003)
    serve_task = asyncio.create_task(provider.serve_forever())

    async with Buyer(buyer_t) as buyer:
        task = Task(
            capability="echo",
            input=Payload(type="text", data="x"),
            escrow_id="esc_never_locked",
        )
        with pytest.raises(AcmpError) as exc_info:
            await buyer.invoke(task)

    assert exc_info.value.code == ErrorCode.ESCROW_INVALID

    await serve_task

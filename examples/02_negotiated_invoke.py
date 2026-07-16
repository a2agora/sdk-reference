"""Stage 2 demo: RFQ -> offer -> accept -> invoke (Layer 6 then Layer 1),
backed by a real Layer 4 Escrow Agent (Stage 5) instead of a shared stub.

Same sentiment-analysis capability as examples/01_minimal_invoke.py, but this
time the buyer negotiates a price, locks funds with a genuine Escrow Agent —
its own connection, separate from both buyer and provider — binds it to the
provider, invokes, then releases payment and reclaims the unused remainder.
Follows the message flows in spec/layers/06-negotiation-protocol.md and
spec/layers/04-escrow-settlement.md. Run with:

    python examples/02_negotiated_invoke.py
"""

from __future__ import annotations

import asyncio

from acmp import (
    Buyer,
    EscrowAgent,
    EscrowClient,
    InMemoryTransport,
    Negotiator,
    OfferRequest,
    Payload,
    Provider,
    Task,
)

POSITIVE_WORDS = {"grew", "grow", "growth", "positive", "up"}
NEGATIVE_WORDS = {"compressed", "declined", "decline", "negative", "down"}

BUYER_ID = "agent:buyer-demo:local"


async def sentiment_analysis(task: Task) -> Payload:
    text = task.input.data.lower()
    words = set(text.replace(",", "").replace(".", "").split())
    pos = len(words & POSITIVE_WORDS)
    neg = len(words & NEGATIVE_WORDS)
    sentiment = "mixed" if pos and neg else "positive" if pos else "negative" if neg else "neutral"
    return Payload(type="json", data={"sentiment": sentiment, "pos_hits": pos, "neg_hits": neg})


async def main() -> None:
    buyer_transport, provider_transport = InMemoryTransport.create_pair()
    escrow_client_transport, escrow_agent_transport = InMemoryTransport.create_pair()

    # A real Layer 4 Escrow Agent: a genuinely separate connection, not
    # shared in-process with the provider the way the retired EscrowStub was.
    escrow_agent = EscrowAgent()
    escrow_agent.ledger.credit(BUYER_ID, 1.0)  # demo funding
    escrow_serve_task = asyncio.create_task(
        escrow_agent.serve(escrow_agent_transport, BUYER_ID)
    )

    # This demo doesn't wire the provider to the Escrow Agent (no escrow_id
    # coverage check at invoke time) — examples/05_escrow_e2e.py shows that
    # full three-party topology, including the provider's own EscrowClient.
    provider = Provider(provider_transport, provider_id="agent:openclaw-3:us-east")
    provider.register(
        "sentiment-analysis",
        sentiment_analysis,
        price_cu=0.003,
        tokens_per_call=115,
        latency_sla_ms=800,
    )
    serve_task = asyncio.create_task(provider.serve_forever())

    async with (
        Buyer(buyer_transport) as buyer,
        Buyer(escrow_client_transport) as escrow_conn,
    ):
        negotiator = Negotiator(buyer)
        escrow = EscrowClient(escrow_conn)

        print("--- offer request ---")
        offer_request = OfferRequest(
            capability="sentiment-analysis",
            max_price_cu=0.005,
            max_latency_ms=800,
            proof_method="result-hash",
        )
        offer = await negotiator.request_offer(offer_request)
        print(f"offer_id:       {offer.offer_id}")
        print(f"price_cu:       {offer.price_cu}")
        print(f"latency_sla_ms: {offer.latency_sla_ms}")
        print(f"valid_until_ms: {offer.valid_until_ms:.0f}")

        print("\n--- lock escrow (Layer 4), then accept ---")
        # Per the Layer 6 draft, the BUYER locks the funds and supplies the
        # escrow_id at accept; the ack echoes it. Locking the buyer's budget
        # ceiling (max_price_cu) rather than the exact negotiated price
        # leaves a genuine remainder to reclaim below — the RFC-0001 happy
        # path (lock 0.005, release 0.003, reclaim 0.002).
        locked = await escrow.lock(
            offer_request.max_price_cu, valid_until_ms=int(offer.valid_until_ms) + 3_600_000
        )
        accepted = await negotiator.accept(offer, escrow_id=locked.escrow_id)
        print(f"escrow_id:      {accepted.escrow_id}")
        print(f"price_cu:       {accepted.price_cu}")

        print("\n--- bind escrow to the provider (Layer 4 §4.2) ---")
        await escrow.bind(accepted.escrow_id, provider.provider_id)

        print("\n--- invoke ---")
        task = Task(
            capability="sentiment-analysis",
            input=Payload(
                type="text",
                data="Revenue grew 12% YoY but margins compressed due to rising input costs.",
            ),
            max_price_cu=accepted.price_cu,
            escrow_id=accepted.escrow_id,
            proof_method="result-hash",
        )
        result = await buyer.invoke(task)
        print(f"output:         {result.output.data}")
        print(f"cost_cu:        {result.cost_cu}")
        print(f"proof:          {result.proof}")

        print("\n--- release payment, reclaim the rest (Layer 4) ---")
        released = await escrow.release(accepted.escrow_id, result.cost_cu, provider.provider_id)
        reclaimed = await escrow.reclaim(accepted.escrow_id)
        print(f"released_cu:    {released.released_cu}")
        print(f"reclaimed_cu:   {reclaimed.reclaimed_cu}")
        print(f"escrow state:   {reclaimed.state}")

    await serve_task
    await escrow_serve_task


if __name__ == "__main__":
    asyncio.run(main())

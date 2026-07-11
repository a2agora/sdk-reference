"""Stage 2 demo: RFQ -> offer -> accept -> invoke (Layer 6 then Layer 1).

Same sentiment-analysis capability as examples/01_minimal_invoke.py, but this
time the buyer negotiates a price and locks escrow before invoking, following
the message flow in spec/layers/06-negotiation-protocol.md. Run with:

    python examples/02_negotiated_invoke.py
"""

from __future__ import annotations

import asyncio

from acmp import (
    Buyer,
    EscrowStub,
    InMemoryTransport,
    Negotiator,
    OfferRequest,
    Payload,
    Provider,
    Task,
)

POSITIVE_WORDS = {"grew", "grow", "growth", "positive", "up"}
NEGATIVE_WORDS = {"compressed", "declined", "decline", "negative", "down"}


async def sentiment_analysis(task: Task) -> Payload:
    text = task.input.data.lower()
    words = set(text.replace(",", "").replace(".", "").split())
    pos = len(words & POSITIVE_WORDS)
    neg = len(words & NEGATIVE_WORDS)
    sentiment = "mixed" if pos and neg else "positive" if pos else "negative" if neg else "neutral"
    return Payload(type="json", data={"sentiment": sentiment, "pos_hits": pos, "neg_hits": neg})


async def main() -> None:
    buyer_transport, provider_transport = InMemoryTransport.create_pair()

    # A real Layer 4 escrow service is a neutral third party; here both sides
    # of this in-process demo share one EscrowStub instance for simplicity.
    escrow = EscrowStub()
    provider = Provider(provider_transport, provider_id="agent:openclaw-3:us-east", escrow=escrow)
    provider.register(
        "sentiment-analysis",
        sentiment_analysis,
        price_cu=0.003,
        tokens_per_call=115,
        latency_sla_ms=800,
    )
    serve_task = asyncio.create_task(provider.serve_forever())

    async with Buyer(buyer_transport) as buyer:
        negotiator = Negotiator(buyer)

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
        # escrow_id at accept; the ack echoes it.
        escrow_id = escrow.lock(offer.price_cu)
        accepted = await negotiator.accept(offer, escrow_id=escrow_id)
        print(f"escrow_id:      {accepted.escrow_id}")
        print(f"price_cu:       {accepted.price_cu}")

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

    await serve_task


if __name__ == "__main__":
    asyncio.run(main())

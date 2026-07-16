"""Stage 5 end-to-end demo: buyer, provider, and Escrow Agent as three
genuinely separate WebSocket endpoints (Layer 1 + Layer 4 + Layer 6 together).

Unlike every earlier example, nothing here is shared in-process: the Escrow
Agent and the provider each run their own ``websockets`` server, and the
provider holds its own connection to the Escrow Agent — exactly the
three-party topology Layer 4 §1 describes, and the reference implementation
the retired ``EscrowStub`` explicitly was not.

Two scenarios:

  A. Happy path — lock -> offer/accept -> bind -> invoke -> release ->
     reclaim -> closed (the RFC-0001 sequence).
  B. Safety path — the buyer goes silent after invoke; the provider claims
     payment with its proof; the escrow's injected clock is advanced past
     the challenge window with no wall-clock waiting, and the claim
     auto-releases.

Requires the optional WebSocket extra:

    pip install -e ".[ws]"
    python examples/05_escrow_e2e.py
"""

from __future__ import annotations

import asyncio

from acmp import (
    Buyer,
    EscrowAgent,
    EscrowClient,
    Negotiator,
    OfferRequest,
    Payload,
    Provider,
    Task,
)
from acmp.ws_transport import connect as ws_connect
from acmp.ws_transport import serve as ws_serve

POSITIVE_WORDS = {"grew", "grow", "growth", "positive", "up"}
NEGATIVE_WORDS = {"compressed", "declined", "decline", "negative", "down"}

BUYER_ID = "agent:buyer-demo:local"
PROVIDER_ID = "agent:openclaw-3:us-east"


async def sentiment_analysis(task: Task) -> Payload:
    text = task.input.data.lower()
    words = set(text.replace(",", "").replace(".", "").split())
    pos = len(words & POSITIVE_WORDS)
    neg = len(words & NEGATIVE_WORDS)
    sentiment = "mixed" if pos and neg else "positive" if pos else "negative" if neg else "neutral"
    return Payload(type="json", data={"sentiment": sentiment, "pos_hits": pos, "neg_hits": neg})


def _port_of(server) -> int:
    return server.sockets[0].getsockname()[1]


async def main() -> None:
    # An injectable clock, advanced by hand for Scenario B — no real waiting
    # for a challenge window to elapse (Layer 4 §2, §4.5).
    clock = {"now": 1_000_000}

    escrow_agent = EscrowAgent(now_ms=lambda: clock["now"])
    escrow_agent.ledger.credit(BUYER_ID, 1.0)  # demo funding

    async def escrow_on_connection(transport, party_id) -> None:
        await escrow_agent.serve(transport, party_id)

    escrow_server = await ws_serve(escrow_on_connection, "127.0.0.1", 0)
    escrow_port = _port_of(escrow_server)
    escrow_uri = f"ws://127.0.0.1:{escrow_port}"
    print(f"Escrow Agent listening on {escrow_uri}")

    async def provider_on_connection(transport, _party_id) -> None:
        # A fresh Provider per connection, wired to the Escrow Agent over
        # its own connection — the provider checks escrow_invalid (-33005)
        # and later claims through this same client, never in-process.
        provider_escrow_transport = await ws_connect(f"{escrow_uri}/?party_id={PROVIDER_ID}")
        async with Buyer(provider_escrow_transport) as provider_escrow_conn:
            provider = Provider(
                transport,
                provider_id=PROVIDER_ID,
                escrow=EscrowClient(provider_escrow_conn),
            )
            provider.register(
                "sentiment-analysis",
                sentiment_analysis,
                price_cu=0.003,
                tokens_per_call=115,
                latency_sla_ms=800,
            )
            await provider.serve_forever()

    provider_server = await ws_serve(provider_on_connection, "127.0.0.1", 0)
    provider_port = _port_of(provider_server)
    provider_uri = f"ws://127.0.0.1:{provider_port}"
    print(f"Provider listening on {provider_uri}\n")

    buyer_transport = await ws_connect(f"{provider_uri}/")
    buyer_escrow_transport = await ws_connect(f"{escrow_uri}/?party_id={BUYER_ID}")

    async with (
        Buyer(buyer_transport) as buyer,
        Buyer(buyer_escrow_transport) as buyer_escrow_conn,
    ):
        negotiator = Negotiator(buyer)
        escrow = EscrowClient(buyer_escrow_conn)

        # === Scenario A: happy path =========================================
        print("=== Scenario A: happy path (lock -> ... -> closed) ===\n")

        offer_request = OfferRequest(
            capability="sentiment-analysis", max_price_cu=0.005, proof_method="result-hash"
        )
        offer = await negotiator.request_offer(offer_request)
        print(f"offer price_cu:   {offer.price_cu}")

        locked = await escrow.lock(
            offer_request.max_price_cu,
            valid_until_ms=clock["now"] + 3_600_000,
            challenge_window_ms=60_000,
        )
        accepted = await negotiator.accept(offer, escrow_id=locked.escrow_id)
        await escrow.bind(accepted.escrow_id, PROVIDER_ID)
        print(f"escrow locked:    {locked.amount_cu} CU (escrow_id={locked.escrow_id})")

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
        print(f"invoke output:    {result.output.data}")
        print(f"invoke cost_cu:   {result.cost_cu}")

        released = await escrow.release(accepted.escrow_id, result.cost_cu, PROVIDER_ID)
        reclaimed = await escrow.reclaim(accepted.escrow_id)
        print(f"released_cu:      {released.released_cu}")
        print(f"reclaimed_cu:     {reclaimed.reclaimed_cu}")
        print(f"final state:      {reclaimed.state}")
        assert reclaimed.state == "closed"

        # === Scenario B: safety path (silent buyer -> claim -> auto-release) ==
        print("\n=== Scenario B: safety path (silent buyer -> claim -> auto-release) ===\n")

        offer_b = await negotiator.request_offer(offer_request)
        locked_b = await escrow.lock(
            offer_request.max_price_cu,
            valid_until_ms=clock["now"] + 3_600_000,
            challenge_window_ms=60_000,
        )
        accepted_b = await negotiator.accept(offer_b, escrow_id=locked_b.escrow_id)
        await escrow.bind(accepted_b.escrow_id, PROVIDER_ID)

        task_b = Task(
            capability="sentiment-analysis",
            input=Payload(type="text", data="Guidance declined sharply amid weak demand."),
            max_price_cu=accepted_b.price_cu,
            escrow_id=accepted_b.escrow_id,
            proof_method="result-hash",
        )
        result_b = await buyer.invoke(task_b)
        print(f"invoke output:    {result_b.output.data}")
        print("buyer goes silent — no release call")

        # The provider claims with its own connection to the Escrow Agent.
        provider_claim_transport = await ws_connect(f"{escrow_uri}/?party_id={PROVIDER_ID}")
        async with Buyer(provider_claim_transport) as provider_claim_conn:
            provider_escrow = EscrowClient(provider_claim_conn)
            claimed = await provider_escrow.claim(
                accepted_b.escrow_id, result_b.cost_cu, result_b.task_id, result_b.proof
            )
            print(f"provider claimed: {claimed.claim['amount_cu']} CU, state={claimed.state}")

        pre_release_balance = escrow_agent.ledger.balance(PROVIDER_ID)
        clock["now"] = claimed.claim["window_ends_ms"] + 1  # fast-forward, no waiting
        print(f"clock advanced past window_ends_ms={claimed.claim['window_ends_ms']}")

        status_b = await escrow.status(accepted_b.escrow_id)
        print(f"status after window elapsed: state={status_b.state}, claim={status_b.claim}")
        assert status_b.claim is None  # auto-released
        assert escrow_agent.ledger.balance(PROVIDER_ID) == pre_release_balance + result_b.cost_cu
        print("provider was paid automatically — the safety path held.")

    provider_server.close()
    await provider_server.wait_closed()
    escrow_server.close()
    await escrow_server.wait_closed()


if __name__ == "__main__":
    asyncio.run(main())

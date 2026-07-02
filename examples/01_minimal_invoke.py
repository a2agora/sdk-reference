"""Stage 1 demo: a buyer invokes a sentiment-analysis task on a provider.

Mirrors the worked example in spec/layers/02-task-format.md §6.1, running over
an in-process transport (no network needed). Run with:

    python examples/01_minimal_invoke.py
"""

from __future__ import annotations

import asyncio

from acmp import Buyer, InMemoryTransport, Payload, Provider, Task

POSITIVE_WORDS = {"grew", "grow", "growth", "positive", "up"}
NEGATIVE_WORDS = {"compressed", "declined", "decline", "negative", "down"}


async def sentiment_analysis(task: Task) -> Payload:
    """A toy capability handler: naive keyword-based sentiment."""
    text = task.input.data.lower()
    words = set(text.replace(",", "").replace(".", "").split())
    pos = len(words & POSITIVE_WORDS)
    neg = len(words & NEGATIVE_WORDS)

    if pos and neg:
        sentiment = "mixed"
    elif pos:
        sentiment = "positive"
    elif neg:
        sentiment = "negative"
    else:
        sentiment = "neutral"

    return Payload(type="json", data={"sentiment": sentiment, "pos_hits": pos, "neg_hits": neg})


async def main() -> None:
    buyer_transport, provider_transport = InMemoryTransport.create_pair()

    provider = Provider(provider_transport, provider_id="agent:openclaw-3:us-east")
    provider.register(
        "sentiment-analysis",
        sentiment_analysis,
        price_cu=0.003,
        tokens_per_call=115,
    )
    serve_task = asyncio.create_task(provider.serve_forever())

    async with Buyer(buyer_transport) as buyer:
        task = Task(
            capability="sentiment-analysis",
            input=Payload(
                type="text",
                data="Revenue grew 12% YoY but margins compressed due to rising input costs.",
            ),
            max_price_cu=0.005,
            proof_method="result-hash",
        )
        result = await buyer.invoke(task)

        print(f"task_id:     {result.task_id}")
        print(f"output:      {result.output.data}")
        print(f"tokens_used: {result.tokens_used}")
        print(f"cost_cu:     {result.cost_cu}")
        print(f"proof:       {result.proof}")
        print(f"provider_id: {result.provider_id}")

    # Exiting the `async with` block closed the buyer's transport, which
    # signals the provider to stop; wait for its serve loop to end cleanly.
    await serve_task


if __name__ == "__main__":
    asyncio.run(main())

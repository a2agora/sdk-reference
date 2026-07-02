"""Stage 3 demo: split -> parallel sentiment -> aggregate (Layer 2 DAG).

Mirrors the worked example in spec/layers/02-task-format.md §6.2:

    [text-split] -> [sentiment-02a] -> [aggregate]
                 -> [sentiment-02b] -/

The two sentiment tasks have no dependency on each other, so the
orchestrator invokes them concurrently. In this demo all three capabilities
happen to be served by one provider for simplicity — in a real deployment
each could be a different agent, discovered independently via Layer 5. Run
with:

    python examples/03_dag_pipeline.py
"""

from __future__ import annotations

import asyncio

from acmp import (
    Buyer,
    Dag,
    DagOrchestrator,
    DagTaskSpec,
    Edge,
    InMemoryTransport,
    InputRef,
    Payload,
    Provider,
    Task,
)

POSITIVE_WORDS = {"grew", "grow", "growth", "positive", "up", "strong"}
NEGATIVE_WORDS = {"compressed", "declined", "decline", "negative", "down", "weak"}


async def text_split(task: Task) -> Payload:
    """Split the input roughly in half, on a word boundary."""
    text: str = task.input.data
    midpoint = len(text) // 2
    split_at = text.rfind(" ", 0, midpoint)
    if split_at == -1:
        split_at = midpoint
    chunks = [text[:split_at].strip(), text[split_at:].strip()]
    return Payload(type="json", data={"chunks": chunks})


async def sentiment_analysis(task: Task) -> Payload:
    text = task.input.data.lower()
    words = set(text.replace(",", "").replace(".", "").split())
    pos = len(words & POSITIVE_WORDS)
    neg = len(words & NEGATIVE_WORDS)
    sentiment = "mixed" if pos and neg else "positive" if pos else "negative" if neg else "neutral"
    return Payload(type="json", data={"sentiment": sentiment})


async def result_aggregation(task: Task) -> Payload:
    """Combine per-chunk sentiment outputs (Layer 2 §1: from_tasks forwards
    each upstream task's full ``{type, data}`` output as an ordered array)."""
    parts = [item["data"]["sentiment"] for item in task.input.data]
    overall = parts[0] if len(set(parts)) == 1 else "mixed"
    return Payload(type="json", data={"overall_sentiment": overall, "parts": parts})


async def main() -> None:
    buyer_transport, provider_transport = InMemoryTransport.create_pair()

    provider = Provider(provider_transport, provider_id="agent:openclaw-3:us-east")
    provider.register("text-split", text_split, price_cu=0.001, tokens_per_call=20)
    provider.register("sentiment-analysis", sentiment_analysis, price_cu=0.003, tokens_per_call=115)
    provider.register("result-aggregation", result_aggregation, price_cu=0.001, tokens_per_call=10)
    serve_task = asyncio.create_task(provider.serve_forever())

    async with Buyer(buyer_transport) as buyer:
        dag = Dag(
            dag_id="dag_demo",
            tasks=[
                DagTaskSpec(
                    task_id="task_split_01",
                    capability="text-split",
                    input=Payload(
                        type="text",
                        data=(
                            "Revenue grew 12% YoY on strong demand. "
                            "Margins compressed sharply due to rising input costs."
                        ),
                    ),
                ),
                DagTaskSpec(
                    task_id="task_sent_02a",
                    capability="sentiment-analysis",
                    input=InputRef(from_task="task_split_01", field="data.chunks[0]"),
                ),
                DagTaskSpec(
                    task_id="task_sent_02b",
                    capability="sentiment-analysis",
                    input=InputRef(from_task="task_split_01", field="data.chunks[1]"),
                ),
                DagTaskSpec(
                    task_id="task_agg_03",
                    capability="result-aggregation",
                    input=InputRef(from_tasks=["task_sent_02a", "task_sent_02b"]),
                ),
            ],
            edges=[
                Edge(from_task="task_split_01", to_task="task_sent_02a"),
                Edge(from_task="task_split_01", to_task="task_sent_02b"),
                Edge(from_task="task_sent_02a", to_task="task_agg_03"),
                Edge(from_task="task_sent_02b", to_task="task_agg_03"),
            ],
        )

        results = await DagOrchestrator(buyer).run(dag)

        print(f"chunk 0 sentiment: {results['task_sent_02a'].output.data}")
        print(f"chunk 1 sentiment: {results['task_sent_02b'].output.data}")
        print(f"aggregated:        {results['task_agg_03'].output.data}")
        total_cost = sum(r.cost_cu for r in results.values())
        print(f"total cost_cu:     {total_cost}")

    await serve_task


if __name__ == "__main__":
    asyncio.run(main())

"""Stage 4 demo: output streaming, heartbeats, and cancellation (Layer 1).

A provider streams a token-by-token "translation" while emitting progress
heartbeats; the buyer consumes the chunks live. A second invocation is then
cancelled mid-flight. Run with:

    python examples/04_streaming_cancel.py
"""

from __future__ import annotations

import asyncio

from acmp import Buyer, InMemoryTransport, Payload, Provider, Task, TaskContext

WORDS = ["Agents", "trade", "compute", "like", "a", "commodity."]


async def streaming_translate(task: Task, ctx: TaskContext) -> None:
    """Emits one word per chunk, with progress heartbeats in between."""
    for i, word in enumerate(WORDS):
        if ctx.cancelled:
            return None
        await asyncio.sleep(0.05)  # simulated work per token
        await ctx.heartbeat(progress=(i + 1) / len(WORDS))
        await ctx.emit(Payload(type="text", data=word), final=(i == len(WORDS) - 1))
    return None


async def main() -> None:
    buyer_transport, provider_transport = InMemoryTransport.create_pair()
    provider = Provider(
        provider_transport,
        provider_id="agent:openclaw-3:us-east",
        output_streaming=True,
        heartbeat_interval_ms=100,
    )
    provider.register("translate", streaming_translate, price_cu=0.002, tokens_per_call=42)
    serve_task = asyncio.create_task(provider.serve_forever())

    async with Buyer(buyer_transport) as buyer:
        print("--- streaming invoke ---")
        words: list[str] = []

        def on_chunk(chunk: Payload, seq: int, final: bool) -> None:
            words.append(chunk.data)
            print(f"  chunk {seq}: {chunk.data!r}{'  (final)' if final else ''}")

        def on_heartbeat(progress: float | None, detail: str | None) -> None:
            if progress is not None:
                print(f"  heartbeat: {progress:.0%}")

        task = Task(capability="translate", input=Payload(type="text", data="..."), stream=True)
        result = await buyer.invoke(task, on_chunk=on_chunk, on_heartbeat=on_heartbeat)
        print(f"assembled:       {' '.join(words)!r}")
        print(f"output_streamed: {result.output_streamed}, cost_cu: {result.cost_cu}")

        print("\n--- cancellation ---")
        task2 = Task(capability="translate", input=Payload(type="text", data="..."), stream=True)
        invoke = asyncio.create_task(buyer.invoke(task2, on_chunk=on_chunk))
        await asyncio.sleep(0.12)  # let a couple of chunks through
        await buyer.cancel(task2.task_id, reason="user aborted")
        try:
            await invoke
        except Exception as exc:  # AcmpError CANCELLED (-33004)
            print(f"invoke ended with: {exc}")

    await serve_task


if __name__ == "__main__":
    asyncio.run(main())

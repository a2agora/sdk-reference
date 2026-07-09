"""Tests for Layer 1 streaming, heartbeats, cancellation, and timeouts."""

from __future__ import annotations

import asyncio

import pytest

from acmp import (
    AcmpError,
    Buyer,
    Dag,
    DagOrchestrator,
    DagTaskSpec,
    ErrorCode,
    InMemoryTransport,
    Payload,
    Provider,
    Task,
    TaskContext,
)
from acmp.messages import make_notification


def make_pair(**provider_kwargs):
    buyer_t, provider_t = InMemoryTransport.create_pair()
    provider = Provider(provider_t, provider_id="agent:test-provider:local", **provider_kwargs)
    return buyer_t, provider_t, provider


# -- output streaming ---------------------------------------------------------


async def counting_streamer(task: Task, ctx: TaskContext) -> None:
    for i in range(3):
        await ctx.emit(Payload(type="json", data={"n": i}), final=(i == 2))
    return None


@pytest.mark.asyncio
async def test_output_streaming_chunks_in_order_and_streamed_result():
    buyer_t, provider_t, provider = make_pair(output_streaming=True)
    provider.register("count", counting_streamer, price_cu=0.001)
    serve_task = asyncio.create_task(provider.serve_forever())

    received: list[tuple[int, dict, bool]] = []

    async with Buyer(buyer_t) as buyer:
        task = Task(capability="count", input=Payload(type="json", data=None), stream=True)
        result = await buyer.invoke(
            task, on_chunk=lambda chunk, seq, final: received.append((seq, chunk.data, final))
        )

    assert [seq for seq, _, _ in received] == [0, 1, 2]
    assert [d["n"] for _, d, _ in received] == [0, 1, 2]
    assert [f for _, _, f in received] == [False, False, True]
    assert result.output_streamed is True
    assert result.output is None

    await serve_task


@pytest.mark.asyncio
async def test_stream_rejected_without_output_streaming_feature():
    buyer_t, provider_t, provider = make_pair()  # feature NOT advertised
    provider.register("count", counting_streamer, price_cu=0.001)
    serve_task = asyncio.create_task(provider.serve_forever())

    async with Buyer(buyer_t) as buyer:
        task = Task(capability="count", input=Payload(type="json", data=None), stream=True)
        with pytest.raises(AcmpError) as exc_info:
            await buyer.invoke(task)

    assert exc_info.value.code == ErrorCode.FEATURE_UNSUPPORTED
    assert exc_info.value.data["feature"] == "output_streaming"

    await serve_task


@pytest.mark.asyncio
async def test_chunks_without_final_is_a_handler_bug():
    async def sloppy(task: Task, ctx: TaskContext) -> Payload:
        await ctx.emit(Payload(type="json", data={}))  # never sends final
        return Payload(type="json", data={})

    buyer_t, provider_t, provider = make_pair(output_streaming=True)
    provider.register("sloppy", sloppy, price_cu=0.0)
    serve_task = asyncio.create_task(provider.serve_forever())

    async with Buyer(buyer_t) as buyer:
        task = Task(capability="sloppy", input=Payload(type="json", data=None), stream=True)
        with pytest.raises(AcmpError) as exc_info:
            await buyer.invoke(task)

    assert exc_info.value.code == ErrorCode.INTERNAL

    await serve_task


# -- input streaming ----------------------------------------------------------


async def concatenator(task: Task, ctx: TaskContext) -> Payload:
    parts = []
    async for chunk in ctx.input_chunks():
        parts.append(chunk.data)
    return Payload(type="text", data="".join(parts))


@pytest.mark.asyncio
async def test_input_streaming_without_inline_input():
    buyer_t, provider_t, provider = make_pair(input_streaming=True)
    provider.register("concat", concatenator, price_cu=0.001)
    serve_task = asyncio.create_task(provider.serve_forever())

    async with Buyer(buyer_t) as buyer:
        task = Task(capability="concat", input=None, input_stream=True)
        invoke = asyncio.create_task(buyer.invoke(task))
        await buyer.send_input_chunk(task.task_id, Payload(type="text", data="Hel"))
        await buyer.send_input_chunk(task.task_id, Payload(type="text", data="lo"), final=True)
        result = await invoke

    assert result.output.data == "Hello"

    await serve_task


@pytest.mark.asyncio
async def test_input_chunks_reordered_by_seq():
    buyer_t, provider_t, provider = make_pair(input_streaming=True)
    provider.register("concat", concatenator, price_cu=0.001)
    serve_task = asyncio.create_task(provider.serve_forever())

    async with Buyer(buyer_t) as buyer:
        task = Task(capability="concat", input=None, input_stream=True)
        invoke = asyncio.create_task(buyer.invoke(task))
        await asyncio.sleep(0.01)  # let the invoke dispatch start
        # Send seq 1 (final) BEFORE seq 0 — bypassing the auto-seq helper.
        for seq, data, final in [(1, "B", True), (0, "A", False)]:
            await buyer._transport.send(
                make_notification(
                    "acmp/inputChunk",
                    {
                        "task_id": task.task_id,
                        "seq": seq,
                        "chunk": {"type": "text", "data": data},
                        "final": final,
                    },
                )
            )
        result = await invoke

    assert result.output.data == "AB"  # reordered despite arrival order B, A

    await serve_task


@pytest.mark.asyncio
async def test_input_stream_rejected_without_feature():
    buyer_t, provider_t, provider = make_pair()  # feature NOT advertised
    provider.register("concat", concatenator, price_cu=0.001)
    serve_task = asyncio.create_task(provider.serve_forever())

    async with Buyer(buyer_t) as buyer:
        task = Task(capability="concat", input=None, input_stream=True)
        with pytest.raises(AcmpError) as exc_info:
            await buyer.invoke(task)

    assert exc_info.value.code == ErrorCode.FEATURE_UNSUPPORTED

    await serve_task


@pytest.mark.asyncio
async def test_missing_input_without_input_stream_is_an_error():
    buyer_t, provider_t, provider = make_pair()
    provider.register("concat", concatenator, price_cu=0.001)
    serve_task = asyncio.create_task(provider.serve_forever())

    async with Buyer(buyer_t) as buyer:
        task = Task(capability="concat", input=None)  # no input, no streaming
        with pytest.raises(AcmpError) as exc_info:
            await buyer.invoke(task)

    assert exc_info.value.code == ErrorCode.INTERNAL

    await serve_task


# -- cancellation -------------------------------------------------------------


@pytest.mark.asyncio
async def test_cancel_stops_context_aware_handler():
    observed_cancel = asyncio.Event()

    async def long_runner(task: Task, ctx: TaskContext) -> Payload:
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            if ctx.cancelled:
                observed_cancel.set()
            raise
        return Payload(type="json", data={})

    buyer_t, provider_t, provider = make_pair()
    provider.register("long", long_runner, price_cu=0.0)
    serve_task = asyncio.create_task(provider.serve_forever())

    async with Buyer(buyer_t) as buyer:
        task = Task(capability="long", input=Payload(type="json", data=None))
        invoke = asyncio.create_task(buyer.invoke(task))
        await asyncio.sleep(0.02)
        await buyer.cancel(task.task_id, reason="changed my mind")
        with pytest.raises(AcmpError) as exc_info:
            await invoke

    assert exc_info.value.code == ErrorCode.CANCELLED
    assert observed_cancel.is_set()

    await serve_task


@pytest.mark.asyncio
async def test_cancel_stops_legacy_handler_too():
    async def legacy_long(task: Task) -> Payload:
        await asyncio.sleep(10)
        return Payload(type="json", data={})

    buyer_t, provider_t, provider = make_pair()
    provider.register("long", legacy_long, price_cu=0.0)
    serve_task = asyncio.create_task(provider.serve_forever())

    async with Buyer(buyer_t) as buyer:
        task = Task(capability="long", input=Payload(type="json", data=None))
        invoke = asyncio.create_task(buyer.invoke(task))
        await asyncio.sleep(0.02)
        await buyer.cancel(task.task_id)
        with pytest.raises(AcmpError) as exc_info:
            await invoke

    assert exc_info.value.code == ErrorCode.CANCELLED

    await serve_task


@pytest.mark.asyncio
async def test_dag_fail_fast_sends_cancel_to_provider():
    sibling_cancelled = asyncio.Event()

    async def slow_sibling(task: Task, ctx: TaskContext) -> Payload:
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            sibling_cancelled.set()
            raise
        return Payload(type="json", data={})

    buyer_t, provider_t, provider = make_pair()
    provider.register("slow", slow_sibling, price_cu=0.0)
    serve_task = asyncio.create_task(provider.serve_forever())

    dag = Dag(
        dag_id="ff",
        tasks=[
            DagTaskSpec(task_id="broken", capability="nope", input=Payload(type="json", data=None)),
            DagTaskSpec(task_id="sibling", capability="slow", input=Payload(type="json", data=None)),
        ],
        edges=[],
    )

    async with Buyer(buyer_t) as buyer:
        with pytest.raises(AcmpError):
            await DagOrchestrator(buyer).run(dag)
        # The orchestrator sent acmp/cancel for the sibling; give the
        # provider a moment to process it.
        await asyncio.wait_for(sibling_cancelled.wait(), timeout=1)

    assert sibling_cancelled.is_set()

    await serve_task


# -- heartbeats ---------------------------------------------------------------


@pytest.mark.asyncio
async def test_automatic_and_manual_heartbeats():
    async def worker(task: Task, ctx: TaskContext) -> Payload:
        await asyncio.sleep(0.05)
        await ctx.heartbeat(progress=0.5, detail="halfway")
        await asyncio.sleep(0.05)
        return Payload(type="json", data={"done": True})

    buyer_t, provider_t, provider = make_pair(heartbeat_interval_ms=20)
    provider.register("work", worker, price_cu=0.0)
    serve_task = asyncio.create_task(provider.serve_forever())

    beats: list[tuple[float | None, str | None]] = []

    async with Buyer(buyer_t) as buyer:
        task = Task(capability="work", input=Payload(type="json", data=None))
        result = await buyer.invoke(
            task, on_heartbeat=lambda progress, detail: beats.append((progress, detail))
        )

    assert result.output.data == {"done": True}
    assert len(beats) >= 3  # several automatic keep-alives plus the manual one
    assert (0.5, "halfway") in beats

    await serve_task


# -- timeout ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_timeout_raises_and_cancels_provider_side():
    handler_cancelled = asyncio.Event()

    async def too_slow(task: Task, ctx: TaskContext) -> Payload:
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            handler_cancelled.set()
            raise
        return Payload(type="json", data={})

    buyer_t, provider_t, provider = make_pair()
    provider.register("slow", too_slow, price_cu=0.0)
    serve_task = asyncio.create_task(provider.serve_forever())

    async with Buyer(buyer_t) as buyer:
        task = Task(capability="slow", input=Payload(type="json", data=None), timeout_ms=80)
        with pytest.raises(AcmpError) as exc_info:
            await buyer.invoke(task)
        assert exc_info.value.code == ErrorCode.TIMEOUT
        # invoke sent acmp/cancel on its way out — the provider should stop.
        await asyncio.wait_for(handler_cancelled.wait(), timeout=1)

    await serve_task

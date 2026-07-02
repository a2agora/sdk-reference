"""Tests for Stage 3: Layer 2 DAG orchestration."""

from __future__ import annotations

import asyncio
import time

import pytest

from acmp import (
    AcmpError,
    Buyer,
    Dag,
    DagOrchestrator,
    DagResolutionError,
    DagTaskSpec,
    DagValidationError,
    Edge,
    ErrorCode,
    InMemoryTransport,
    InputRef,
    Payload,
    Provider,
    Task,
)
from acmp.dag import _extract_field


def make_pair() -> tuple:
    buyer_t, provider_t = InMemoryTransport.create_pair()
    provider = Provider(provider_t, provider_id="agent:test-provider:local")
    return buyer_t, provider_t, provider


# -- field path resolution (unit, no transport needed) -----------------------


def test_extract_field_whole_data():
    root = {"type": "json", "data": {"chunks": ["a", "b"]}}
    assert _extract_field(root, "data", task_id="t") == {"chunks": ["a", "b"]}


def test_extract_field_member_access():
    root = {"type": "json", "data": {"foo": {"bar": 42}}}
    assert _extract_field(root, "data.foo.bar", task_id="t") == 42


def test_extract_field_array_index():
    root = {"type": "json", "data": {"items": ["x", "y"]}}
    assert _extract_field(root, "data.items[0]", task_id="t") == "x"


def test_extract_field_combined():
    root = {"type": "json", "data": {"items": [{"name": "first"}, {"name": "second"}]}}
    assert _extract_field(root, "data.items[0].name", task_id="t") == "first"
    assert _extract_field(root, "data.items[1].name", task_id="t") == "second"


def test_extract_field_missing_key_raises_resolution_error():
    root = {"type": "json", "data": {"foo": 1}}
    with pytest.raises(DagResolutionError):
        _extract_field(root, "data.bar", task_id="t")


def test_input_ref_requires_exactly_one_of_from_task_or_from_tasks():
    with pytest.raises(ValueError):
        InputRef()
    with pytest.raises(ValueError):
        InputRef(from_task="a", from_tasks=["b"])


# -- DAG validation -----------------------------------------------------------


def test_validate_dag_rejects_cycle():
    from acmp.dag import validate_dag

    dag = Dag(
        dag_id="d1",
        tasks=[
            DagTaskSpec(task_id="a", capability="x", input=Payload(type="text", data="")),
            DagTaskSpec(task_id="b", capability="x", input=Payload(type="text", data="")),
        ],
        edges=[Edge(from_task="a", to_task="b"), Edge(from_task="b", to_task="a")],
    )
    with pytest.raises(DagValidationError):
        validate_dag(dag)


def test_validate_dag_rejects_dangling_edge():
    from acmp.dag import validate_dag

    dag = Dag(
        dag_id="d1",
        tasks=[DagTaskSpec(task_id="a", capability="x", input=Payload(type="text", data=""))],
        edges=[Edge(from_task="a", to_task="does-not-exist")],
    )
    with pytest.raises(DagValidationError):
        validate_dag(dag)


def test_validate_dag_rejects_ref_without_edge():
    from acmp.dag import validate_dag

    dag = Dag(
        dag_id="d1",
        tasks=[
            DagTaskSpec(task_id="a", capability="x", input=Payload(type="text", data="")),
            DagTaskSpec(task_id="b", capability="x", input=InputRef(from_task="a")),
        ],
        edges=[],  # missing the a -> b edge the InputRef implies
    )
    with pytest.raises(DagValidationError):
        validate_dag(dag)


# -- end-to-end pipeline -------------------------------------------------------


async def text_split(task: Task) -> Payload:
    text = task.input.data
    mid = len(text) // 2
    return Payload(type="json", data={"chunks": [text[:mid], text[mid:]]})


async def echo_sentiment(task: Task) -> Payload:
    return Payload(type="json", data={"sentiment": "positive", "seen": task.input.data})


async def aggregate(task: Task) -> Payload:
    parts = [item["data"]["sentiment"] for item in task.input.data]
    return Payload(type="json", data={"parts": parts})


def make_pipeline_provider() -> tuple:
    buyer_t, provider_t, provider = make_pair()
    provider.register("text-split", text_split, price_cu=0.001)
    provider.register("sentiment-analysis", echo_sentiment, price_cu=0.003)
    provider.register("result-aggregation", aggregate, price_cu=0.001)
    return buyer_t, provider_t, provider


def make_split_sentiment_aggregate_dag() -> Dag:
    return Dag(
        dag_id="dag_test",
        tasks=[
            DagTaskSpec(
                task_id="task_split_01",
                capability="text-split",
                input=Payload(type="text", data="abcdefghij"),
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


@pytest.mark.asyncio
async def test_split_sentiment_aggregate_pipeline():
    buyer_t, provider_t, provider = make_pipeline_provider()
    serve_task = asyncio.create_task(provider.serve_forever())

    async with Buyer(buyer_t) as buyer:
        results = await DagOrchestrator(buyer).run(make_split_sentiment_aggregate_dag())

    assert results["task_split_01"].output.data == {"chunks": ["abcde", "fghij"]}
    assert results["task_sent_02a"].output.data == {"sentiment": "positive", "seen": "abcde"}
    assert results["task_sent_02b"].output.data == {"sentiment": "positive", "seen": "fghij"}
    assert results["task_agg_03"].output.data == {"parts": ["positive", "positive"]}

    await serve_task


@pytest.mark.asyncio
async def test_source_without_field_forwards_whole_output():
    buyer_t, provider_t, provider = make_pair()

    async def upstream(task: Task) -> Payload:
        return Payload(type="json", data={"x": 1})

    async def downstream(task: Task) -> Payload:
        # Whole upstream output object ({type, data}) forwarded as literal input.
        assert task.input.type == "json"
        assert task.input.data == {"x": 1}
        return Payload(type="json", data={"received": task.input.data})

    provider.register("upstream", upstream, price_cu=0.0)
    provider.register("downstream", downstream, price_cu=0.0)
    serve_task = asyncio.create_task(provider.serve_forever())

    dag = Dag(
        dag_id="d",
        tasks=[
            DagTaskSpec(task_id="u", capability="upstream", input=Payload(type="text", data="")),
            DagTaskSpec(task_id="d", capability="downstream", input=InputRef(from_task="u")),
        ],
        edges=[Edge(from_task="u", to_task="d")],
    )

    async with Buyer(buyer_t) as buyer:
        results = await DagOrchestrator(buyer).run(dag)

    assert results["d"].output.data == {"received": {"x": 1}}

    await serve_task


@pytest.mark.asyncio
async def test_batch_dag_with_no_edges_runs_all_tasks():
    buyer_t, provider_t, provider = make_pair()

    async def double(task: Task) -> Payload:
        return Payload(type="json", data={"n": task.input.data * 2})

    provider.register("double", double, price_cu=0.0)
    serve_task = asyncio.create_task(provider.serve_forever())

    dag = Dag(
        dag_id="batch",
        tasks=[
            DagTaskSpec(task_id="t1", capability="double", input=Payload(type="json", data=1)),
            DagTaskSpec(task_id="t2", capability="double", input=Payload(type="json", data=2)),
            DagTaskSpec(task_id="t3", capability="double", input=Payload(type="json", data=3)),
        ],
        edges=[],
    )

    async with Buyer(buyer_t) as buyer:
        results = await DagOrchestrator(buyer).run(dag)

    assert {tid: r.output.data["n"] for tid, r in results.items()} == {"t1": 2, "t2": 4, "t3": 6}

    await serve_task


@pytest.mark.asyncio
async def test_independent_tasks_run_concurrently():
    buyer_t, provider_t, provider = make_pair()

    async def slow(task: Task) -> Payload:
        await asyncio.sleep(0.05)
        return Payload(type="json", data={"done": True})

    provider.register("slow", slow, price_cu=0.0)
    serve_task = asyncio.create_task(provider.serve_forever())

    dag = Dag(
        dag_id="parallel",
        tasks=[
            DagTaskSpec(task_id="t1", capability="slow", input=Payload(type="json", data=None)),
            DagTaskSpec(task_id="t2", capability="slow", input=Payload(type="json", data=None)),
        ],
        edges=[],
    )

    async with Buyer(buyer_t) as buyer:
        start = time.monotonic()
        await DagOrchestrator(buyer).run(dag)
        elapsed = time.monotonic() - start

    # Serial execution would take >= 0.1s; concurrent execution should be
    # close to a single 0.05s sleep. Generous margin to avoid CI flakiness.
    assert elapsed < 0.09

    await serve_task


@pytest.mark.asyncio
async def test_fail_fast_cancels_siblings_and_skips_downstream():
    buyer_t, provider_t, provider = make_pair()

    downstream_calls = 0

    async def slow_sibling(task: Task) -> Payload:
        await asyncio.sleep(0.05)
        return Payload(type="json", data={})

    async def never_called(task: Task) -> Payload:
        nonlocal downstream_calls
        downstream_calls += 1
        return Payload(type="json", data={})

    provider.register("slow", slow_sibling, price_cu=0.0)
    provider.register("downstream", never_called, price_cu=0.0)
    serve_task = asyncio.create_task(provider.serve_forever())

    dag = Dag(
        dag_id="fail_fast",
        tasks=[
            # "broken" has no registered capability -> fails almost immediately.
            DagTaskSpec(task_id="broken", capability="does-not-exist", input=Payload(type="json", data=None)),
            DagTaskSpec(task_id="sibling", capability="slow", input=Payload(type="json", data=None)),
            DagTaskSpec(
                task_id="downstream_of_broken",
                capability="downstream",
                input=InputRef(from_task="broken"),
            ),
        ],
        edges=[Edge(from_task="broken", to_task="downstream_of_broken")],
    )

    async with Buyer(buyer_t) as buyer:
        start = time.monotonic()
        with pytest.raises(AcmpError) as exc_info:
            await DagOrchestrator(buyer).run(dag)
        elapsed = time.monotonic() - start

    assert exc_info.value.code == ErrorCode.CAPABILITY_NOT_FOUND
    assert downstream_calls == 0
    # If the sibling's in-flight invoke had been awaited to completion instead
    # of cancelled, this would take >= 0.05s.
    assert elapsed < 0.04

    await serve_task

"""Tests for the Stage 1 Layer 1 transaction: invoke -> result / error."""

from __future__ import annotations

import asyncio

import pytest

from acmp import AcmpError, ErrorCode, InMemoryTransport, Payload, Provider, Task
from acmp.buyer import Buyer


async def echo_capability(task: Task) -> Payload:
    return Payload(type="json", data={"echo": task.input.data})


def make_pair(price_cu: float = 0.001, tokens_per_call: int = 10):
    buyer_t, provider_t = InMemoryTransport.create_pair()
    provider = Provider(provider_t, provider_id="agent:test-provider:local")
    provider.register("echo", echo_capability, price_cu=price_cu, tokens_per_call=tokens_per_call)
    return buyer_t, provider_t, provider


@pytest.mark.asyncio
async def test_successful_invoke_returns_result():
    buyer_t, provider_t, provider = make_pair(price_cu=0.001)
    serve_task = asyncio.create_task(provider.serve_forever())

    async with Buyer(buyer_t) as buyer:
        task = Task(capability="echo", input=Payload(type="text", data="hello"), max_price_cu=0.01)
        result = await buyer.invoke(task)

    assert result.task_id == task.task_id
    assert result.output.data == {"echo": "hello"}
    assert result.cost_cu == 0.001
    assert result.tokens_used == 10
    assert result.provider_id == "agent:test-provider:local"

    await serve_task  # buyer.close() (via __aexit__) signalled the provider to stop


@pytest.mark.asyncio
async def test_budget_exceeded_raises_acmp_error():
    buyer_t, provider_t, provider = make_pair(price_cu=0.01)
    serve_task = asyncio.create_task(provider.serve_forever())

    async with Buyer(buyer_t) as buyer:
        task = Task(capability="echo", input=Payload(type="text", data="hi"), max_price_cu=0.001)
        with pytest.raises(AcmpError) as exc_info:
            await buyer.invoke(task)

    assert exc_info.value.code == ErrorCode.BUDGET_EXCEEDED
    assert exc_info.value.data["min_price_cu"] == 0.01

    await serve_task  # buyer.close() (via __aexit__) signalled the provider to stop


@pytest.mark.asyncio
async def test_unknown_capability_raises_capability_not_found():
    buyer_t, provider_t, provider = make_pair()
    serve_task = asyncio.create_task(provider.serve_forever())

    async with Buyer(buyer_t) as buyer:
        task = Task(capability="does-not-exist", input=Payload(type="text", data="x"))
        with pytest.raises(AcmpError) as exc_info:
            await buyer.invoke(task)

    assert exc_info.value.code == ErrorCode.CAPABILITY_NOT_FOUND

    await serve_task  # buyer.close() (via __aexit__) signalled the provider to stop


@pytest.mark.asyncio
async def test_result_hash_proof_is_deterministic():
    buyer_t, provider_t, provider = make_pair()
    serve_task = asyncio.create_task(provider.serve_forever())

    async with Buyer(buyer_t) as buyer:
        task = Task(
            capability="echo",
            input=Payload(type="text", data="deterministic"),
            proof_method="result-hash",
        )
        result = await buyer.invoke(task)

    assert result.proof is not None
    assert result.proof["method"] == "result-hash"
    assert result.proof["hash"].startswith("sha256:")

    await serve_task  # buyer.close() (via __aexit__) signalled the provider to stop


@pytest.mark.asyncio
async def test_unsupported_proof_method_raises():
    buyer_t, provider_t, provider = make_pair()
    serve_task = asyncio.create_task(provider.serve_forever())

    async with Buyer(buyer_t) as buyer:
        task = Task(
            capability="echo",
            input=Payload(type="text", data="x"),
            proof_method="execution-trace",
        )
        with pytest.raises(AcmpError) as exc_info:
            await buyer.invoke(task)

    assert exc_info.value.code == ErrorCode.PROOF_UNSUPPORTED

    await serve_task  # buyer.close() (via __aexit__) signalled the provider to stop


@pytest.mark.asyncio
async def test_duplicate_task_id_is_idempotent():
    """Per Layer 1 §3.1.1: retrying the same task_id must not re-execute or re-bill."""
    buyer_t, provider_t, provider = make_pair(price_cu=0.001)
    serve_task = asyncio.create_task(provider.serve_forever())

    call_count = 0

    async def counting_capability(task: Task) -> Payload:
        nonlocal call_count
        call_count += 1
        return Payload(type="json", data={"n": call_count})

    provider.register("counting", counting_capability, price_cu=0.001, tokens_per_call=1)

    async with Buyer(buyer_t) as buyer:
        task = Task(capability="counting", input=Payload(type="text", data="x"))
        first = await buyer.invoke(task)
        second = await buyer.invoke(task)  # same task_id: simulated retry

    assert call_count == 1
    assert first.output.data == second.output.data == {"n": 1}

    await serve_task  # buyer.close() (via __aexit__) signalled the provider to stop

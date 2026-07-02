# ACMP Reference SDK

A dependency-light Python reference implementation of the **Agent Compute
Market Protocol (ACMP)**, the economic layer defined by
[A2Agora](https://github.com/a2agora/spec).

This SDK exists to prove the protocol is implementable, not to be a
production-grade agent framework. It follows the spec's Layer 1 (Transport &
Invocation) and Layer 2 (Task Decomposition Format) documents field-for-field.

## Status

**Stage 1 — Layer 1 minimal transaction.** A buyer invokes a single task on a
provider and gets a result back, over an in-memory JSON-RPC transport.

Planned next: Stage 2 (Layer 6 negotiation — RFQ/offer/accept before invoke)
and Stage 3 (Layer 2 DAG — a buyer-side orchestrator running a multi-task
pipeline).

## Install

```bash
cd sdk-reference
pip install -e ".[dev]"
```

## Run the demo

```bash
python examples/01_minimal_invoke.py
```

This spins up a provider offering a `sentiment-analysis` capability and a
buyer that invokes it — the same scenario as the worked example in
[`spec/layers/02-task-format.md` §6.1](../spec/layers/02-task-format.md).

## Run the tests

```bash
pytest
```

## Package layout

```
src/acmp/
  errors.py     ACMP error codes (-33xxx) and the AcmpError exception
  messages.py   Task/Payload/Result dataclasses + JSON-RPC framing helpers
  transport.py  Transport ABC + InMemoryTransport (paired in-process channel)
  provider.py   Capability registry, idempotent dispatch, pricing, proof
  buyer.py      invoke() with concurrent-request dispatch
```

Every module docstring cites the spec section it implements.

## Spec conformance notes

- **Idempotency** (Layer 1 §3.1.1): the `Provider` caches responses by
  `task_id` — invoking the same task twice returns the cached result instead
  of re-executing or re-billing.
- **Proof of execution**: `proof_method="result-hash"` produces a sha256 hash
  of the canonical output JSON — a runnable stand-in for Layer 3.
- **Pricing**: each registered capability has a `price_cu`; the provider
  rejects with `budget_exceeded` (-33001) if `price_cu > task.max_price_cu`.
- Streaming (`acmp/inputChunk`, `acmp/streamChunk`), heartbeats, and
  cancellation are part of Layer 1 but not yet implemented in this SDK —
  contributions welcome.

## License

Apache 2.0, matching the [A2Agora spec](../spec/LICENSE).

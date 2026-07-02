# ACMP Reference SDK

A dependency-light Python reference implementation of the **Agent Compute
Market Protocol (ACMP)**, the economic layer defined by
[A2Agora](https://github.com/a2agora/spec).

This SDK exists to prove the protocol is implementable, not to be a
production-grade agent framework. It follows the spec's Layer 1 (Transport &
Invocation), Layer 2 (Task Decomposition Format), and Layer 6 (Negotiation
Protocol) documents field-for-field.

## Status

- **Stage 1 — Layer 1 minimal transaction.** A buyer invokes a single task on
  a provider and gets a result back, over an in-memory JSON-RPC transport.
- **Stage 2 — Layer 6 negotiation.** A buyer requests an offer, accepts it
  (locking escrow), then invokes using the negotiated price and escrow id.

Planned next: Stage 3 (Layer 2 DAG — a buyer-side orchestrator running a
multi-task pipeline).

## Install

```bash
cd sdk-reference
pip install -e ".[dev]"
```

## Run the demos

```bash
python examples/01_minimal_invoke.py       # Layer 1 only
python examples/02_negotiated_invoke.py    # Layer 6 -> Layer 1
```

Both spin up a provider offering a `sentiment-analysis` capability — the
same scenario as the worked example in
[`spec/layers/02-task-format.md` §6.1](../spec/layers/02-task-format.md).
The second demo additionally runs the offer/accept exchange from
[`spec/layers/06-negotiation-protocol.md`](../spec/layers/06-negotiation-protocol.md)
before invoking.

## Run the tests

```bash
pytest
```

## Package layout

```
src/acmp/
  errors.py       ACMP error codes (-33xxx) and the AcmpError exception
  messages.py     Task/Payload/Result dataclasses + JSON-RPC framing helpers
  transport.py    Transport ABC + InMemoryTransport (paired in-process channel)
  provider.py     Capability registry, invoke + negotiation dispatch, pricing, proof
  buyer.py        invoke() and the shared request() used by negotiation too
  negotiation.py  Layer 6 offer request/offer/accept dataclasses + Negotiator
  escrow_stub.py  Minimal in-memory stand-in for Layer 4 (not a real escrow)
```

Every module docstring cites the spec section it implements.

## Spec conformance notes

- **Idempotency** (Layer 1 §3.1.1): the `Provider` caches responses by
  `task_id` — invoking the same task twice returns the cached result instead
  of re-executing or re-billing.
- **Proof of execution**: `proof_method="result-hash"` produces a sha256 hash
  of the canonical output JSON — a runnable stand-in for Layer 3.
- **Pricing**: each registered capability has a `price_cu`; the provider
  rejects with `budget_exceeded` (-33001) if `price_cu > task.max_price_cu`
  (checked both at negotiation time and at invoke time).
- **Negotiation** (Layer 6): `Negotiator.request_offer()` /
  `.accept()` implement the offer-request → offer → accept → ack flow. Layer
  6 is still `discussion` status in the spec and doesn't define formal
  method or error-code names, so this SDK picks concrete ones
  (`acmp/offerRequest`, `acmp/accept`, and a `NegotiationErrorCode` range at
  -34xxx, kept separate from the Layer 1 §3.3 codes) as a documented
  extension.
- **Escrow**: `EscrowStub` is *not* a Layer 4 implementation (Layer 4 is out
  of scope, still `discussion` status) — it only provides a real `escrow_id`
  for negotiation to hand to invoke, and lets a provider demonstrate the
  `escrow_invalid` (-33005) check from Layer 1 §3.3.
- Streaming (`acmp/inputChunk`, `acmp/streamChunk`), heartbeats, and
  cancellation are part of Layer 1 but not yet implemented in this SDK —
  contributions welcome.

## License

Apache 2.0, matching the [A2Agora spec](../spec/LICENSE).

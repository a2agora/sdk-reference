# ACMP Reference SDK

A dependency-light Python reference implementation of the **Agent Compute
Market Protocol (ACMP)**, the economic layer defined by A2Agora
([GitHub](https://github.com/a2agora/spec) · [Codeberg](https://codeberg.org/a2agora/spec)).

This SDK exists to prove the protocol is implementable, not to be a
production-grade agent framework. It follows the spec's Layer 1 (Transport &
Invocation), Layer 2 (Task Decomposition Format), and Layer 6 (Negotiation
Protocol) documents field-for-field.

## Status

- **Stage 1 — Layer 1 minimal transaction.** A buyer invokes a single task on
  a provider and gets a result back, over an in-memory JSON-RPC transport.
- **Stage 2 — Layer 6 negotiation.** A buyer requests an offer, accepts it
  (locking escrow), then invokes using the negotiated price and escrow id.
- **Stage 3 — Layer 2 DAG.** A buyer-side orchestrator walks a task DAG,
  resolving each task's input from upstream outputs and invoking independent
  tasks concurrently.

This completes the three planned reference stages. Further contributions
(streaming, cancellation, additional transports) are welcome — see the notes
below on what's intentionally out of scope so far.

## Install

```bash
cd sdk-reference
pip install -e ".[dev]"
```

## Run the demos

```bash
python examples/01_minimal_invoke.py       # Layer 1 only
python examples/02_negotiated_invoke.py    # Layer 6 -> Layer 1
python examples/03_dag_pipeline.py         # Layer 2 DAG -> Layer 1 (concurrent)
```

The first two spin up a provider offering a `sentiment-analysis`
capability — the same scenario as the worked example in
`spec/layers/02-task-format.md` §6.1
([GitHub](https://github.com/a2agora/spec/blob/main/layers/02-task-format.md) · [Codeberg](https://codeberg.org/a2agora/spec/src/branch/main/layers/02-task-format.md)).
The second demo additionally runs the offer/accept exchange from
`spec/layers/06-negotiation-protocol.md`
([GitHub](https://github.com/a2agora/spec/blob/main/layers/06-negotiation-protocol.md) · [Codeberg](https://codeberg.org/a2agora/spec/src/branch/main/layers/06-negotiation-protocol.md))
before invoking. The third demo runs the split → parallel sentiment →
aggregate pipeline from `spec/layers/02-task-format.md` §6.2
([GitHub](https://github.com/a2agora/spec/blob/main/layers/02-task-format.md) · [Codeberg](https://codeberg.org/a2agora/spec/src/branch/main/layers/02-task-format.md)).

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
  dag.py          Layer 2 DAG/Edge/InputRef model + DagOrchestrator
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
- **DAG execution** (Layer 2 §3): `DagOrchestrator` treats the DAG purely as
  a buyer-side plan — it is never sent over the wire. Providers only ever
  see individual, literal `acmp/invoke` tasks; `InputRef`s (the `source`
  form) are always resolved to a concrete `Payload` before invoking.
  Independent tasks (no unresolved dependency) are invoked concurrently.
- **Field path resolution** (Layer 2 §3 "Path grammar"): supports the
  spec's small subset — member access (`data.foo`) and array indexing
  (`data.items[0]`), rooted at the referenced task's whole `{type, data}`
  output. Wildcards/slices are out of scope, per the spec.
- **Failure policy** (Layer 2 §4): `DagOrchestrator` is **fail-fast** — the
  first task failure cancels sibling tasks still in flight and re-raises.
  Best-effort (continue independent branches) is a valid alternative per
  the spec but not implemented here.
- Streaming (`acmp/inputChunk`, `acmp/streamChunk`), heartbeats, and
  cancellation (`acmp/cancel`) are part of Layer 1 but not yet implemented
  in this SDK — contributions welcome. As a consequence, `DagOrchestrator`'s
  fail-fast behaviour cancels its own awaiting of in-flight sibling
  invocations but doesn't yet notify their providers to stop processing.

## License

Apache 2.0, matching the A2Agora spec ([GitHub](https://github.com/a2agora/spec/blob/main/LICENSE) · [Codeberg](https://codeberg.org/a2agora/spec/src/branch/main/LICENSE)).

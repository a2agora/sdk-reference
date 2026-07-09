"""Layer 2 DAG: a buyer-side orchestration plan, not a wire format.

Per spec/layers/02-task-format.md §3: "The DAG is an orchestration plan, not
a wire format ... The DAG is held and executed by the buyer's orchestrator,
which walks the graph and emits one Layer 1 acmp/invoke per task." Providers
never see a DAG or an :class:`InputRef` — :class:`DagOrchestrator` always
resolves references into a concrete, literal :class:`~acmp.messages.Task`
before invoking (Layer 2 §3 "Input References").

Scope note: :class:`DagTaskSpec` carries the fields most relevant to proving
the DAG mechanics (capability, input, pricing, proof). It intentionally
leaves out escrow_id/timeout_ms/streaming, which are Stage 1/2 transport
concerns orthogonal to task decomposition — a fuller implementation could
carry all of :class:`~acmp.messages.Task`'s fields per DAG node.
"""

from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass, field
from typing import Any

from .buyer import Buyer
from .messages import Payload, Result, Task, put_if_set

_PATH_TOKEN_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*|\[\d+\]")


class DagValidationError(Exception):
    """The DAG fails structural validation: cycles, dangling references, etc."""


class DagResolutionError(Exception):
    """A ``field`` path could not be resolved against an upstream output."""


@dataclass
class InputRef:
    """A reference to another task's output (Layer 2 §1.1, §3 "Input References").

    Exactly one of ``from_task``/``from_tasks`` is set. ``field`` is a small
    JSONPath-like subset rooted at the referenced task's full ``output``
    object (``{type, data}``) — see spec §3 "Path grammar". If omitted, the
    entire output object is forwarded as-is.
    """

    from_task: str | None = None
    from_tasks: list[str] | None = None
    field: str | None = None

    def __post_init__(self) -> None:
        if (self.from_task is None) == (self.from_tasks is None):
            raise ValueError("InputRef requires exactly one of from_task or from_tasks")

    def to_dict(self) -> dict[str, Any]:
        source: dict[str, Any] = {}
        if self.from_task is not None:
            source["from_task"] = self.from_task
        if self.from_tasks is not None:
            source["from_tasks"] = self.from_tasks
        if self.field is not None:
            source["field"] = self.field
        return {"source": source}

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "InputRef":
        source = d["source"]
        return cls(
            from_task=source.get("from_task"),
            from_tasks=source.get("from_tasks"),
            field=source.get("field"),
        )

    @property
    def referenced_tasks(self) -> list[str]:
        return self.from_tasks if self.from_tasks is not None else [self.from_task]  # type: ignore[list-item]


@dataclass
class DagTaskSpec:
    """A task node within a DAG template (Layer 2 §1, §3).

    ``input`` is either a literal :class:`Payload` (typically for root tasks)
    or an :class:`InputRef` (for tasks depending on another task's output).
    """

    task_id: str
    capability: str
    input: Payload | InputRef
    output_type: str = "json"
    input_tokens_est: int | None = None
    max_price_cu: float | None = None
    preferred_tier: str | None = None
    proof_method: str | None = None
    metadata: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {
            "task_id": self.task_id,
            "capability": self.capability,
            "input": self.input.to_dict(),
            "output_type": self.output_type,
        }
        put_if_set(d, self, "input_tokens_est", "max_price_cu", "preferred_tier", "proof_method", "metadata")
        return d

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "DagTaskSpec":
        raw_input = d["input"]
        parsed_input: Payload | InputRef = (
            InputRef.from_dict(raw_input) if "source" in raw_input else Payload.from_dict(raw_input)
        )
        return cls(
            task_id=d["task_id"],
            capability=d["capability"],
            input=parsed_input,
            output_type=d.get("output_type", "json"),
            input_tokens_est=d.get("input_tokens_est"),
            max_price_cu=d.get("max_price_cu"),
            preferred_tier=d.get("preferred_tier"),
            proof_method=d.get("proof_method"),
            metadata=d.get("metadata"),
        )


@dataclass
class Edge:
    """An unconditional data dependency between two DAG tasks (Layer 2 §3).

    Conditional branching is intentionally not supported — see the Layer 2
    Design Decisions table. ``stream_eligible`` is accepted for schema
    fidelity but not acted on: this SDK doesn't yet implement Layer 1
    streaming, so streamed edges are out of scope for this stage.
    """

    from_task: str
    to_task: str
    stream_eligible: bool = False

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {"from": self.from_task, "to": self.to_task}
        if self.stream_eligible:
            d["stream_eligible"] = True
        return d

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "Edge":
        return cls(from_task=d["from"], to_task=d["to"], stream_eligible=d.get("stream_eligible", False))


@dataclass
class Dag:
    """A buyer-side orchestration plan (Layer 2 §3). Never sent over the wire."""

    dag_id: str
    tasks: list[DagTaskSpec] = field(default_factory=list)
    edges: list[Edge] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "dag_id": self.dag_id,
            "tasks": [t.to_dict() for t in self.tasks],
            "edges": [e.to_dict() for e in self.edges],
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "Dag":
        return cls(
            dag_id=d["dag_id"],
            tasks=[DagTaskSpec.from_dict(t) for t in d["tasks"]],
            edges=[Edge.from_dict(e) for e in d.get("edges", [])],
        )


def validate_dag(dag: Dag) -> None:
    """Raise :class:`DagValidationError` if ``dag`` violates Layer 2 §3 constraints."""
    task_ids = [t.task_id for t in dag.tasks]
    if len(set(task_ids)) != len(task_ids):
        raise DagValidationError(f"duplicate task_id in DAG {dag.dag_id!r}")
    task_id_set = set(task_ids)

    incoming: dict[str, set[str]] = {tid: set() for tid in task_ids}
    outgoing: dict[str, list[str]] = {tid: [] for tid in task_ids}
    for edge in dag.edges:
        for tid in (edge.from_task, edge.to_task):
            if tid not in task_id_set:
                raise DagValidationError(f"edge references unknown task_id: {tid!r}")
        incoming[edge.to_task].add(edge.from_task)
        outgoing[edge.from_task].append(edge.to_task)

    # Cycle detection via DFS with a recursion-stack set.
    visited: set[str] = set()
    stack: set[str] = set()

    def visit(node: str) -> None:
        visited.add(node)
        stack.add(node)
        for nxt in outgoing[node]:
            if nxt in stack:
                raise DagValidationError(f"cycle detected in DAG {dag.dag_id!r} at {nxt!r}")
            if nxt not in visited:
                visit(nxt)
        stack.discard(node)

    for tid in task_ids:
        if tid not in visited:
            visit(tid)

    # Every InputRef must correspond to an actual incoming edge.
    for spec in dag.tasks:
        if isinstance(spec.input, InputRef):
            for ref in spec.input.referenced_tasks:
                if ref not in incoming[spec.task_id]:
                    raise DagValidationError(
                        f"{spec.task_id!r} references {ref!r} without a corresponding edge"
                    )


def _extract_field(root: dict[str, Any], path: str, *, task_id: str) -> Any:
    tokens = _PATH_TOKEN_RE.findall(path)
    if not tokens:
        raise DagResolutionError(f"empty or invalid field path {path!r} for {task_id!r}")
    current: Any = root
    try:
        for token in tokens:
            if token.startswith("["):
                current = current[int(token[1:-1])]
            else:
                current = current[token]
    except (KeyError, IndexError, TypeError) as exc:
        raise DagResolutionError(
            f"could not resolve field {path!r} against output of {task_id!r}: {exc}"
        ) from exc
    return current


def _infer_literal_type(value: Any) -> str:
    """Best-effort ``type`` tag for a value extracted via a ``field`` path.

    The spec doesn't pin this down (it only specifies that the *whole*
    output is passed through unchanged when ``field`` is absent). This is an
    SDK-level convention, not a protocol requirement.
    """
    return "text" if isinstance(value, str) else "json"


def resolve_input(spec_input: Payload | InputRef, completed: dict[str, Result], *, task_id: str) -> Payload:
    """Resolve a DAG task's input into a concrete literal :class:`Payload`."""
    if isinstance(spec_input, Payload):
        return spec_input

    if spec_input.from_tasks is not None:
        outputs = [completed[t].output.to_dict() for t in spec_input.from_tasks]
        return Payload(type="json", data=outputs)

    assert spec_input.from_task is not None  # guaranteed by InputRef.__post_init__
    upstream = completed[spec_input.from_task]
    output_dict = upstream.output.to_dict()
    if spec_input.field is None:
        return Payload(type=output_dict["type"], data=output_dict["data"])

    value = _extract_field(output_dict, spec_input.field, task_id=task_id)
    return Payload(type=_infer_literal_type(value), data=value)


class DagOrchestrator:
    """Executes a :class:`Dag` by walking it and invoking one task at a time.

    Independent tasks (no unresolved dependencies) are invoked concurrently.
    Failure policy is **fail-fast** (Layer 2 §4): the first task failure
    stops scheduling of anything not already in flight, cancels sibling
    tasks still awaiting their ``acmp/invoke`` response, sends
    ``acmp/cancel`` for each of them so their providers stop working too
    (Layer 1 §3.7), and re-raises the original error.
    """

    def __init__(self, buyer: Buyer) -> None:
        self._buyer = buyer

    async def run(self, dag: Dag) -> dict[str, Result]:
        """Execute every task in ``dag`` and return ``task_id -> Result``."""
        validate_dag(dag)

        specs = {t.task_id: t for t in dag.tasks}
        incoming: dict[str, set[str]] = {tid: set() for tid in specs}
        outgoing: dict[str, list[str]] = {tid: [] for tid in specs}
        for edge in dag.edges:
            incoming[edge.to_task].add(edge.from_task)
            outgoing[edge.from_task].append(edge.to_task)

        completed: dict[str, Result] = {}
        pending_deps = {tid: set(deps) for tid, deps in incoming.items()}
        ready = [tid for tid, deps in pending_deps.items() if not deps]
        scheduled: set[str] = set(ready)
        in_flight: dict[asyncio.Task, str] = {}

        async def run_one(task_id: str) -> Result:
            spec = specs[task_id]
            resolved = resolve_input(spec.input, completed, task_id=task_id)
            task = Task(
                task_id=spec.task_id,
                capability=spec.capability,
                input=resolved,
                output_type=spec.output_type,
                input_tokens_est=spec.input_tokens_est,
                max_price_cu=spec.max_price_cu,
                preferred_tier=spec.preferred_tier,
                proof_method=spec.proof_method,
                metadata=spec.metadata,
            )
            return await self._buyer.invoke(task)

        try:
            while ready or in_flight:
                for tid in ready:
                    handle = asyncio.create_task(run_one(tid))
                    in_flight[handle] = tid
                ready = []

                done, _ = await asyncio.wait(in_flight.keys(), return_when=asyncio.FIRST_COMPLETED)

                failure: BaseException | None = None
                for handle in done:
                    finished_id = in_flight.pop(handle)
                    exc = handle.exception()
                    if exc is not None:
                        failure = failure or exc
                        continue
                    completed[finished_id] = handle.result()
                    for dependent in outgoing[finished_id]:
                        pending_deps[dependent].discard(finished_id)
                        if not pending_deps[dependent] and dependent not in scheduled:
                            scheduled.add(dependent)
                            ready.append(dependent)

                if failure is not None:
                    raise failure
        finally:
            if in_flight:
                abandoned = list(in_flight.values())
                for handle in in_flight:
                    handle.cancel()
                await asyncio.gather(*in_flight.keys(), return_exceptions=True)
                # Tell the providers to stop working on the abandoned tasks
                # (best-effort: the transport may already be closing).
                for tid in abandoned:
                    try:
                        await self._buyer.cancel(tid, reason="dag fail-fast")
                    except Exception:  # noqa: BLE001
                        pass

        return completed

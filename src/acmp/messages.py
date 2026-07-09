"""ACMP message data structures and JSON-RPC framing.

Mirrors the schemas in Layer 1 §3 and Layer 2 §1. Uses stdlib dataclasses so
the wire format stays obvious and dependency-free.
"""

from __future__ import annotations

import secrets
from dataclasses import dataclass, field
from typing import Any

JSONRPC_VERSION = "2.0"
ACMP_VERSION = "0.1.0"


def new_task_id() -> str:
    """Generate a globally-unique task id (also the Layer 1 idempotency key)."""
    return f"task_{secrets.token_hex(6)}"


def new_request_id() -> str:
    """Generate a JSON-RPC request id."""
    return f"req_{secrets.token_hex(4)}"


def put_if_set(d: dict[str, Any], obj: Any, *names: str) -> None:
    """Copy each named attribute from ``obj`` into ``d``, skipping ``None`` values.

    Shared by the several dataclasses in this SDK (:class:`Task`,
    :class:`~acmp.negotiation.OfferRequest`,
    :class:`~acmp.dag.DagTaskSpec`) that serialize a set of optional fields
    the same way: include it only if the buyer actually set it.
    """
    for name in names:
        value = getattr(obj, name)
        if value is not None:
            d[name] = value


@dataclass
class Payload:
    """A typed payload: ``{type, data}`` (Layer 2 §1).

    Used for both task input (literal form) and task output.
    """

    type: str
    data: Any

    def to_dict(self) -> dict[str, Any]:
        return {"type": self.type, "data": self.data}

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "Payload":
        return cls(type=d["type"], data=d["data"])


@dataclass
class Task:
    """A single unit of work carried by ``acmp/invoke`` (Layer 2 §1).

    ``input`` MAY be ``None`` when ``input_stream`` is ``True`` — the input
    then arrives incrementally via ``acmp/inputChunk`` (Layer 1 §3.4).
    """

    capability: str
    input: Payload | None = None
    task_id: str = field(default_factory=new_task_id)
    output_type: str = "json"
    input_tokens_est: int | None = None
    max_price_cu: float | None = None
    preferred_tier: str | None = None
    timeout_ms: int = 30000
    stream: bool = False
    input_stream: bool = False
    escrow_id: str | None = None
    proof_method: str | None = None
    metadata: dict[str, Any] | None = None

    def to_params(self) -> dict[str, Any]:
        """Serialize into the ``params`` of an ``acmp/invoke`` request."""
        params: dict[str, Any] = {
            "task_id": self.task_id,
            "capability": self.capability,
            "output_type": self.output_type,
            "timeout_ms": self.timeout_ms,
            "stream": self.stream,
            "input_stream": self.input_stream,
        }
        if self.input is not None:
            params["input"] = self.input.to_dict()
        put_if_set(
            params,
            self,
            "input_tokens_est",
            "max_price_cu",
            "preferred_tier",
            "escrow_id",
            "proof_method",
            "metadata",
        )
        return params

    @classmethod
    def from_params(cls, params: dict[str, Any]) -> "Task":
        raw_input = params.get("input")
        return cls(
            task_id=params["task_id"],
            capability=params["capability"],
            input=Payload.from_dict(raw_input) if raw_input is not None else None,
            output_type=params.get("output_type", "json"),
            input_tokens_est=params.get("input_tokens_est"),
            max_price_cu=params.get("max_price_cu"),
            preferred_tier=params.get("preferred_tier"),
            timeout_ms=params.get("timeout_ms", 30000),
            stream=params.get("stream", False),
            input_stream=params.get("input_stream", False),
            escrow_id=params.get("escrow_id"),
            proof_method=params.get("proof_method"),
            metadata=params.get("metadata"),
        )


@dataclass
class Result:
    """The result of a completed task (Layer 1 §3.2)."""

    task_id: str
    output: Payload | None
    tokens_used: int
    cost_cu: float
    output_streamed: bool = False
    proof: dict[str, Any] | None = None
    provider_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {
            "task_id": self.task_id,
            "tokens_used": self.tokens_used,
            "cost_cu": self.cost_cu,
            "output_streamed": self.output_streamed,
        }
        if self.output is not None:
            d["output"] = self.output.to_dict()
        if self.proof is not None:
            d["proof"] = self.proof
        if self.provider_id is not None:
            d["provider_id"] = self.provider_id
        return d

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "Result":
        output = d.get("output")
        return cls(
            task_id=d["task_id"],
            output=Payload.from_dict(output) if output is not None else None,
            tokens_used=d["tokens_used"],
            cost_cu=d["cost_cu"],
            output_streamed=d.get("output_streamed", False),
            proof=d.get("proof"),
            provider_id=d.get("provider_id"),
        )


# --- JSON-RPC envelope helpers -------------------------------------------------


def make_request(method: str, params: dict[str, Any], req_id: str) -> dict[str, Any]:
    """Build a JSON-RPC 2.0 request object."""
    return {
        "jsonrpc": JSONRPC_VERSION,
        "method": method,
        "id": req_id,
        "params": params,
    }


def make_notification(method: str, params: dict[str, Any]) -> dict[str, Any]:
    """Build a JSON-RPC 2.0 notification (no id, no response expected)."""
    return {"jsonrpc": JSONRPC_VERSION, "method": method, "params": params}


def make_result_response(req_id: str, result: dict[str, Any]) -> dict[str, Any]:
    """Build a JSON-RPC 2.0 success response."""
    return {"jsonrpc": JSONRPC_VERSION, "id": req_id, "result": result}


def make_error_response(req_id: str, error: dict[str, Any]) -> dict[str, Any]:
    """Build a JSON-RPC 2.0 error response."""
    return {"jsonrpc": JSONRPC_VERSION, "id": req_id, "error": error}

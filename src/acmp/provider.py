"""ACMP Provider: serves ``acmp/invoke`` requests over a Transport.

Implements Layer 1 §3.1–§3.3: capability dispatch, idempotent retries via
``task_id``, budget enforcement, and an optional result-hash proof.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from .errors import AcmpError, ErrorCode
from .messages import Payload, Result, Task, make_error_response, make_result_response
from .transport import Transport, TransportClosed

CapabilityHandler = Callable[[Task], Awaitable[Payload]]
"""A handler receives the resolved :class:`Task` and returns the output Payload.

It may raise :class:`AcmpError` to signal a protocol-level failure (e.g. the
task asks for a capability tier the handler can't meet).
"""


@dataclass
class _Capability:
    handler: CapabilityHandler
    price_cu: float
    tokens_per_call: int = 0


def _result_hash(output: Payload) -> str:
    """A minimal Layer 3 proof stub: sha256 over the canonical output JSON."""
    canonical = json.dumps(output.to_dict(), sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    return f"sha256:{digest}"


class Provider:
    """Serves ``acmp/invoke`` requests for a set of registered capabilities."""

    def __init__(self, transport: Transport, provider_id: str) -> None:
        self._transport = transport
        self.provider_id = provider_id
        self._capabilities: dict[str, _Capability] = {}
        # task_id -> {"result": <content>} | {"error": <content>}. Deliberately
        # cached *without* a JSON-RPC id: a retried task_id typically arrives on
        # a new request with a different id, so the envelope must be rebuilt
        # against the id of the request currently being served (see _dispatch).
        self._result_cache: dict[str, dict] = {}

    def register(
        self,
        capability: str,
        handler: CapabilityHandler,
        *,
        price_cu: float,
        tokens_per_call: int = 0,
    ) -> None:
        """Register a handler for a capability tag, with its price in CU."""
        self._capabilities[capability] = _Capability(handler, price_cu, tokens_per_call)

    async def serve_forever(self) -> None:
        """Read requests from the transport until it closes."""
        try:
            while True:
                message = await self._transport.receive()
                await self._dispatch(message)
        except TransportClosed:
            return

    async def _dispatch(self, message: dict) -> None:
        if message.get("method") != "acmp/invoke":
            return  # Stage 1 only handles invoke; later stages add more methods.

        req_id = message["id"]
        task_id = message["params"]["task_id"]

        # Idempotency (Layer 1 §3.1.1): a repeated task_id replays the cached
        # outcome instead of re-executing, but the envelope is rebuilt with
        # *this* request's id — the retry's JSON-RPC id will generally differ
        # from the original request's.
        cached = self._result_cache.get(task_id)
        if cached is not None:
            if "error" in cached:
                await self._transport.send(make_error_response(req_id, cached["error"]))
            else:
                await self._transport.send(make_result_response(req_id, cached["result"]))
            return

        try:
            task = Task.from_params(message["params"])
            content = await self._execute(task)
        except AcmpError as err:
            self._result_cache[task_id] = {"error": err.to_jsonrpc()}
            await self._transport.send(make_error_response(req_id, err.to_jsonrpc()))
            return
        except Exception as exc:  # noqa: BLE001 - convert any bug into -33099
            err = AcmpError(ErrorCode.INTERNAL, str(exc))
            self._result_cache[task_id] = {"error": err.to_jsonrpc()}
            await self._transport.send(make_error_response(req_id, err.to_jsonrpc()))
            return

        self._result_cache[task_id] = {"result": content}
        await self._transport.send(make_result_response(req_id, content))

    async def _execute(self, task: Task) -> dict:
        cap = self._capabilities.get(task.capability)
        if cap is None:
            raise AcmpError(
                ErrorCode.CAPABILITY_NOT_FOUND,
                data={"task_id": task.task_id, "capability": task.capability},
            )

        if task.max_price_cu is not None and cap.price_cu > task.max_price_cu:
            raise AcmpError(
                ErrorCode.BUDGET_EXCEEDED,
                data={
                    "task_id": task.task_id,
                    "min_price_cu": cap.price_cu,
                    "detail": (
                        f"Minimum price for {task.capability} is {cap.price_cu} CU."
                    ),
                },
            )

        output = await cap.handler(task)

        proof = None
        if task.proof_method == "result-hash":
            proof = {"method": "result-hash", "hash": _result_hash(output)}
        elif task.proof_method is not None:
            raise AcmpError(
                ErrorCode.PROOF_UNSUPPORTED,
                data={"task_id": task.task_id, "proof_method": task.proof_method},
            )

        result = Result(
            task_id=task.task_id,
            output=output,
            tokens_used=cap.tokens_per_call,
            cost_cu=cap.price_cu,
            proof=proof,
            provider_id=self.provider_id,
        )
        return result.to_dict()

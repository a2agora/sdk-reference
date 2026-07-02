"""ACMP Provider: serves ``acmp/invoke`` and Layer 6 negotiation requests.

Implements Layer 1 §3.1–§3.3 (capability dispatch, idempotent retries via
``task_id``, budget enforcement, an optional result-hash proof) and the
provider side of Layer 6 negotiation (``acmp/offerRequest``, ``acmp/accept``).
"""

from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from .errors import AcmpError, ErrorCode
from .escrow_stub import EscrowStub, new_escrow_id
from .messages import Payload, Result, Task, make_error_response, make_result_response
from .negotiation import (
    DEFAULT_OFFER_VALID_MS,
    NegotiationErrorCode,
    Offer,
    OfferRequest,
    new_offer_id,
    now_ms,
)
from .transport import Transport, TransportClosed

CapabilityHandler = Callable[[Task], Awaitable[Payload]]
"""A handler receives the resolved :class:`Task` and returns the output Payload.

It may raise :class:`AcmpError` to signal a protocol-level failure (e.g. the
task asks for a capability tier the handler can't meet).
"""

# JSON-RPC's own reserved range (-32xxx); used only for methods this provider
# doesn't recognize at all, not for any ACMP-specific failure.
_METHOD_NOT_FOUND = -32601


@dataclass
class _Capability:
    handler: CapabilityHandler
    price_cu: float
    tokens_per_call: int = 0
    latency_sla_ms: int | None = None


@dataclass
class _OfferRecord:
    capability: str
    price_cu: float
    latency_sla_ms: int | None
    proof_method: str | None
    valid_until_ms: float
    accepted: bool = False


def _result_hash(output: Payload) -> str:
    """A minimal Layer 3 proof stub: sha256 over the canonical output JSON."""
    canonical = json.dumps(output.to_dict(), sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    return f"sha256:{digest}"


class Provider:
    """Serves ``acmp/invoke`` requests for a set of registered capabilities.

    ``escrow`` is optional and only meaningful once negotiation is in use: if
    given, invoked tasks that carry an ``escrow_id`` are checked against it
    (Layer 1 §3.3 ``escrow_invalid``, -33005). If omitted (as in a
    Stage-1-only setup), escrow_id is accepted at face value and not checked.
    """

    def __init__(
        self,
        transport: Transport,
        provider_id: str,
        *,
        escrow: EscrowStub | None = None,
    ) -> None:
        self._transport = transport
        self.provider_id = provider_id
        self._escrow = escrow
        self._capabilities: dict[str, _Capability] = {}
        # task_id -> {"result": <content>} | {"error": <content>}. Deliberately
        # cached *without* a JSON-RPC id: a retried task_id typically arrives on
        # a new request with a different id, so the envelope must be rebuilt
        # against the id of the request currently being served (see _dispatch).
        self._result_cache: dict[str, dict] = {}
        self._offers: dict[str, _OfferRecord] = {}

    def register(
        self,
        capability: str,
        handler: CapabilityHandler,
        *,
        price_cu: float,
        tokens_per_call: int = 0,
        latency_sla_ms: int | None = None,
    ) -> None:
        """Register a handler for a capability tag, with its price in CU."""
        self._capabilities[capability] = _Capability(
            handler, price_cu, tokens_per_call, latency_sla_ms
        )

    async def serve_forever(self) -> None:
        """Read requests from the transport until it closes.

        Each message is dispatched as its own concurrent task rather than
        being awaited inline — otherwise a slow capability handler for one
        request (e.g. one branch of a Layer 2 DAG) would block this provider
        from even *reading* the next incoming request, let alone answering
        it, serializing everything through a single provider.
        """
        in_flight: set[asyncio.Task] = set()
        try:
            while True:
                message = await self._transport.receive()
                task = asyncio.create_task(self._dispatch(message))
                in_flight.add(task)
                task.add_done_callback(in_flight.discard)
        except TransportClosed:
            if in_flight:
                await asyncio.gather(*in_flight, return_exceptions=True)
            return

    async def _dispatch(self, message: dict) -> None:
        method = message.get("method")
        handler = {
            "acmp/invoke": self._handle_invoke,
            "acmp/offerRequest": self._handle_offer_request,
            "acmp/accept": self._handle_accept,
        }.get(method)

        if handler is None:
            # Unknown method: reply with a JSON-RPC error if it was a request
            # (has an id) so the caller doesn't hang waiting for a response
            # that will never come. Silently ignore unknown notifications, for
            # forward compatibility with future notification types.
            if "id" in message:
                await self._transport.send(
                    make_error_response(
                        message["id"],
                        {"code": _METHOD_NOT_FOUND, "message": f"Method not found: {method}"},
                    )
                )
            return

        await handler(message)

    async def _reply_error(self, req_id: str, err: AcmpError) -> None:
        """Send ``err`` as a JSON-RPC error response for ``req_id``.

        Pulled out because every handler below needs to do this identically
        both for expected :class:`AcmpError`\\ s and for unexpected bugs
        (wrapped as ``ErrorCode.INTERNAL``).
        """
        await self._transport.send(make_error_response(req_id, err.to_jsonrpc()))

    # -- acmp/invoke --------------------------------------------------------

    async def _handle_invoke(self, message: dict) -> None:
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
            await self._reply_error(req_id, err)
            return
        except Exception as exc:  # noqa: BLE001 - convert any bug into -33099
            err = AcmpError(ErrorCode.INTERNAL, str(exc))
            self._result_cache[task_id] = {"error": err.to_jsonrpc()}
            await self._reply_error(req_id, err)
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

        if task.escrow_id is not None and self._escrow is not None:
            if not self._escrow.covers(task.escrow_id, cap.price_cu):
                raise AcmpError(
                    ErrorCode.ESCROW_INVALID,
                    data={"task_id": task.task_id, "escrow_id": task.escrow_id},
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

    # -- Layer 6 negotiation --------------------------------------------------

    async def _handle_offer_request(self, message: dict) -> None:
        req_id = message["id"]
        try:
            offer = self._build_offer(OfferRequest.from_params(message["params"]))
        except AcmpError as err:
            await self._reply_error(req_id, err)
            return
        except Exception as exc:  # noqa: BLE001 - see _handle_invoke for rationale
            await self._reply_error(req_id, AcmpError(ErrorCode.INTERNAL, str(exc)))
            return

        self._offers[offer.offer_id] = _OfferRecord(
            capability=offer.capability,
            price_cu=offer.price_cu,
            latency_sla_ms=offer.latency_sla_ms,
            proof_method=offer.proof_method,
            valid_until_ms=offer.valid_until_ms,
        )
        await self._transport.send(make_result_response(req_id, offer.to_dict()))

    def _build_offer(self, offer_request: OfferRequest) -> Offer:
        cap = self._capabilities.get(offer_request.capability)
        if cap is None:
            raise AcmpError(
                ErrorCode.CAPABILITY_NOT_FOUND,
                data={"capability": offer_request.capability},
            )

        if offer_request.max_price_cu is not None and cap.price_cu > offer_request.max_price_cu:
            raise AcmpError(
                ErrorCode.BUDGET_EXCEEDED,
                data={
                    "min_price_cu": cap.price_cu,
                    "detail": (
                        f"Minimum price for {offer_request.capability} is "
                        f"{cap.price_cu} CU."
                    ),
                },
            )

        valid_ms = offer_request.offer_valid_ms or DEFAULT_OFFER_VALID_MS
        return Offer(
            offer_id=new_offer_id(),
            capability=offer_request.capability,
            price_cu=cap.price_cu,
            valid_until_ms=now_ms() + valid_ms,
            latency_sla_ms=cap.latency_sla_ms,
            proof_method=offer_request.proof_method,
        )

    async def _handle_accept(self, message: dict) -> None:
        req_id = message["id"]
        offer_id = message["params"]["offer_id"]
        record = self._offers.get(offer_id)

        try:
            if record is None:
                raise AcmpError(
                    NegotiationErrorCode.OFFER_NOT_FOUND, data={"offer_id": offer_id}
                )
            if record.accepted:
                raise AcmpError(
                    NegotiationErrorCode.ALREADY_ACCEPTED, data={"offer_id": offer_id}
                )
            if now_ms() > record.valid_until_ms:
                raise AcmpError(
                    NegotiationErrorCode.OFFER_EXPIRED, data={"offer_id": offer_id}
                )
        except AcmpError as err:
            await self._reply_error(req_id, err)
            return
        except Exception as exc:  # noqa: BLE001 - see _handle_invoke for rationale
            await self._reply_error(req_id, AcmpError(ErrorCode.INTERNAL, str(exc)))
            return

        record.accepted = True
        escrow_id = (
            self._escrow.lock(record.price_cu) if self._escrow is not None else new_escrow_id()
        )
        ack = {"offer_id": offer_id, "escrow_id": escrow_id, "price_cu": record.price_cu}
        await self._transport.send(make_result_response(req_id, ack))

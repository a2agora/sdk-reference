"""ACMP Provider: serves ``acmp/invoke``, streaming, and negotiation requests.

Implements Layer 1 §3.1–§3.7 (capability dispatch, idempotent retries via
``task_id``, budget enforcement, result-hash proof, output/input streaming,
heartbeats, cancellation) and the provider side of Layer 6 negotiation
(``acmp/offerRequest``, ``acmp/accept``).

Streaming features are capability-gated per Layer 1 §1: a buyer requesting
``stream``/``input_stream`` against a provider that was not constructed with
the matching feature flag is rejected with ``feature_unsupported`` (-33007).
"""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from typing import Any, Union

from .errors import AcmpError, ErrorCode
from .escrow import EscrowVerifier
from .messages import (
    Payload,
    Result,
    Task,
    make_error_response,
    make_notification,
    make_result_response,
)
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
"""Legacy single-argument handler: receives the Task, returns the output."""

StreamingCapabilityHandler = Callable[[Task, "TaskContext"], Awaitable[Payload | None]]
"""Context-aware two-argument handler.

Receives the Task plus a :class:`TaskContext` for streaming output
(``ctx.emit``), consuming streamed input (``ctx.input_chunks``), progress
heartbeats (``ctx.heartbeat``), and cooperative cancellation
(``ctx.cancelled``). A handler that finished its output via
``ctx.emit(..., final=True)`` returns ``None``.
"""

AnyCapabilityHandler = Union[CapabilityHandler, StreamingCapabilityHandler]

# JSON-RPC's own reserved range (-32xxx); used only for methods this provider
# doesn't recognize at all, not for any ACMP-specific failure.
_METHOD_NOT_FOUND = -32601


@dataclass
class _Capability:
    handler: AnyCapabilityHandler
    price_cu: float
    tokens_per_call: int = 0
    latency_sla_ms: int | None = None
    context_aware: bool = False


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


class TaskContext:
    """Per-invocation context handed to context-aware capability handlers.

    Created by the Provider for every invoke; only two-argument handlers
    (see :data:`StreamingCapabilityHandler`) receive it.
    """

    def __init__(self, provider: "Provider", task: Task) -> None:
        self._provider = provider
        self._task = task
        self._out_seq = 0
        self.emitted_chunks = 0
        self.output_final_sent = False
        # Input-side reordering: chunks may arrive out of seq order on a
        # reordering transport; release them to the handler strictly in order.
        self._in_expected = 0
        self._in_buffer: dict[int, tuple[Payload, bool]] = {}
        self._in_queue: asyncio.Queue[tuple[Payload, bool]] = asyncio.Queue()
        self.cancel_event = asyncio.Event()

    @property
    def cancelled(self) -> bool:
        """Whether ``acmp/cancel`` arrived for this task (cooperative check)."""
        return self.cancel_event.is_set()

    async def emit(self, chunk: Payload, *, final: bool = False) -> None:
        """Send one ``acmp/streamChunk`` (Layer 1 §3.5).

        Requires the buyer to have set ``stream: true`` — emitting on a
        non-streaming invoke is a handler bug, reported as -33099.
        """
        if not self._task.stream:
            raise RuntimeError("handler emitted a chunk but the buyer did not set stream=true")
        if self.output_final_sent:
            raise RuntimeError("handler emitted a chunk after final=True")
        await self._provider._transport.send(
            make_notification(
                "acmp/streamChunk",
                {
                    "task_id": self._task.task_id,
                    "seq": self._out_seq,
                    "chunk": chunk.to_dict(),
                    "final": final,
                },
            )
        )
        self._out_seq += 1
        self.emitted_chunks += 1
        if final:
            self.output_final_sent = True

    async def input_chunks(self) -> AsyncIterator[Payload]:
        """Yield streamed input chunks in ``seq`` order until ``final``."""
        while True:
            chunk, final = await self._in_queue.get()
            yield chunk
            if final:
                return

    async def heartbeat(self, progress: float | None = None, detail: str | None = None) -> None:
        """Send an ``acmp/heartbeat`` with optional progress (Layer 1 §3.6)."""
        params: dict[str, Any] = {"task_id": self._task.task_id}
        if progress is not None:
            params["progress"] = progress
        if detail is not None:
            params["detail"] = detail
        await self._provider._transport.send(make_notification("acmp/heartbeat", params))

    def _feed_input(self, seq: int, chunk: Payload, final: bool) -> None:
        self._in_buffer[seq] = (chunk, final)
        while self._in_expected in self._in_buffer:
            item = self._in_buffer.pop(self._in_expected)
            self._in_expected += 1
            self._in_queue.put_nowait(item)


class Provider:
    """Serves ``acmp/invoke`` requests for a set of registered capabilities.

    ``escrow`` is optional and only meaningful once negotiation is in use (see
    Stage 2) — typically an :class:`~acmp.escrow.EscrowClient` connected to
    the Escrow Agent that bound this provider as payee (Layer 4). Any object
    satisfying :class:`~acmp.escrow.EscrowVerifier` works.
    ``output_streaming`` / ``input_streaming`` /
    ``heartbeat_interval_ms`` are the Layer 1 §1 feature advertisements: the
    in-memory SDK has no MCP ``initialize`` handshake, so the flags gate
    enforcement on the provider side (-33007 for non-advertised features).
    When ``heartbeat_interval_ms`` is set, a keep-alive ``acmp/heartbeat`` is
    emitted automatically for every running task at that interval.
    """

    def __init__(
        self,
        transport: Transport,
        provider_id: str,
        *,
        escrow: EscrowVerifier | None = None,
        output_streaming: bool = False,
        input_streaming: bool = False,
        heartbeat_interval_ms: int | None = None,
    ) -> None:
        self._transport = transport
        self.provider_id = provider_id
        self._escrow = escrow
        self.output_streaming = output_streaming
        self.input_streaming = input_streaming
        self.heartbeat_interval_ms = heartbeat_interval_ms
        self._capabilities: dict[str, _Capability] = {}
        # task_id -> {"result": <content>} | {"error": <content>}. Deliberately
        # cached *without* a JSON-RPC id: a retried task_id typically arrives on
        # a new request with a different id, so the envelope must be rebuilt
        # against the id of the request currently being served (see _dispatch).
        self._result_cache: dict[str, dict] = {}
        self._offers: dict[str, _OfferRecord] = {}
        # task_id -> (handler asyncio task, ctx) for cancel/inputChunk routing.
        self._running: dict[str, tuple[asyncio.Task, TaskContext]] = {}
        # Input chunks that raced ahead of their invoke's dispatch. Known
        # limitation: chunks for a task whose invoke never arrives are held
        # forever — a production provider would evict them after a deadline.
        self._pending_input: dict[str, list[tuple[int, Payload, bool]]] = {}

    def register(
        self,
        capability: str,
        handler: AnyCapabilityHandler,
        *,
        price_cu: float,
        tokens_per_call: int = 0,
        latency_sla_ms: int | None = None,
    ) -> None:
        """Register a handler for a capability tag, with its price in CU.

        The handler's arity decides its flavor: one parameter → legacy
        ``handler(task)``; two parameters → context-aware
        ``handler(task, ctx)`` with streaming/heartbeat/cancel support.
        """
        arity = len(inspect.signature(handler).parameters)
        self._capabilities[capability] = _Capability(
            handler, price_cu, tokens_per_call, latency_sla_ms, context_aware=(arity >= 2)
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
            "acmp/cancel": self._handle_cancel,
            "acmp/inputChunk": self._handle_input_chunk,
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

    def _check_features(self, task: Task) -> None:
        """Layer 1 §1: reject features this provider did not advertise."""
        if task.stream and not self.output_streaming:
            raise AcmpError(
                ErrorCode.FEATURE_UNSUPPORTED,
                data={"task_id": task.task_id, "feature": "output_streaming"},
            )
        if task.input_stream and not self.input_streaming:
            raise AcmpError(
                ErrorCode.FEATURE_UNSUPPORTED,
                data={"task_id": task.task_id, "feature": "input_streaming"},
            )
        if task.input is None and not task.input_stream:
            raise AcmpError(
                ErrorCode.INTERNAL,
                "invoke params carry no input and input_stream is false",
                data={"task_id": task.task_id},
            )

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
            if not await self._escrow.covers(task.escrow_id, cap.price_cu):
                raise AcmpError(
                    ErrorCode.ESCROW_INVALID,
                    data={"task_id": task.task_id, "escrow_id": task.escrow_id},
                )

        self._check_features(task)

        ctx = TaskContext(self, task)
        # Adopt input chunks that raced ahead of this dispatch. No await may
        # occur between this adoption and the _running registration below —
        # the single-threaded event loop then guarantees no chunk is lost.
        for seq, chunk, final in self._pending_input.pop(task.task_id, []):
            ctx._feed_input(seq, chunk, final)

        handler_coro = (
            cap.handler(task, ctx) if cap.context_aware else cap.handler(task)  # type: ignore[call-arg]
        )
        handler_task: asyncio.Task = asyncio.create_task(handler_coro)
        self._running[task.task_id] = (handler_task, ctx)

        keepalive: asyncio.Task | None = None
        if self.heartbeat_interval_ms is not None:
            keepalive = asyncio.create_task(self._keepalive_loop(task.task_id))

        try:
            output = await handler_task
        except asyncio.CancelledError:
            # The handler task was cancelled via acmp/cancel — the dispatch
            # task itself was not, so no re-raise is needed (Layer 1 §3.7).
            raise AcmpError(ErrorCode.CANCELLED, data={"task_id": task.task_id}) from None
        finally:
            self._running.pop(task.task_id, None)
            if keepalive is not None:
                keepalive.cancel()

        if ctx.output_final_sent:
            if output is not None:
                raise RuntimeError("handler returned an output after emitting final=True")
            if task.proof_method is not None:
                raise AcmpError(
                    ErrorCode.PROOF_UNSUPPORTED,
                    data={
                        "task_id": task.task_id,
                        "detail": "proof over streamed output is not implemented in this SDK",
                    },
                )
            result = Result(
                task_id=task.task_id,
                output=None,
                output_streamed=True,
                tokens_used=cap.tokens_per_call,
                cost_cu=cap.price_cu,
                provider_id=self.provider_id,
            )
            return result.to_dict()

        if ctx.emitted_chunks > 0:
            raise RuntimeError("handler emitted chunks but never sent final=True")
        if output is None:
            raise RuntimeError("handler returned no output and streamed none")

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

    async def _keepalive_loop(self, task_id: str) -> None:
        """Automatic bare heartbeats at the advertised interval (Layer 1 §3.6)."""
        assert self.heartbeat_interval_ms is not None
        try:
            while True:
                await asyncio.sleep(self.heartbeat_interval_ms / 1000)
                await self._transport.send(
                    make_notification("acmp/heartbeat", {"task_id": task_id})
                )
        except asyncio.CancelledError:
            pass

    # -- streaming & cancellation notifications ------------------------------

    async def _handle_cancel(self, message: dict) -> None:
        """Layer 1 §3.7: stop work; the invoke answers with -33004."""
        task_id = message["params"]["task_id"]
        entry = self._running.get(task_id)
        if entry is None:
            return  # unknown or already finished — notifications are best-effort
        handler_task, ctx = entry
        ctx.cancel_event.set()
        handler_task.cancel()

    async def _handle_input_chunk(self, message: dict) -> None:
        """Layer 1 §3.4: route streamed input to the running task's context."""
        params = message["params"]
        task_id = params["task_id"]
        chunk = Payload.from_dict(params["chunk"])
        seq = params["seq"]
        final = params.get("final", False)
        entry = self._running.get(task_id)
        if entry is not None:
            entry[1]._feed_input(seq, chunk, final)
        else:
            # The chunk raced ahead of the invoke's dispatch — hold it.
            self._pending_input.setdefault(task_id, []).append((seq, chunk, final))

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

        # Layer 6 §2.1: a provider MUST reject a quote for a proof method it
        # cannot deliver (-33006) rather than promise it and fail at invoke.
        if offer_request.proof_method not in (None, "result-hash"):
            raise AcmpError(
                ErrorCode.PROOF_UNSUPPORTED,
                data={"proof_method": offer_request.proof_method},
            )

        valid_ms = offer_request.offer_valid_ms or DEFAULT_OFFER_VALID_MS
        return Offer(
            offer_id=new_offer_id(),
            capability=offer_request.capability,
            price_cu=cap.price_cu,
            valid_until_ms=now_ms() + valid_ms,
            latency_sla_ms=cap.latency_sla_ms,
            proof_method=offer_request.proof_method,
            # Confirm the buyer's proposed challenge window unchanged; a real
            # provider MAY adjust it here (Layer 6 §2.1).
            challenge_window_ms=offer_request.challenge_window_ms,
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
        # Layer 6 §2.2: the *buyer* locked the escrow (Layer 4) and supplies
        # its id at accept; the ack merely echoes it. Absence signals
        # escrow-less direct settlement.
        escrow_id = message["params"].get("escrow_id")
        ack: dict = {"offer_id": offer_id, "price_cu": record.price_cu}
        if escrow_id is not None:
            ack["escrow_id"] = escrow_id
        await self._transport.send(make_result_response(req_id, ack))

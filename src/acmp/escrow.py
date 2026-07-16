"""Layer 4 (Escrow & Settlement): escrow data model and the credit-ledger rail.

Implements the escrow lifecycle of spec/layers/04-escrow-settlement.md: the
four-state machine (§2), op_ref idempotency (§3), the seven acmp/escrow*
messages (§4), the -35xxx error table (§5), atomicity (§6), and the plain
credit ledger — the always-implementable non-blockchain rail (§7.2, RFC-0001
principle P4).

The :class:`EscrowAgent` here is the *neutral third party* of Layer 4 §1: it
serves buyer and provider over separate ACMP connections rather than being
shared in-process (which is what the retired ``EscrowStub`` did).
"""

from __future__ import annotations

import secrets
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Protocol

from .buyer import Buyer
from .errors import AcmpError, EscrowErrorCode
from .messages import make_error_response, make_result_response
from .transport import Transport, TransportClosed

# JSON-RPC's own reserved code for unrecognized methods (same use as in
# provider.py — never for an ACMP-specific failure).
_METHOD_NOT_FOUND = -32601


def new_escrow_id() -> str:
    return f"esc_{secrets.token_hex(6)}"


def new_op_ref() -> str:
    """Generate a caller-side idempotency key for a mutating escrow op (§3)."""
    return f"op_{secrets.token_hex(6)}"


DEFAULT_CHALLENGE_WINDOW_MS = 86_400_000
"""Agent-policy default challenge window: 24 h (Layer 4 §4.1 RECOMMENDED)."""


class EscrowState(str, Enum):
    """The four escrow states of Layer 4 §2."""

    OPEN = "open"
    CLAIMED = "claimed"
    DISPUTED = "disputed"
    CLOSED = "closed"


@dataclass
class Claim:
    """A pending provider claim (§4.5): challenge window running."""

    amount_cu: float
    task_id: str
    proof: dict[str, Any]
    window_ends_ms: int

    def to_dict(self) -> dict[str, Any]:
        return {"amount_cu": self.amount_cu, "window_ends_ms": self.window_ends_ms}


@dataclass
class Dispute:
    """A buyer's contest of a claim (§4.6): escrow frozen until resolution."""

    reason: str
    evidence: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {"reason": self.reason}
        if self.evidence is not None:
            d["evidence"] = self.evidence
        return d


@dataclass
class Escrow:
    """One escrow's full state, tracked by the agent (§2, §4.7)."""

    escrow_id: str
    buyer_id: str
    locked_cu: float
    valid_until_ms: int
    challenge_window_ms: int
    state: EscrowState = EscrowState.OPEN
    released_cu: float = 0.0
    reclaimed_cu: float = 0.0
    payee_id: str | None = None
    claim: Claim | None = None
    dispute: Dispute | None = None
    had_settlement: bool = False
    """Whether at least one release or resolved claim happened — gates the
    bound-escrow reclaim guard (§4.4)."""
    expired: bool = False
    """Closed by expiry auto-reclaim: further mutations answer -35004."""
    op_results: dict[str, dict[str, Any]] = field(default_factory=dict)
    """op_ref -> first outcome (result or error), replayed on retry (§3)."""

    @property
    def remaining_cu(self) -> float:
        return self.locked_cu - self.released_cu - self.reclaimed_cu


class CreditLedger:
    """The §7.2 non-blockchain rail: a plain credit ledger at the agent.

    Funding debits the buyer's account balance, payouts credit the payee's
    (releases) or the buyer's (reclaims). Payouts are idempotent per
    ``(escrow_id, transition)`` and must be invoked strictly *after* the
    corresponding state transition is recorded (§6, §7.1).
    """

    def __init__(self) -> None:
        self._balances: dict[str, float] = {}
        self._paid: set[tuple[str, str]] = set()

    def balance(self, account_id: str) -> float:
        return self._balances.get(account_id, 0.0)

    def credit(self, account_id: str, amount_cu: float) -> None:
        self._balances[account_id] = self.balance(account_id) + amount_cu

    def debit(self, account_id: str, amount_cu: float) -> None:
        """Take ``amount_cu`` from the account; -35002 if it isn't covered."""
        if self.balance(account_id) < amount_cu:
            raise AcmpError(
                EscrowErrorCode.INSUFFICIENT_FUNDS,
                data={"account_id": account_id, "amount_cu": amount_cu},
            )
        self._balances[account_id] -= amount_cu

    def payout(
        self, escrow_id: str, transition: str, account_id: str, amount_cu: float
    ) -> bool:
        """Disburse once per ``(escrow_id, transition)`` (§7.1).

        Returns ``False`` (and moves no value) when this transition was
        already paid out — the guard against a retried operation paying twice.
        """
        key = (escrow_id, transition)
        if key in self._paid:
            return False
        self._paid.add(key)
        self.credit(account_id, amount_cu)
        return True


class EscrowVerifier(Protocol):
    """What :class:`~acmp.provider.Provider` needs from an escrow party to
    check ``escrow_invalid`` (Layer 1 §3.3, -33005) before running a task."""

    async def covers(self, escrow_id: str, amount_cu: float) -> bool: ...


class EscrowAgent:
    """The Layer 4 §1 neutral third party.

    Serves ``acmp/escrow*`` over any number of :class:`Transport`
    connections (typically one for the buyer, one for the provider), sharing
    one pool of :class:`Escrow` state and one :class:`CreditLedger`. This is
    the piece the retired ``EscrowStub`` explicitly was not: a party
    genuinely separate from both buyer and provider, reachable only over the
    wire.
    """

    def __init__(
        self,
        ledger: CreditLedger | None = None,
        *,
        now_ms: Callable[[], int] = lambda: int(time.time() * 1000),
        default_challenge_window_ms: int = DEFAULT_CHALLENGE_WINDOW_MS,
    ) -> None:
        self._ledger = ledger if ledger is not None else CreditLedger()
        self._now_ms = now_ms
        self._default_challenge_window_ms = default_challenge_window_ms
        self._escrows: dict[str, Escrow] = {}
        self._lock_ops: dict[str, dict[str, Any]] = {}
        """op_ref -> cached escrowLock outcome, keyed agent-wide rather than
        per-escrow: a lock retry has no escrow_id to key off yet, so without
        this a retried escrowLock would open a second escrow and double-debit
        the buyer (§3 applies to escrowLock too, even though its wording
        talks about "the same escrow")."""

    @property
    def ledger(self) -> CreditLedger:
        return self._ledger

    def escrow(self, escrow_id: str) -> Escrow:
        """Look up an escrow by id, resolving pending auto-transitions first.

        Exposed for tests and for the demo's clock-advancing scenarios; not
        part of the wire protocol.
        """
        esc = self._get_escrow(escrow_id)
        self._effective_state(esc)
        return esc

    async def serve(self, transport: Transport, party_id: str) -> None:
        """Read requests from one connection until it closes.

        ``party_id`` is this connection's self-reported identity — verifiable
        binding is Layer 7's job (§1: "``payee_id`` itself is self-reported at
        this layer"). Messages are handled sequentially rather than as
        concurrent tasks (unlike :meth:`Provider.serve_forever`): escrow
        handlers never block on external work, so there is nothing to gain
        from overlapping them, and sequential handling keeps ``op_ref``
        replay and state transitions race-free without a lock.
        """
        try:
            while True:
                message = await transport.receive()
                await self._dispatch(transport, party_id, message)
        except TransportClosed:
            return

    async def _dispatch(self, transport: Transport, party_id: str, message: dict) -> None:
        method = message.get("method")
        handler = {
            "acmp/escrowLock": self._handle_lock,
            "acmp/escrowBind": self._handle_bind,
            "acmp/escrowRelease": self._handle_release,
            "acmp/escrowReclaim": self._handle_reclaim,
            "acmp/escrowClaim": self._handle_claim,
            "acmp/escrowDispute": self._handle_dispute,
            "acmp/escrowStatus": self._handle_status,
        }.get(method)

        if handler is None:
            if "id" in message:
                await transport.send(
                    make_error_response(
                        message["id"],
                        {"code": _METHOD_NOT_FOUND, "message": f"Method not found: {method}"},
                    )
                )
            return

        req_id = message["id"]
        try:
            result = handler(party_id, message.get("params", {}))
        except AcmpError as err:
            await transport.send(make_error_response(req_id, err.to_jsonrpc()))
            return
        except Exception as exc:  # noqa: BLE001 - convert any bug into -35099
            err = AcmpError(EscrowErrorCode.INTERNAL, str(exc))
            await transport.send(make_error_response(req_id, err.to_jsonrpc()))
            return
        await transport.send(make_result_response(req_id, result))

    # -- lookups & guards -----------------------------------------------------

    def _get_escrow(self, escrow_id: str) -> Escrow:
        esc = self._escrows.get(escrow_id)
        if esc is None:
            raise AcmpError(EscrowErrorCode.ESCROW_NOT_FOUND, data={"escrow_id": escrow_id})
        return esc

    def _require_buyer(self, esc: Escrow, party_id: str) -> None:
        if party_id != esc.buyer_id:
            raise AcmpError(EscrowErrorCode.NOT_AUTHORIZED, data={"escrow_id": esc.escrow_id})

    def _require_payee(self, esc: Escrow, party_id: str) -> None:
        if esc.payee_id is None or party_id != esc.payee_id:
            raise AcmpError(EscrowErrorCode.NOT_AUTHORIZED, data={"escrow_id": esc.escrow_id})

    def _require_party(self, esc: Escrow, party_id: str) -> None:
        """§4.7: ``escrowStatus`` is callable by the buyer or the bound payee."""
        if party_id != esc.buyer_id and party_id != esc.payee_id:
            raise AcmpError(EscrowErrorCode.NOT_AUTHORIZED, data={"escrow_id": esc.escrow_id})

    def _run_cached(
        self, esc: Escrow, op_ref: str, op: Callable[[], dict[str, Any]]
    ) -> dict[str, Any]:
        """Idempotency for every escrow-scoped mutating op (§3): a repeated
        ``op_ref`` replays the first outcome — success *or* error — instead
        of re-running ``op``."""
        cached = esc.op_results.get(op_ref)
        if cached is not None:
            if "error" in cached:
                err = cached["error"]
                raise AcmpError(err["code"], err["message"], err.get("data"))
            return cached["result"]
        try:
            result = op()
        except AcmpError as err:
            esc.op_results[op_ref] = {"error": err.to_jsonrpc()}
            raise
        esc.op_results[op_ref] = {"result": result}
        return result

    # -- lazy time evaluation (§2 "Expiry", §4.5 auto-release) -----------------

    def _effective_state(self, esc: Escrow) -> None:
        """Resolve auto-release and expiry against the injected clock before
        any handler reads or mutates ``esc``.

        A pending claim or dispute suspends expiry (§2) — a provider who
        claimed in time cannot lose payment to the clock. An unchallenged
        claim's window elapsing (§4.5) may return the escrow to ``open``,
        which is then immediately eligible for expiry in the same call.
        """
        if esc.state is EscrowState.CLOSED:
            return
        now = self._now_ms()

        if esc.state is EscrowState.CLAIMED and esc.claim is not None:
            if now >= esc.claim.window_ends_ms:
                self._auto_release(esc)

        if esc.state is EscrowState.OPEN and esc.claim is None and esc.dispute is None:
            if now >= esc.valid_until_ms:
                self._auto_reclaim_expiry(esc)

    def _auto_release(self, esc: Escrow) -> None:
        """§4.5: unchallenged claim window elapsed — release the claimed
        amount to the payee; any remainder returns to ``open``, still
        reclaimable."""
        claim = esc.claim
        assert claim is not None
        assert esc.payee_id is not None  # claiming requires a bound payee
        esc.released_cu += claim.amount_cu
        esc.had_settlement = True
        esc.claim = None
        esc.state = EscrowState.CLOSED if esc.remaining_cu <= 0 else EscrowState.OPEN
        self._ledger.payout(
            esc.escrow_id, f"auto_release:{claim.task_id}", esc.payee_id, claim.amount_cu
        )

    def _auto_reclaim_expiry(self, esc: Escrow) -> None:
        """§2 "Expiry": the lock passed ``valid_until_ms`` while ``open`` —
        reclaim the remainder to the buyer and close."""
        amount = esc.remaining_cu
        esc.reclaimed_cu += amount
        esc.state = EscrowState.CLOSED
        esc.expired = True
        if amount > 0:
            self._ledger.payout(esc.escrow_id, "expiry_reclaim", esc.buyer_id, amount)

    # -- acmp/escrowLock --------------------------------------------------------

    def _handle_lock(self, party_id: str, params: dict[str, Any]) -> dict[str, Any]:
        op_ref = params["op_ref"]
        cached = self._lock_ops.get(op_ref)
        if cached is not None:
            if "error" in cached:
                err = cached["error"]
                raise AcmpError(err["code"], err["message"], err.get("data"))
            return cached["result"]
        try:
            result = self._do_lock(party_id, params)
        except AcmpError as err:
            self._lock_ops[op_ref] = {"error": err.to_jsonrpc()}
            raise
        self._lock_ops[op_ref] = {"result": result}
        return result

    def _do_lock(self, party_id: str, params: dict[str, Any]) -> dict[str, Any]:
        amount_cu = params["amount_cu"]
        valid_until_ms = params["valid_until_ms"]
        challenge_window_ms = params.get("challenge_window_ms")
        if challenge_window_ms is None:
            challenge_window_ms = self._default_challenge_window_ms

        self._ledger.debit(party_id, amount_cu)  # raises -35002 if uncovered

        escrow_id = new_escrow_id()
        esc = Escrow(
            escrow_id=escrow_id,
            buyer_id=party_id,
            locked_cu=amount_cu,
            valid_until_ms=valid_until_ms,
            challenge_window_ms=challenge_window_ms,
            payee_id=params.get("payee_id"),
        )
        self._escrows[escrow_id] = esc
        return {
            "escrow_id": escrow_id,
            "state": esc.state.value,
            "amount_cu": amount_cu,
            "valid_until_ms": valid_until_ms,
        }

    # -- acmp/escrowBind ----------------------------------------------------

    def _handle_bind(self, party_id: str, params: dict[str, Any]) -> dict[str, Any]:
        escrow_id = params["escrow_id"]
        esc = self._get_escrow(escrow_id)
        op_ref = params["op_ref"]

        def op() -> dict[str, Any]:
            self._effective_state(esc)
            if esc.expired:
                raise AcmpError(EscrowErrorCode.ESCROW_EXPIRED, data={"escrow_id": escrow_id})
            self._require_buyer(esc, party_id)
            if esc.payee_id is not None:
                # Rebinding MUST be rejected (§4.2) — including a bind that
                # merely repeats the already-bound payee_id.
                raise AcmpError(
                    EscrowErrorCode.INVALID_STATE,
                    data={"escrow_id": escrow_id, "detail": "escrow is already bound"},
                )
            payee_id = params["payee_id"]
            esc.payee_id = payee_id
            new_window = params.get("challenge_window_ms")
            if new_window is not None:
                esc.challenge_window_ms = new_window
            return {"escrow_id": escrow_id, "payee_id": payee_id}

        return self._run_cached(esc, op_ref, op)

    # -- acmp/escrowRelease ---------------------------------------------------

    def _handle_release(self, party_id: str, params: dict[str, Any]) -> dict[str, Any]:
        escrow_id = params["escrow_id"]
        esc = self._get_escrow(escrow_id)
        op_ref = params["op_ref"]

        def op() -> dict[str, Any]:
            self._effective_state(esc)
            if esc.expired:
                raise AcmpError(EscrowErrorCode.ESCROW_EXPIRED, data={"escrow_id": escrow_id})
            self._require_buyer(esc, party_id)

            if esc.state not in (EscrowState.OPEN, EscrowState.CLAIMED):
                raise AcmpError(
                    EscrowErrorCode.INVALID_STATE,
                    data={"escrow_id": escrow_id, "state": esc.state.value},
                )

            amount_cu = params["amount_cu"]
            payee_id = params["payee_id"]
            resolves_claim = esc.state is EscrowState.CLAIMED

            if resolves_claim:
                assert esc.claim is not None
                if amount_cu < esc.claim.amount_cu:
                    # §4.3: a buyer who believes less is owed must dispute,
                    # not undercut a pending claim with a smaller release.
                    raise AcmpError(
                        EscrowErrorCode.INVALID_STATE,
                        data={
                            "escrow_id": escrow_id,
                            "detail": "release below pending claim amount; dispute instead",
                        },
                    )

            # §4.3: payee_id MUST equal the bound payee; an unbound escrow is
            # implicitly bound by this release.
            if esc.payee_id is not None and payee_id != esc.payee_id:
                raise AcmpError(EscrowErrorCode.NOT_AUTHORIZED, data={"escrow_id": escrow_id})
            if amount_cu > esc.remaining_cu:
                raise AcmpError(
                    EscrowErrorCode.AMOUNT_EXCEEDS_REMAINING, data={"escrow_id": escrow_id}
                )

            # All checks passed — mutate.
            if esc.payee_id is None:
                esc.payee_id = payee_id
            if resolves_claim:
                esc.claim = None  # fast-forward: this release resolves the claim
            esc.released_cu += amount_cu
            esc.had_settlement = True
            esc.state = EscrowState.CLOSED if esc.remaining_cu <= 0 else EscrowState.OPEN
            self._ledger.payout(escrow_id, f"release:{op_ref}", payee_id, amount_cu)

            return {
                "escrow_id": escrow_id,
                "released_cu": amount_cu,
                "remaining_cu": esc.remaining_cu,
                "state": esc.state.value,
            }

        return self._run_cached(esc, op_ref, op)

    # -- acmp/escrowReclaim ---------------------------------------------------

    def _handle_reclaim(self, party_id: str, params: dict[str, Any]) -> dict[str, Any]:
        escrow_id = params["escrow_id"]
        esc = self._get_escrow(escrow_id)
        op_ref = params["op_ref"]

        def op() -> dict[str, Any]:
            self._effective_state(esc)
            if esc.expired:
                raise AcmpError(EscrowErrorCode.ESCROW_EXPIRED, data={"escrow_id": escrow_id})
            self._require_buyer(esc, party_id)

            if esc.state is not EscrowState.OPEN:
                # A pending claim (or dispute) blocks reclaim — a provider's
                # in-flight claim cannot be undercut (§4.4).
                raise AcmpError(
                    EscrowErrorCode.INVALID_STATE,
                    data={"escrow_id": escrow_id, "state": esc.state.value},
                )
            if esc.payee_id is not None and not esc.had_settlement:
                # Bound-escrow guard (§4.4): without it a buyer could drain
                # the escrow mid-task and beat the provider's claim to the
                # punch. Unbound escrows are reclaimable freely.
                raise AcmpError(
                    EscrowErrorCode.INVALID_STATE,
                    data={
                        "escrow_id": escrow_id,
                        "detail": "bound escrow requires a settlement before reclaim",
                    },
                )

            amount_cu = params.get("amount_cu")
            if amount_cu is None:
                amount_cu = esc.remaining_cu
            elif amount_cu > esc.remaining_cu:
                raise AcmpError(
                    EscrowErrorCode.AMOUNT_EXCEEDS_REMAINING, data={"escrow_id": escrow_id}
                )

            esc.reclaimed_cu += amount_cu
            esc.state = EscrowState.CLOSED if esc.remaining_cu <= 0 else EscrowState.OPEN
            if amount_cu > 0:
                self._ledger.payout(escrow_id, f"reclaim:{op_ref}", esc.buyer_id, amount_cu)

            return {
                "escrow_id": escrow_id,
                "reclaimed_cu": amount_cu,
                "remaining_cu": esc.remaining_cu,
                "state": esc.state.value,
            }

        return self._run_cached(esc, op_ref, op)

    # -- acmp/escrowClaim -------------------------------------------------------

    def _handle_claim(self, party_id: str, params: dict[str, Any]) -> dict[str, Any]:
        escrow_id = params["escrow_id"]
        esc = self._get_escrow(escrow_id)
        op_ref = params["op_ref"]

        def op() -> dict[str, Any]:
            self._effective_state(esc)
            if esc.expired:
                raise AcmpError(EscrowErrorCode.ESCROW_EXPIRED, data={"escrow_id": escrow_id})
            # §4.5: MUST be bound, and the caller MUST be the bound payee.
            self._require_payee(esc, party_id)

            if esc.state is not EscrowState.OPEN:
                # Also covers "one claim may be pending at a time" — once
                # claimed, the state is no longer open.
                raise AcmpError(
                    EscrowErrorCode.INVALID_STATE,
                    data={"escrow_id": escrow_id, "state": esc.state.value},
                )

            amount_cu = params["amount_cu"]
            if amount_cu > esc.remaining_cu:
                raise AcmpError(
                    EscrowErrorCode.AMOUNT_EXCEEDS_REMAINING, data={"escrow_id": escrow_id}
                )

            esc.claim = Claim(
                amount_cu=amount_cu,
                task_id=params["task_id"],
                proof=params["proof"],
                window_ends_ms=self._now_ms() + esc.challenge_window_ms,
            )
            esc.state = EscrowState.CLAIMED
            return {
                "escrow_id": escrow_id,
                "state": esc.state.value,
                "claim": esc.claim.to_dict(),
            }

        return self._run_cached(esc, op_ref, op)

    # -- acmp/escrowDispute -----------------------------------------------------

    def _handle_dispute(self, party_id: str, params: dict[str, Any]) -> dict[str, Any]:
        escrow_id = params["escrow_id"]
        esc = self._get_escrow(escrow_id)
        op_ref = params["op_ref"]

        def op() -> dict[str, Any]:
            self._effective_state(esc)
            if esc.expired:
                raise AcmpError(EscrowErrorCode.ESCROW_EXPIRED, data={"escrow_id": escrow_id})
            self._require_buyer(esc, party_id)

            if esc.state is not EscrowState.CLAIMED:
                # Includes the case where the challenge window already
                # elapsed: _effective_state above auto-released it before
                # this check runs, so a late dispute correctly lands here.
                raise AcmpError(
                    EscrowErrorCode.INVALID_STATE,
                    data={"escrow_id": escrow_id, "state": esc.state.value},
                )

            esc.dispute = Dispute(reason=params["reason"], evidence=params.get("evidence"))
            esc.state = EscrowState.DISPUTED
            return {"escrow_id": escrow_id, "state": esc.state.value}

        return self._run_cached(esc, op_ref, op)

    # -- acmp/escrowStatus ----------------------------------------------------

    def _handle_status(self, party_id: str, params: dict[str, Any]) -> dict[str, Any]:
        esc = self._get_escrow(params["escrow_id"])
        self._effective_state(esc)
        self._require_party(esc, party_id)
        return {
            "escrow_id": esc.escrow_id,
            "state": esc.state.value,
            "locked_cu": esc.locked_cu,
            "released_cu": esc.released_cu,
            "reclaimed_cu": esc.reclaimed_cu,
            "remaining_cu": esc.remaining_cu,
            "payee_id": esc.payee_id,
            "valid_until_ms": esc.valid_until_ms,
            "claim": esc.claim.to_dict() if esc.claim is not None else None,
            "dispute": esc.dispute.to_dict() if esc.dispute is not None else None,
        }


@dataclass
class LockResult:
    """The result of ``acmp/escrowLock`` (§4.1)."""

    escrow_id: str
    state: str
    amount_cu: float
    valid_until_ms: int

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "LockResult":
        return cls(
            escrow_id=d["escrow_id"],
            state=d["state"],
            amount_cu=d["amount_cu"],
            valid_until_ms=d["valid_until_ms"],
        )


@dataclass
class ReleaseResult:
    """The result of ``acmp/escrowRelease`` (§4.3)."""

    escrow_id: str
    released_cu: float
    remaining_cu: float
    state: str

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "ReleaseResult":
        return cls(
            escrow_id=d["escrow_id"],
            released_cu=d["released_cu"],
            remaining_cu=d["remaining_cu"],
            state=d["state"],
        )


@dataclass
class ReclaimResult:
    """The result of ``acmp/escrowReclaim`` (§4.4)."""

    escrow_id: str
    reclaimed_cu: float
    remaining_cu: float
    state: str

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "ReclaimResult":
        return cls(
            escrow_id=d["escrow_id"],
            reclaimed_cu=d["reclaimed_cu"],
            remaining_cu=d["remaining_cu"],
            state=d["state"],
        )


@dataclass
class ClaimResult:
    """The result of ``acmp/escrowClaim`` (§4.5)."""

    escrow_id: str
    state: str
    claim: dict[str, Any]

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "ClaimResult":
        return cls(escrow_id=d["escrow_id"], state=d["state"], claim=d["claim"])


@dataclass
class StatusResult:
    """The result of ``acmp/escrowStatus`` (§4.7)."""

    escrow_id: str
    state: str
    locked_cu: float
    released_cu: float
    reclaimed_cu: float
    remaining_cu: float
    payee_id: str | None
    valid_until_ms: int
    claim: dict[str, Any] | None
    dispute: dict[str, Any] | None

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "StatusResult":
        return cls(
            escrow_id=d["escrow_id"],
            state=d["state"],
            locked_cu=d["locked_cu"],
            released_cu=d["released_cu"],
            reclaimed_cu=d["reclaimed_cu"],
            remaining_cu=d["remaining_cu"],
            payee_id=d.get("payee_id"),
            valid_until_ms=d["valid_until_ms"],
            claim=d.get("claim"),
            dispute=d.get("dispute"),
        )


class EscrowClient:
    """A connection's Layer 4 client — buyer or provider side alike.

    Built on a Stage 1 :class:`~acmp.buyer.Buyer` the same way
    :class:`acmp.negotiation.Negotiator` is: a thin typed wrapper over
    :meth:`Buyer.request`, the one connection to the Escrow Agent that all
    seven ``acmp/escrow*`` calls from this party go through.
    """

    def __init__(self, buyer: Buyer) -> None:
        self._buyer = buyer

    async def lock(
        self,
        amount_cu: float,
        *,
        valid_until_ms: int,
        tier: str | None = None,
        payee_id: str | None = None,
        challenge_window_ms: int | None = None,
        op_ref: str | None = None,
    ) -> LockResult:
        """Send ``acmp/escrowLock`` (§4.1) and return the opened escrow."""
        params: dict[str, Any] = {
            "op_ref": op_ref or new_op_ref(),
            "amount_cu": amount_cu,
            "valid_until_ms": valid_until_ms,
        }
        if tier is not None:
            params["tier"] = tier
        if payee_id is not None:
            params["payee_id"] = payee_id
        if challenge_window_ms is not None:
            params["challenge_window_ms"] = challenge_window_ms
        result = await self._buyer.request("acmp/escrowLock", params)
        return LockResult.from_dict(result)

    async def bind(
        self,
        escrow_id: str,
        payee_id: str,
        *,
        challenge_window_ms: int | None = None,
        op_ref: str | None = None,
    ) -> dict[str, Any]:
        """Send ``acmp/escrowBind`` (§4.2): once-only payee binding."""
        params: dict[str, Any] = {
            "op_ref": op_ref or new_op_ref(),
            "escrow_id": escrow_id,
            "payee_id": payee_id,
        }
        if challenge_window_ms is not None:
            params["challenge_window_ms"] = challenge_window_ms
        return await self._buyer.request("acmp/escrowBind", params)

    async def release(
        self,
        escrow_id: str,
        amount_cu: float,
        payee_id: str,
        *,
        task_id: str | None = None,
        proof: dict[str, Any] | None = None,
        op_ref: str | None = None,
    ) -> ReleaseResult:
        """Send ``acmp/escrowRelease`` (§4.3): pay out against verified proof.

        Also the fast-forward path for a pending claim (``amount_cu`` must be
        ≥ the claimed amount) and the implicit-bind path for an unbound
        escrow.
        """
        params: dict[str, Any] = {
            "op_ref": op_ref or new_op_ref(),
            "escrow_id": escrow_id,
            "amount_cu": amount_cu,
            "payee_id": payee_id,
        }
        if task_id is not None:
            params["task_id"] = task_id
        if proof is not None:
            params["proof"] = proof
        result = await self._buyer.request("acmp/escrowRelease", params)
        return ReleaseResult.from_dict(result)

    async def reclaim(
        self,
        escrow_id: str,
        *,
        amount_cu: float | None = None,
        op_ref: str | None = None,
    ) -> ReclaimResult:
        """Send ``acmp/escrowReclaim`` (§4.4). Omitting ``amount_cu`` reclaims
        the entire remaining balance."""
        params: dict[str, Any] = {"op_ref": op_ref or new_op_ref(), "escrow_id": escrow_id}
        if amount_cu is not None:
            params["amount_cu"] = amount_cu
        result = await self._buyer.request("acmp/escrowReclaim", params)
        return ReclaimResult.from_dict(result)

    async def claim(
        self,
        escrow_id: str,
        amount_cu: float,
        task_id: str,
        proof: dict[str, Any],
        *,
        op_ref: str | None = None,
    ) -> ClaimResult:
        """Send ``acmp/escrowClaim`` (§4.5): the safety path for a silent or
        unresponsive buyer — starts the challenge window."""
        params: dict[str, Any] = {
            "op_ref": op_ref or new_op_ref(),
            "escrow_id": escrow_id,
            "amount_cu": amount_cu,
            "task_id": task_id,
            "proof": proof,
        }
        result = await self._buyer.request("acmp/escrowClaim", params)
        return ClaimResult.from_dict(result)

    async def dispute(
        self,
        escrow_id: str,
        reason: str,
        *,
        evidence: dict[str, Any] | None = None,
        op_ref: str | None = None,
    ) -> dict[str, Any]:
        """Send ``acmp/escrowDispute`` (§4.6): contest a pending claim before
        its challenge window elapses."""
        params: dict[str, Any] = {
            "op_ref": op_ref or new_op_ref(),
            "escrow_id": escrow_id,
            "reason": reason,
        }
        if evidence is not None:
            params["evidence"] = evidence
        return await self._buyer.request("acmp/escrowDispute", params)

    async def status(self, escrow_id: str) -> StatusResult:
        """Send ``acmp/escrowStatus`` (§4.7): callable by buyer or bound payee."""
        result = await self._buyer.request("acmp/escrowStatus", {"escrow_id": escrow_id})
        return StatusResult.from_dict(result)

    async def covers(self, escrow_id: str, amount_cu: float) -> bool:
        """Whether ``escrow_id`` currently has at least ``amount_cu`` remaining.

        Implements :class:`EscrowVerifier` so a
        :class:`~acmp.provider.Provider` can use an :class:`EscrowClient`
        directly as its ``escrow`` check (Layer 1 §3.3 ``escrow_invalid``,
        -33005). Mirrors the retired ``EscrowStub.covers`` in returning
        ``False`` for an unknown escrow rather than raising.
        """
        try:
            status = await self.status(escrow_id)
        except AcmpError as err:
            if err.code == EscrowErrorCode.ESCROW_NOT_FOUND:
                return False
            raise
        return status.remaining_cu >= amount_cu

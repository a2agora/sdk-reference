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
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from .errors import AcmpError, EscrowErrorCode


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

"""A minimal in-memory stand-in for Layer 4 (Escrow & Settlement).

Layer 4 is still `discussion` status in the spec and is explicitly out of
scope for this SDK. :class:`EscrowStub` exists only so Stage 2 negotiation
has a real ``escrow_id`` to thread from ``acmp/accept`` through to
``acmp/invoke``, and so a provider can demonstrate the ``escrow_invalid``
(-33005) check from Layer 1 §3.3. It has no notion of settlement, release,
partial spend, or reclaiming unused funds — a real Layer 4 implementation
would run as a neutral trusted party (or on-chain contract), not be shared
in-process between buyer and provider as it is in this demo.
"""

from __future__ import annotations

import secrets
from dataclasses import dataclass


def new_escrow_id() -> str:
    return f"esc_{secrets.token_hex(6)}"


@dataclass
class _Lock:
    amount_cu: float


class EscrowStub:
    """Tracks CU amounts locked against escrow ids."""

    def __init__(self) -> None:
        self._locks: dict[str, _Lock] = {}

    def lock(self, amount_cu: float) -> str:
        """Lock ``amount_cu`` and return a new escrow id referencing it."""
        escrow_id = new_escrow_id()
        self._locks[escrow_id] = _Lock(amount_cu)
        return escrow_id

    def covers(self, escrow_id: str, amount_cu: float) -> bool:
        """Whether ``escrow_id`` exists and locks at least ``amount_cu``."""
        lock = self._locks.get(escrow_id)
        return lock is not None and lock.amount_cu >= amount_cu

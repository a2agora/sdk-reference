"""ACMP error codes and exception type.

Layer 1 §3.3 codes live in the -33xxx range to avoid collision with the
JSON-RPC reserved range (-32xxx). Layer 4 §5 escrow codes use -35xxx
(-34xxx is informally taken by negotiation, see
:class:`acmp.negotiation.NegotiationErrorCode`).
"""

from __future__ import annotations

from enum import IntEnum
from typing import Any


class ErrorCode(IntEnum):
    """ACMP-specific JSON-RPC error codes (Layer 1 §3.3)."""

    BUDGET_EXCEEDED = -33001
    CAPABILITY_NOT_FOUND = -33002
    TIMEOUT = -33003
    CANCELLED = -33004
    ESCROW_INVALID = -33005
    PROOF_UNSUPPORTED = -33006
    FEATURE_UNSUPPORTED = -33007
    INTERNAL = -33099


class EscrowErrorCode(IntEnum):
    """Layer 4 §5 escrow error codes (-35xxx), raised by an Escrow Agent."""

    ESCROW_NOT_FOUND = -35001
    INSUFFICIENT_FUNDS = -35002
    INVALID_STATE = -35003
    ESCROW_EXPIRED = -35004
    NOT_AUTHORIZED = -35005
    AMOUNT_EXCEEDS_REMAINING = -35006
    INTERNAL = -35099


# Default human-readable messages for each code.
_DEFAULT_MESSAGES: dict[IntEnum, str] = {
    ErrorCode.BUDGET_EXCEEDED: "Budget exceeded",
    ErrorCode.CAPABILITY_NOT_FOUND: "Capability not found",
    ErrorCode.TIMEOUT: "Task timed out",
    ErrorCode.CANCELLED: "Task cancelled",
    ErrorCode.ESCROW_INVALID: "Escrow invalid",
    ErrorCode.PROOF_UNSUPPORTED: "Proof method unsupported",
    ErrorCode.FEATURE_UNSUPPORTED: "Feature unsupported",
    ErrorCode.INTERNAL: "Internal provider error",
    EscrowErrorCode.ESCROW_NOT_FOUND: "Unknown escrow_id",
    EscrowErrorCode.INSUFFICIENT_FUNDS: "Lock could not be funded",
    EscrowErrorCode.INVALID_STATE: "Operation not permitted in the current state",
    EscrowErrorCode.ESCROW_EXPIRED: "The lock passed valid_until_ms and was auto-reclaimed",
    EscrowErrorCode.NOT_AUTHORIZED: "Caller is not the party allowed to perform this operation",
    EscrowErrorCode.AMOUNT_EXCEEDS_REMAINING: "Amount exceeds the remaining balance",
    EscrowErrorCode.INTERNAL: "Internal escrow-agent error",
}


class AcmpError(Exception):
    """An ACMP protocol error.

    Raised by provider handlers (or the SDK internals) and serialized into a
    JSON-RPC error response. Carries the ACMP error code and an optional
    structured ``data`` payload as described in Layer 1 §3.3.
    """

    def __init__(
        self,
        code: "IntEnum | int",
        message: str | None = None,
        data: dict[str, Any] | None = None,
    ) -> None:
        self.code = code
        self.message = message or _DEFAULT_MESSAGES.get(code, "ACMP error")
        self.data = data or {}
        super().__init__(f"[{int(code)}] {self.message}")

    def to_jsonrpc(self) -> dict[str, Any]:
        """Render the error as a JSON-RPC ``error`` object."""
        return {"code": int(self.code), "message": self.message, "data": self.data}

    @classmethod
    def from_jsonrpc(cls, err: dict[str, Any]) -> "AcmpError":
        """Reconstruct an :class:`AcmpError` from a JSON-RPC ``error`` object.

        The code is resolved to :class:`ErrorCode` (Layer 1) or
        :class:`EscrowErrorCode` (Layer 4) where possible, and kept as a
        plain ``int`` otherwise — e.g. a Layer 6 negotiation error, whose
        enum lives in :mod:`acmp.negotiation`.
        """
        raw_code = err["code"]
        code: "IntEnum | int" = raw_code
        for enum_cls in (ErrorCode, EscrowErrorCode):
            try:
                code = enum_cls(raw_code)
                break
            except ValueError:
                continue
        return cls(code, err.get("message"), err.get("data"))

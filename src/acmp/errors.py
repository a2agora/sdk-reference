"""ACMP error codes and exception type.

Codes are defined in Layer 1 §3.3. They live in the -33xxx range to avoid
collision with the JSON-RPC reserved range (-32xxx).
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


# Default human-readable messages for each code.
_DEFAULT_MESSAGES: dict[ErrorCode, str] = {
    ErrorCode.BUDGET_EXCEEDED: "Budget exceeded",
    ErrorCode.CAPABILITY_NOT_FOUND: "Capability not found",
    ErrorCode.TIMEOUT: "Task timed out",
    ErrorCode.CANCELLED: "Task cancelled",
    ErrorCode.ESCROW_INVALID: "Escrow invalid",
    ErrorCode.PROOF_UNSUPPORTED: "Proof method unsupported",
    ErrorCode.FEATURE_UNSUPPORTED: "Feature unsupported",
    ErrorCode.INTERNAL: "Internal provider error",
}


class AcmpError(Exception):
    """An ACMP protocol error.

    Raised by provider handlers (or the SDK internals) and serialized into a
    JSON-RPC error response. Carries the ACMP error code and an optional
    structured ``data`` payload as described in Layer 1 §3.3.
    """

    def __init__(
        self,
        code: "ErrorCode | int",
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

        The code is kept as a plain ``int`` when it falls outside
        :class:`ErrorCode` — e.g. a Layer 6 negotiation error, which isn't
        part of the Layer 1 §3.3 table this enum models.
        """
        raw_code = err["code"]
        try:
            code: "ErrorCode | int" = ErrorCode(raw_code)
        except ValueError:
            code = raw_code
        return cls(code, err.get("message"), err.get("data"))

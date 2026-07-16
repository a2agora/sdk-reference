"""Tests for Stage 5: Layer 4 escrow & settlement.

Starts with the -35xxx error-code table (Layer 4 §5); the EscrowAgent state
machine, idempotency, and lifecycle tests build on top as the stage grows.
"""

from __future__ import annotations

from acmp import AcmpError, EscrowErrorCode


def test_escrow_error_codes_match_spec_table():
    """Codes and names exactly as in Layer 4 §5."""
    expected = {
        "ESCROW_NOT_FOUND": -35001,
        "INSUFFICIENT_FUNDS": -35002,
        "INVALID_STATE": -35003,
        "ESCROW_EXPIRED": -35004,
        "NOT_AUTHORIZED": -35005,
        "AMOUNT_EXCEEDS_REMAINING": -35006,
        "INTERNAL": -35099,
    }
    actual = {member.name: member.value for member in EscrowErrorCode}
    assert actual == expected


def test_escrow_error_roundtrips_through_jsonrpc():
    """An agent-side -35xxx error survives serialization with a typed code."""
    err = AcmpError(EscrowErrorCode.NOT_AUTHORIZED, data={"escrow_id": "esc_x"})
    wire = err.to_jsonrpc()
    assert wire["code"] == -35005

    back = AcmpError.from_jsonrpc(wire)
    assert back.code == EscrowErrorCode.NOT_AUTHORIZED
    assert isinstance(back.code, EscrowErrorCode)
    assert back.data == {"escrow_id": "esc_x"}


def test_escrow_error_has_default_message():
    assert "auto-reclaimed" in AcmpError(EscrowErrorCode.ESCROW_EXPIRED).message

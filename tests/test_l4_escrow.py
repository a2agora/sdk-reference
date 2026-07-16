"""Tests for Stage 5: Layer 4 escrow & settlement.

Covers the -35xxx error-code table (§5), the escrow data model, and the
credit-ledger rail (§7.2); the EscrowAgent state machine, idempotency, and
lifecycle tests build on top as the stage grows.
"""

from __future__ import annotations

import pytest

from acmp import AcmpError, EscrowErrorCode
from acmp.escrow import CreditLedger, Escrow, EscrowState


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


# --- data model ----------------------------------------------------------------


def make_escrow(**overrides) -> Escrow:
    defaults = dict(
        escrow_id="esc_test",
        buyer_id="agent:buyer:test",
        locked_cu=0.005,
        valid_until_ms=1_000_000,
        challenge_window_ms=60_000,
    )
    defaults.update(overrides)
    return Escrow(**defaults)


def test_escrow_remaining_balance_tracks_release_and_reclaim():
    esc = make_escrow()
    assert esc.state is EscrowState.OPEN
    assert esc.remaining_cu == 0.005

    esc.released_cu = 0.003
    esc.reclaimed_cu = 0.002
    assert esc.remaining_cu == 0  # released + reclaimed == locked (§2 closed)


# --- credit ledger (§7.2) -------------------------------------------------------


def test_ledger_debit_and_credit_roundtrip():
    ledger = CreditLedger()
    ledger.credit("agent:buyer:test", 1.0)
    ledger.debit("agent:buyer:test", 0.005)
    assert ledger.balance("agent:buyer:test") == pytest.approx(0.995)


def test_ledger_underfunded_debit_raises_insufficient_funds():
    ledger = CreditLedger()
    ledger.credit("agent:buyer:test", 0.001)

    with pytest.raises(AcmpError) as exc_info:
        ledger.debit("agent:buyer:test", 0.005)

    assert exc_info.value.code == EscrowErrorCode.INSUFFICIENT_FUNDS
    assert ledger.balance("agent:buyer:test") == 0.001  # nothing moved


def test_ledger_unknown_account_has_zero_balance():
    assert CreditLedger().balance("agent:nobody:test") == 0


def test_ledger_payout_is_once_per_transition():
    """§7.1: one payout per (escrow_id, transition) — a retry pays nothing."""
    ledger = CreditLedger()

    assert ledger.payout("esc_1", "release:op_a", "agent:payee:test", 0.003) is True
    assert ledger.payout("esc_1", "release:op_a", "agent:payee:test", 0.003) is False
    assert ledger.balance("agent:payee:test") == 0.003

    # A different transition on the same escrow pays normally.
    assert ledger.payout("esc_1", "release:op_b", "agent:payee:test", 0.001) is True
    assert ledger.balance("agent:payee:test") == 0.004

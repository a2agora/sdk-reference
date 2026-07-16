"""Tests for Stage 5: Layer 4 escrow & settlement.

Covers the -35xxx error-code table (§5), the escrow data model, and the
credit-ledger rail (§7.2); the EscrowAgent state machine, idempotency, and
lifecycle tests build on top as the stage grows.
"""

from __future__ import annotations

import asyncio

import pytest

from acmp import AcmpError, Buyer, EscrowErrorCode, InMemoryTransport
from acmp.escrow import (
    DEFAULT_CHALLENGE_WINDOW_MS,
    CreditLedger,
    Escrow,
    EscrowAgent,
    EscrowClient,
    EscrowState,
)

BUYER_ID = "agent:buyer:test"
PAYEE_ID = "agent:payee:test"
FAR_FUTURE_MS = 4_000_000_000_000  # ~year 2096; epoch-ms is 13 digits by 2026


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


# --- EscrowAgent: lock / bind / status (§4.1, §4.2, §4.7) -----------------------


async def _wire(agent: EscrowAgent, *party_ids: str):
    """Spin up one InMemoryTransport pair + agent.serve() task per party_id.

    Returns ``(buyers, serve_tasks)`` where ``buyers`` are still-open
    :class:`Buyer` connections (the caller enters/exits them) and
    ``serve_tasks`` must be awaited after every buyer is closed.
    """
    buyers = []
    serve_tasks = []
    for party_id in party_ids:
        client_t, agent_t = InMemoryTransport.create_pair()
        serve_tasks.append(asyncio.create_task(agent.serve(agent_t, party_id)))
        buyers.append(Buyer(client_t))
    return buyers, serve_tasks


@pytest.mark.asyncio
async def test_lock_bind_status_happy_path():
    agent = EscrowAgent()
    agent.ledger.credit(BUYER_ID, 1.0)
    (buyer_conn, payee_conn), serve_tasks = await _wire(agent, BUYER_ID, PAYEE_ID)

    async with buyer_conn, payee_conn:
        buyer_escrow = EscrowClient(buyer_conn)
        payee_escrow = EscrowClient(payee_conn)

        locked = await buyer_escrow.lock(0.005, valid_until_ms=FAR_FUTURE_MS)
        assert locked.state == "open"
        assert locked.amount_cu == 0.005
        assert agent.ledger.balance(BUYER_ID) == pytest.approx(0.995)

        bound = await buyer_escrow.bind(locked.escrow_id, PAYEE_ID)
        assert bound == {"escrow_id": locked.escrow_id, "payee_id": PAYEE_ID}

        status_from_buyer = await buyer_escrow.status(locked.escrow_id)
        status_from_payee = await payee_escrow.status(locked.escrow_id)
        assert status_from_buyer.state == "open"
        assert status_from_buyer.payee_id == PAYEE_ID
        assert status_from_buyer.remaining_cu == 0.005
        assert status_from_payee == status_from_buyer

    await asyncio.gather(*serve_tasks)


@pytest.mark.asyncio
async def test_lock_without_funding_raises_insufficient_funds():
    agent = EscrowAgent()
    (buyer_conn,), serve_tasks = await _wire(agent, BUYER_ID)

    async with buyer_conn:
        with pytest.raises(AcmpError) as exc_info:
            await EscrowClient(buyer_conn).lock(0.005, valid_until_ms=FAR_FUTURE_MS)

    assert exc_info.value.code == EscrowErrorCode.INSUFFICIENT_FUNDS
    await asyncio.gather(*serve_tasks)


@pytest.mark.asyncio
async def test_rebind_raises_invalid_state():
    agent = EscrowAgent()
    agent.ledger.credit(BUYER_ID, 1.0)
    (buyer_conn,), serve_tasks = await _wire(agent, BUYER_ID)

    async with buyer_conn:
        escrow = EscrowClient(buyer_conn)
        locked = await escrow.lock(0.005, valid_until_ms=FAR_FUTURE_MS)
        await escrow.bind(locked.escrow_id, PAYEE_ID)

        with pytest.raises(AcmpError) as exc_info:
            await escrow.bind(locked.escrow_id, "agent:someone-else:test")

    assert exc_info.value.code == EscrowErrorCode.INVALID_STATE
    await asyncio.gather(*serve_tasks)


@pytest.mark.asyncio
async def test_lock_with_payee_id_counts_as_already_bound():
    """§4.1: payee_id MAY be set at lock time instead of via a later bind —
    doing so already satisfies the once-only bind (§4.2)."""
    agent = EscrowAgent()
    agent.ledger.credit(BUYER_ID, 1.0)
    (buyer_conn,), serve_tasks = await _wire(agent, BUYER_ID)

    async with buyer_conn:
        escrow = EscrowClient(buyer_conn)
        locked = await escrow.lock(0.005, valid_until_ms=FAR_FUTURE_MS, payee_id=PAYEE_ID)
        status = await escrow.status(locked.escrow_id)
        assert status.payee_id == PAYEE_ID

        with pytest.raises(AcmpError) as exc_info:
            await escrow.bind(locked.escrow_id, PAYEE_ID)

    assert exc_info.value.code == EscrowErrorCode.INVALID_STATE
    await asyncio.gather(*serve_tasks)


@pytest.mark.asyncio
async def test_bind_by_non_buyer_raises_not_authorized():
    agent = EscrowAgent()
    agent.ledger.credit(BUYER_ID, 1.0)
    (buyer_conn, stranger_conn), serve_tasks = await _wire(agent, BUYER_ID, "agent:stranger:test")

    async with buyer_conn, stranger_conn:
        locked = await EscrowClient(buyer_conn).lock(0.005, valid_until_ms=FAR_FUTURE_MS)

        with pytest.raises(AcmpError) as exc_info:
            await EscrowClient(stranger_conn).bind(locked.escrow_id, PAYEE_ID)

    assert exc_info.value.code == EscrowErrorCode.NOT_AUTHORIZED
    await asyncio.gather(*serve_tasks)


@pytest.mark.asyncio
async def test_status_by_non_party_raises_not_authorized():
    agent = EscrowAgent()
    agent.ledger.credit(BUYER_ID, 1.0)
    (buyer_conn, stranger_conn), serve_tasks = await _wire(agent, BUYER_ID, "agent:stranger:test")

    async with buyer_conn, stranger_conn:
        locked = await EscrowClient(buyer_conn).lock(0.005, valid_until_ms=FAR_FUTURE_MS)

        with pytest.raises(AcmpError) as exc_info:
            await EscrowClient(stranger_conn).status(locked.escrow_id)

    assert exc_info.value.code == EscrowErrorCode.NOT_AUTHORIZED
    await asyncio.gather(*serve_tasks)


@pytest.mark.asyncio
async def test_status_unknown_escrow_raises_escrow_not_found():
    agent = EscrowAgent()
    (buyer_conn,), serve_tasks = await _wire(agent, BUYER_ID)

    async with buyer_conn:
        with pytest.raises(AcmpError) as exc_info:
            await EscrowClient(buyer_conn).status("esc_never_locked")

    assert exc_info.value.code == EscrowErrorCode.ESCROW_NOT_FOUND
    await asyncio.gather(*serve_tasks)


@pytest.mark.asyncio
async def test_lock_uses_default_challenge_window_when_unset():
    agent = EscrowAgent()
    agent.ledger.credit(BUYER_ID, 1.0)
    (buyer_conn,), serve_tasks = await _wire(agent, BUYER_ID)

    async with buyer_conn:
        locked = await EscrowClient(buyer_conn).lock(0.005, valid_until_ms=FAR_FUTURE_MS)

    assert agent.escrow(locked.escrow_id).challenge_window_ms == DEFAULT_CHALLENGE_WINDOW_MS
    await asyncio.gather(*serve_tasks)


@pytest.mark.asyncio
async def test_bind_challenge_window_overrides_lock_default():
    """§4.2: the negotiated value at bind time overrides any lock-time default."""
    agent = EscrowAgent()
    agent.ledger.credit(BUYER_ID, 1.0)
    (buyer_conn,), serve_tasks = await _wire(agent, BUYER_ID)

    async with buyer_conn:
        escrow = EscrowClient(buyer_conn)
        locked = await escrow.lock(0.005, valid_until_ms=FAR_FUTURE_MS, challenge_window_ms=1000)
        await escrow.bind(locked.escrow_id, PAYEE_ID, challenge_window_ms=2000)

    assert agent.escrow(locked.escrow_id).challenge_window_ms == 2000
    await asyncio.gather(*serve_tasks)


# --- op_ref idempotency (§3, incl. the F1 lock-dedup fix) -----------------------


@pytest.mark.asyncio
async def test_lock_retry_with_same_op_ref_does_not_double_debit():
    """A retried escrowLock (e.g. a network-failure retry) must not open a
    second escrow or debit the buyer twice — this is what the agent-wide
    ``_lock_ops`` dedup index exists for (§3 applies to escrowLock too, even
    though it has no escrow_id to key a per-escrow cache off yet)."""
    agent = EscrowAgent()
    agent.ledger.credit(BUYER_ID, 1.0)
    (buyer_conn,), serve_tasks = await _wire(agent, BUYER_ID)

    async with buyer_conn:
        escrow = EscrowClient(buyer_conn)
        op_ref = "op_retry_test"
        first = await escrow.lock(0.005, valid_until_ms=FAR_FUTURE_MS, op_ref=op_ref)
        second = await escrow.lock(0.005, valid_until_ms=FAR_FUTURE_MS, op_ref=op_ref)

    assert first.escrow_id == second.escrow_id
    assert len(agent._escrows) == 1
    assert agent.ledger.balance(BUYER_ID) == pytest.approx(0.995)  # debited once, not twice
    await asyncio.gather(*serve_tasks)


@pytest.mark.asyncio
async def test_lock_retry_replays_cached_error_even_after_funding_changes():
    agent = EscrowAgent()
    agent.ledger.credit(BUYER_ID, 0.001)
    (buyer_conn,), serve_tasks = await _wire(agent, BUYER_ID)

    async with buyer_conn:
        escrow = EscrowClient(buyer_conn)
        op_ref = "op_fail_retry"
        with pytest.raises(AcmpError) as first_exc:
            await escrow.lock(0.005, valid_until_ms=FAR_FUTURE_MS, op_ref=op_ref)

        agent.ledger.credit(BUYER_ID, 1.0)  # now funded — replay must still fail identically

        with pytest.raises(AcmpError) as second_exc:
            await escrow.lock(0.005, valid_until_ms=FAR_FUTURE_MS, op_ref=op_ref)

    assert first_exc.value.code == second_exc.value.code == EscrowErrorCode.INSUFFICIENT_FUNDS
    await asyncio.gather(*serve_tasks)


@pytest.mark.asyncio
async def test_bind_retry_with_same_op_ref_replays_result():
    agent = EscrowAgent()
    agent.ledger.credit(BUYER_ID, 1.0)
    (buyer_conn,), serve_tasks = await _wire(agent, BUYER_ID)

    async with buyer_conn:
        escrow = EscrowClient(buyer_conn)
        locked = await escrow.lock(0.005, valid_until_ms=FAR_FUTURE_MS)
        op_ref = "op_bind_retry"
        first = await escrow.bind(locked.escrow_id, PAYEE_ID, op_ref=op_ref)
        second = await escrow.bind(locked.escrow_id, PAYEE_ID, op_ref=op_ref)

    assert first == second == {"escrow_id": locked.escrow_id, "payee_id": PAYEE_ID}
    await asyncio.gather(*serve_tasks)


# --- expiry via the injected clock (§2 "Expiry", R6/R15) ------------------------
# Full claim/dispute auto-release coverage lands with Task 6; these two exercise
# the expiry half of _effective_state deliberately, on top of the already-shared
# lock/status/bind machinery from this task.


@pytest.mark.asyncio
async def test_status_reflects_expiry_auto_reclaim():
    clock = {"now": 1_000_000}
    agent = EscrowAgent(now_ms=lambda: clock["now"])
    agent.ledger.credit(BUYER_ID, 1.0)
    (buyer_conn,), serve_tasks = await _wire(agent, BUYER_ID)

    async with buyer_conn:
        escrow = EscrowClient(buyer_conn)
        locked = await escrow.lock(0.005, valid_until_ms=1_000_500)
        clock["now"] = 1_000_501  # past valid_until_ms

        status = await escrow.status(locked.escrow_id)

    assert status.state == "closed"
    assert status.reclaimed_cu == 0.005
    assert agent.ledger.balance(BUYER_ID) == pytest.approx(1.0)  # fully refunded
    await asyncio.gather(*serve_tasks)


@pytest.mark.asyncio
async def test_expired_escrow_rejects_further_ops_with_escrow_expired():
    """F4: ops on an expiry-closed escrow answer -35004, not the generic -35003."""
    clock = {"now": 1_000_000}
    agent = EscrowAgent(now_ms=lambda: clock["now"])
    agent.ledger.credit(BUYER_ID, 1.0)
    (buyer_conn,), serve_tasks = await _wire(agent, BUYER_ID)

    async with buyer_conn:
        escrow = EscrowClient(buyer_conn)
        locked = await escrow.lock(0.005, valid_until_ms=1_000_500)
        clock["now"] = 1_000_501

        with pytest.raises(AcmpError) as exc_info:
            await escrow.bind(locked.escrow_id, PAYEE_ID)

    assert exc_info.value.code == EscrowErrorCode.ESCROW_EXPIRED
    await asyncio.gather(*serve_tasks)

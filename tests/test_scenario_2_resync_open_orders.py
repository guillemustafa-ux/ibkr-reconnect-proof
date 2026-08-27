"""Scenario 2 — state resync after a drop with live orders.

An acknowledged order is working at the broker when the connection dies.
After reconnecting (same clientId, fresh process), the client's view must
converge to the broker's: the working order is visible again, adopted by
the ledger, and nothing is resent or duplicated.
"""

from ibkr_reconnect_proof.ledger import ACKED
from tests.conftest import es_future, wait_disconnected


async def test_working_order_survives_drop_and_is_rebound(broker, trader, ledger):
    contract = es_future()

    ref, trade = await trader.place_limit(contract, "SELL", 2, 5100.0)
    while ledger.get(ref).state != ACKED:  # wait for the broker's ack
        await trader.ib.updateEvent
    assert trade.orderStatus.status == "Submitted"

    # The socket dies mid-session. Broker keeps working the order.
    broker.drop_connections()
    await wait_disconnected(trader)
    assert len(broker.open_orders()) == 1

    # Fresh process, same clientId: open-order sync must rebind the order.
    await trader.connect()
    report = await trader.recover({"ES": contract})

    assert report.resent == [], "nothing to resend: order was never lost"
    open_refs = {t.order.orderRef for t in trader.ib.openTrades()}
    assert ref in open_refs, "client's view converged to the broker's"
    assert ledger.get(ref).state == ACKED
    assert len(broker.orders_by_ref(ref)) == 1
    assert broker.place_order_count[ref] == 1, "exactly one placeOrder ever sent"


async def test_ledger_floor_prevents_order_id_reuse(broker, trader, ledger):
    """The broker's nextValidId hint must never override the ledger.

    After losing an in-flight order, the broker's idea of the next id can
    lag the ids the client already reserved. Trusting the hint would reuse
    the id of an order whose fate is unknown — placeOrder on a known id is
    a MODIFY, so a reused id can silently rewrite a live order.
    """
    contract = es_future()
    broker.arm_fault("drop_before_process")

    ref, _trade = await trader.place_limit(contract, "BUY", 1, 5000.0)
    reserved_id = ledger.get(ref).order_id
    await wait_disconnected(trader)

    # Broker never saw the order, so its nextValidId hint still points at
    # (or below) the reserved id.
    await trader.connect()
    next_id = trader.ib.client.getReqId()
    assert next_id > reserved_id, (
        "id sequence must start above the ledger high-water mark, "
        "not at the broker's stale hint"
    )

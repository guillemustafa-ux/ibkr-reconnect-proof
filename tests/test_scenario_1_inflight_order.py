"""Scenario 1 — the in-flight order.

The connection dies between the placeOrder socket write and the broker's
acknowledgement. The client cannot know whether the broker received the
order. Proof obligations:

* the ledger reserved (orderId, orderRef) durably BEFORE the socket write,
  so recovery has something to interrogate the broker with;
* recovery asks the broker for the fate of THAT order and only resends when
  it is proven absent;
* in no branch does the broker end up with a duplicate.

The naive-retry test at the bottom shows the failure this harness exists to
catch: a blind resend under a fresh orderId duplicates the order.
"""

import asyncio

from ib_async import LimitOrder

from ibkr_reconnect_proof.ledger import ACKED, PENDING_STATES
from tests.conftest import es_future, wait_disconnected


async def test_order_lost_in_transit_is_resent_exactly_once(broker, trader, ledger):
    contract = es_future()
    broker.arm_fault("drop_before_process")

    # The TCP write succeeds (kernel buffer), the broker never sees the order.
    ref, _trade = await trader.place_limit(contract, "BUY", 1, 5000.0)
    await wait_disconnected(trader)

    assert broker.orders_by_ref(ref) == [], "broker must not know the order"
    pending = ledger.pending()
    assert [o.order_ref for o in pending] == [ref], \
        "ledger must hold the unresolved reservation"
    reserved_id = pending[0].order_id

    # Crash-restart: fresh IB instance, recovery driven by the ledger alone.
    await trader.connect()
    report = await trader.recover({"ES": contract})

    assert report.resent == [ref]
    assert report.adopted == [] and report.filled == []
    orders = broker.orders_by_ref(ref)
    assert len(orders) == 1, "exactly one order at the broker, no duplicate"
    assert orders[0].order_id == reserved_id, \
        "resend reuses the reserved orderId, no id burned or reused"
    assert broker.place_order_count[ref] == 1, \
        "broker received exactly one placeOrder for this ref"


async def test_order_reached_broker_ack_lost_is_adopted_not_resent(
    broker, trader, ledger
):
    contract = es_future()
    broker.arm_fault("drop_after_process")

    # The broker registers and works the order; the ack dies with the socket.
    ref, _trade = await trader.place_limit(contract, "BUY", 1, 5000.0)
    await wait_disconnected(trader)

    assert len(broker.orders_by_ref(ref)) == 1, "broker is working the order"
    assert ledger.get(ref).state in PENDING_STATES, "client-side fate unknown"

    await trader.connect()
    report = await trader.recover({"ES": contract})

    assert report.adopted == [ref], "recovery must adopt, not resend"
    assert report.resent == []
    assert ledger.get(ref).state == ACKED
    assert len(broker.orders_by_ref(ref)) == 1, "still exactly one order"
    assert broker.place_order_count[ref] == 1, \
        "no second placeOrder ever reached the broker"


async def test_naive_retry_duplicates_the_position(broker, trader, ledger):
    """The anti-pattern, demonstrated: this is the bug the ledger prevents.

    A naive client treats the timeout as 'order failed', reconnects, takes a
    fresh orderId from the broker and resends. The broker was working the
    original all along: the account is now exposed at double size.
    """
    contract = es_future()
    broker.arm_fault("drop_after_process")

    ref, _trade = await trader.place_limit(contract, "BUY", 1, 5000.0)
    await wait_disconnected(trader)

    # Naive retry: reconnect and blindly resend with a broker-issued id.
    await trader.connect()
    naive_order = LimitOrder("BUY", 1, 5000.0,
                             orderId=trader.ib.client.getReqId(), orderRef=ref)
    trader.ib.placeOrder(contract, naive_order)
    await asyncio.sleep(0.05)

    assert len(broker.orders_by_ref(ref)) == 2, \
        "blind retry produced two live orders for one intent"
    assert broker.place_order_count[ref] == 2

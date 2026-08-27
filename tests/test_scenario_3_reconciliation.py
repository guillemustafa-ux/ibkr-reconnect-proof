"""Scenario 3 — the reconciliation ledger bites.

Two proof obligations:

* A fill that happens WHILE the client is disconnected is folded into the
  ledger via execution replay on reconnect, and reconciliation then reports
  a clean match — no false alarm on the recoverable case.
* A broker-side position change with no execution behind it (manual trade,
  broker correction, missed message) IS reported, with its exact delta.
  A reconciler is only worth having if this test can fail.
"""

from ibkr_reconnect_proof.ledger import ACKED, FILLED
from ibkr_reconnect_proof.reconcile import reconcile
from tests.conftest import es_future, wait_disconnected


async def test_fill_during_outage_is_recovered_and_reconciles_clean(
    broker, trader, ledger
):
    contract = es_future()

    ref, _trade = await trader.place_limit(contract, "BUY", 3, 5000.0)
    while ledger.get(ref).state != ACKED:
        await trader.ib.updateEvent

    broker.drop_connections()
    await wait_disconnected(trader)

    # The order fills while nobody is listening. The client's last known
    # state says "working"; the broker's truth says "filled, position +3".
    broker.fill(ref, price=4999.75)

    await trader.connect()
    report = await trader.recover({"ES": contract})

    assert report.filled == [ref], "execution replay resolved the order"
    assert report.resent == []
    assert ledger.get(ref).state == FILLED
    assert ledger.filled_quantity(ref) == 3

    recon = await reconcile(trader.ib, ledger)
    assert recon.clean, f"no divergence expected, got {recon.divergences}"
    assert recon.matched == {"ES": 3.0}


async def test_execution_replay_is_idempotent(broker, trader, ledger):
    """Reconnecting twice replays the same executions; execId keying must
    absorb the replay without double-counting a single contract."""
    contract = es_future()

    ref, _trade = await trader.place_limit(contract, "BUY", 3, 5000.0)
    while ledger.get(ref).state != ACKED:
        await trader.ib.updateEvent
    broker.fill(ref, price=4999.75)
    while ledger.filled_quantity(ref) < 3:
        await trader.ib.updateEvent

    for _ in range(2):  # two extra reconnects, two extra full replays
        broker.drop_connections()
        await wait_disconnected(trader)
        await trader.connect()
        await trader.recover({"ES": contract})

    assert ledger.filled_quantity(ref) == 3, "replays must not double-count"
    recon = await reconcile(trader.ib, ledger)
    assert recon.clean and recon.matched == {"ES": 3.0}


async def test_unexplained_broker_drift_is_detected(broker, trader, ledger):
    contract = es_future()

    ref, _trade = await trader.place_limit(contract, "BUY", 3, 5000.0)
    while ledger.get(ref).state != ACKED:
        await trader.ib.updateEvent
    broker.fill(ref, price=4999.75)
    while ledger.filled_quantity(ref) < 3:
        await trader.ib.updateEvent

    # Broker-side truth moves with no execution to explain it.
    order = broker.orders_by_ref(ref)[0]
    broker.inject_position_drift(order.contract11, delta=+2)

    recon = await reconcile(trader.ib, ledger)
    assert not recon.clean, "reconciler must flag unexplained drift"
    (div,) = recon.divergences
    assert div.symbol == "ES"
    assert div.expected == 3.0 and div.actual == 5.0 and div.delta == 2.0

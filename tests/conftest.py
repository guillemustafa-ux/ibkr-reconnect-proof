import asyncio

import pytest
from ib_async import Contract

from ibkr_reconnect_proof.fake_broker import FakeBroker
from ibkr_reconnect_proof.ledger import OrderLedger
from ibkr_reconnect_proof.trader import ResilientTrader

CLIENT_ID = 7


def es_future() -> Contract:
    """A fully-specified contract so no qualification round-trip is needed."""
    return Contract(
        secType="FUT", conId=495512563, symbol="ES",
        lastTradeDateOrContractMonth="20261218", multiplier="50",
        exchange="CME", currency="USD", localSymbol="ESZ6", tradingClass="ES",
    )


@pytest.fixture
async def broker():
    b = FakeBroker()
    await b.start()
    yield b
    await b.stop()


@pytest.fixture
def ledger(tmp_path):
    ledger = OrderLedger(str(tmp_path / "orders.db"))
    yield ledger
    ledger.close()


@pytest.fixture
async def trader(broker, ledger):
    t = ResilientTrader(ledger, "127.0.0.1", broker.port, CLIENT_ID)
    await t.connect()
    yield t
    t.disconnect()
    await asyncio.sleep(0)


async def wait_disconnected(trader, timeout=2.0):
    """Wait until the client has noticed the socket is gone."""
    deadline = asyncio.get_event_loop().time() + timeout
    while trader.connected:
        assert asyncio.get_event_loop().time() < deadline, "still connected"
        await asyncio.sleep(0.01)

"""Live drill against a REAL IB Gateway paper account (phase 2).

Runs scenario 1 end to end through the chaos proxy:

1. connect through the proxy, arm the fault;
2. place a resting limit order (far from the market on purpose);
3. the proxy cuts the wire at the worst moment around placeOrder;
4. reconnect DIRECTLY to the gateway (the outage is over);
5. recovery interrogates the gateway for the order's fate and prints the
   verdict, then reconciliation runs;
6. cleanup: the drill cancels its own order, leaving the account flat.

Safety: paper accounts only. The script refuses accounts not starting with
"DU". Configuration comes from environment variables (see env.example).

Usage:
    python scripts/live_drill.py --fault after
    python scripts/live_drill.py --fault before --symbol AAPL --price 100 --qty 1
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ib_async import Stock

from ibkr_reconnect_proof.chaos_proxy import ChaosProxy
from ibkr_reconnect_proof.ledger import OrderLedger
from ibkr_reconnect_proof.reconcile import reconcile
from ibkr_reconnect_proof.trader import ResilientTrader


def env(name: str, default: str) -> str:
    # .env values quoted on some platforms are read with literal quotes;
    # strip them so comparisons and ints behave.
    return (os.environ.get(name, default) or default).replace('"', "")


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fault", choices=["before", "after"], default="after")
    parser.add_argument("--symbol", default="AAPL")
    parser.add_argument("--price", type=float, default=100.0,
                        help="limit price; keep it FAR below the market so"
                             " the order rests instead of filling")
    parser.add_argument("--qty", type=float, default=1)
    args = parser.parse_args()

    host = env("IB_HOST", "127.0.0.1")
    port = int(env("IB_PORT", "4002"))
    client_id = int(env("IB_CLIENT_ID", "17"))

    ledger = OrderLedger(f"drill-{time.strftime('%Y%m%d-%H%M%S')}.db")
    proxy = ChaosProxy(host, port)
    await proxy.start()

    print(f"[drill] connecting via chaos proxy 127.0.0.1:{proxy.port}"
          f" -> {host}:{port}")
    trader = ResilientTrader(ledger, "127.0.0.1", proxy.port, client_id)
    await trader.connect(timeout=15)

    accounts = trader.ib.client.getAccounts()
    if not all(a.startswith("DU") for a in accounts):
        trader.disconnect()
        sys.exit(f"[drill] ABORT: {accounts} is not a paper account (DU...)")

    proxy.arm(args.fault)
    contract = Stock(args.symbol, "SMART", "USD")
    print(f"[drill] placing BUY {args.qty} {args.symbol} LMT {args.price}"
          f" with fault '{args.fault}' armed")
    ref, _trade = await trader.place_limit(contract, "BUY", args.qty, args.price)
    print(f"[drill] ledger reserved order_ref={ref} BEFORE the socket write")

    await asyncio.wait_for(proxy.triggered.wait(), timeout=30)
    while trader.connected:
        await asyncio.sleep(0.05)
    print(f"[drill] connection lost; ledger state:"
          f" {ledger.get(ref).state} (fate unknown)")

    # Let the outage last a few real seconds. Reconnecting within
    # milliseconds of the cut hits a different (and nastier) gateway edge:
    # the half-processed order of the dead session gets purged while its
    # orderId stays consumed, so the resend is rejected with error 103.
    # Even then no duplicate is ever created — but the scenario under
    # proof here is a real outage, after which the gateway has settled.
    print("[drill] outage in progress; waiting 5s before reconnecting...")
    await asyncio.sleep(5)
    print("[drill] reconnecting DIRECTLY to the gateway...")
    trader.host, trader.port = host, port
    # The real gateway keeps the dead session's clientId registered for a
    # few seconds after the cut (error 326), unlike the fake broker which
    # frees it instantly. Retry until the gateway releases the id.
    for attempt in range(1, 7):
        try:
            await trader.connect(timeout=15)
            break
        except (TimeoutError, OSError) as exc:
            trader.disconnect()
            print(f"[drill] reconnect attempt {attempt} failed ({exc!r});"
                  " gateway still holds the old session, retrying in 5s")
            await asyncio.sleep(5)
    else:
        sys.exit("[drill] ABORT: gateway never released the clientId")
    report = await trader.recover({args.symbol: contract})
    print(f"[drill] recovery report: adopted={report.adopted}"
          f" resent={report.resent} filled={report.filled}"
          f" missing={report.missing}")

    ours = [t for t in trader.ib.openTrades() if t.order.orderRef == ref]
    print(f"[drill] open orders at gateway with our ref: {len(ours)}"
          f" (must be exactly 1 — no duplicate, no loss)")

    recon = await reconcile(trader.ib, ledger)
    print(f"[drill] reconciliation: clean={recon.clean}"
          f" divergences={recon.divergences}")

    for trade in ours:
        print(f"[drill] cleanup: cancelling orderId={trade.order.orderId}")
        trader.ib.cancelOrder(trade.order)
    await asyncio.sleep(1)
    trader.disconnect()
    await proxy.stop()
    ledger.close()
    print("[drill] done — account left flat")


if __name__ == "__main__":
    asyncio.run(main())

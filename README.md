# ibkr-reconnect-proof

A chaos harness for the failure that actually costs money with the
Interactive Brokers TWS API: **the connection dies between `placeOrder` and
its acknowledgement, and the operator's instinctive retry doubles the
position.**

This repo does not demonstrate "connecting to IBKR". It demonstrates, with
runnable evidence, that a client built on a durable order ledger:

1. **never duplicates an order** across disconnects — including the
   in-flight case where the broker's fate of the order is unknowable;
2. **re-syncs its state from the broker** after reconnecting, adopting
   working orders instead of resending them;
3. **detects any divergence** between what the system believes and what the
   broker reports, with the exact delta.

You can verify all of it in about five minutes, on your machine, **without
an IBKR account** (phase 1), and then reproduce the same drill against your
own paper Gateway (phase 2). No credentials of mine are involved anywhere.

## Why this is hard with IBKR specifically

The TWS API has **no idempotency key**. The only handle on an order is its
`orderId`, and its semantics are sharp:

* `placeOrder` with an id the gateway **already knows** is a **modify**;
* `placeOrder` with an id it never saw is a **new order**;
* so the same retry does two different things depending on state you
  cannot observe during an outage.

Three more traps compound it:

* the TCP write succeeding only proves the bytes reached your kernel's
  buffer — not that the gateway parsed the order;
* `nextValidId` after reconnect is a hint, not a contract — trusting it can
  reuse the id of an order whose fate is unknown, silently rewriting a live
  order (modify semantics again);
* `reqOpenOrders` only returns orders of **your current clientId**;
  fills that happened while you were disconnected only surface through
  execution replay.

## The design under test

* **Reserve before send.** `(orderId, orderRef)` is committed to SQLite
  (WAL, `synchronous=FULL`) **before** the first byte hits the socket.
  Reserve after send and a crash leaves you with nothing to ask the broker
  about.
* **`orderRef` as idempotency key.** IBKR echoes it back in `openOrder`
  and `execDetails`, and it survives client restarts. Recovery matches by
  it, not by in-memory state.
* **Interrogate-first recovery.** After reconnecting, every unresolved
  order is resolved against the broker — execution replay, then open
  orders — and only a **proven-absent** order is resent, with the **same**
  `orderId` and `orderRef`. Blind resends are the bug, not the fix.
* **Idempotent fill accounting.** Executions are keyed by `execId`, so
  IB's replay on every reconnect can never double-count a contract.
* **Reconciliation.** The ledger implies a net position per symbol; the
  broker states its own; the reconciler reports every mismatch with its
  delta — and stays quiet when a fill during the outage explains the
  difference.
* **A ledger-says-working order the broker has no trace of** is reported
  as `missing` for a human decision — never resent on a guess.

## Verify it in five minutes (no IBKR account)

```bash
git clone https://github.com/guillemustafa-ux/ibkr-reconnect-proof
cd ibkr-reconnect-proof
python -m venv .venv && . .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -e ".[dev]"
pytest -v --log-cli-level=INFO
```

The suite runs the real `ib_async` client stack against a **wire-level fake
IB Gateway** (server version 178) that speaks the actual TWS socket
protocol and injects faults at the exact protocol step where money gets
lost. Expected evidence:

```text
reserved order_id=3 order_ref=rcp-aeffa959c3cc (durable, pre-send)
recovery: rcp-aeffa959c3cc proven absent at broker -> RESEND (same orderId=3)
PASSED test_order_lost_in_transit_is_resent_exactly_once

reserved order_id=3 order_ref=rcp-ea348edd9291 (durable, pre-send)
recovery: rcp-ea348edd9291 found alive at broker -> ADOPT
PASSED test_order_reached_broker_ack_lost_is_adopted_not_resent

PASSED test_naive_retry_duplicates_the_position

PASSED test_working_order_survives_drop_and_is_rebound
PASSED test_ledger_floor_prevents_order_id_reuse

recovery: rcp-413b6fbc5930 filled during outage -> FILLED
PASSED test_fill_during_outage_is_recovered_and_reconciles_clean
PASSED test_execution_replay_is_idempotent
PASSED test_unexplained_broker_drift_is_detected

8 passed
```

### What each scenario proves

| # | Test | Cut point | Proven |
|---|------|-----------|--------|
| 1a | `test_order_lost_in_transit_is_resent_exactly_once` | after the socket write, **before** the gateway parses the order | recovery proves absence, resends the **same reserved orderId** once; broker ends with exactly one order |
| 1b | `test_order_reached_broker_ack_lost_is_adopted_not_resent` | after the gateway registers the order, **before** the ack returns | recovery finds the order alive and adopts it; **zero** resends |
| 1c | `test_naive_retry_duplicates_the_position` | same as 1b | **the anti-pattern, demonstrated**: a blind retry under a fresh orderId leaves two live orders — the failure this harness exists to catch |
| 2a | `test_working_order_survives_drop_and_is_rebound` | mid-session, order working | after a crash-restart reconnect, the client's view converges to the broker's |
| 2b | `test_ledger_floor_prevents_order_id_reuse` | in-flight order lost | the id sequence starts above the ledger's high-water mark, ignoring the broker's stale `nextValidId` hint |
| 3a | `test_fill_during_outage_is_recovered_and_reconciles_clean` | order fills while disconnected | execution replay folds the fill into the ledger; reconciliation reports **clean** — no false alarm |
| 3b | `test_execution_replay_is_idempotent` | two extra reconnects | replayed executions never double-count a contract |
| 3c | `test_unexplained_broker_drift_is_detected` | broker position moves with **no** execution behind it | reconciliation flags the divergence with the exact delta (`expected=3 actual=5 delta=+2`) |

Every (re)connect in the suite builds a **fresh client instance**: recovery
is driven by the durable ledger alone, exactly as after a process crash.
In-memory state is not allowed to help.

## Phase 2 — the same drill against a real paper Gateway

The fake broker proves the client logic under deterministic faults. It is
**not** IB's matching engine — so the repo includes a chaos TCP proxy to
run the same in-flight drill against Interactive Brokers' real paper
infrastructure, with your own account, on your own machine.

Requirements: IB Gateway (or TWS) logged into a **paper** account
(`DU...`), API enabled on localhost.

```bash
cp env.example .env        # adjust host/port/client id, then export them
python scripts/live_drill.py --fault after   # or --fault before
```

The drill connects through the proxy, places a far-from-market resting
limit order, and the proxy kills both sockets at the armed moment around
the `placeOrder` frame. It then reconnects **directly** to the gateway,
runs recovery, prints the verdict, verifies exactly one order exists,
reconciles, and cancels its own order — leaving the account flat.

```text
[drill] ledger reserved order_ref=rcp-... BEFORE the socket write
[chaos] placeOrder id=... forwarded; cutting the wire before the ack returns
[drill] connection lost; ledger state: SENT (fate unknown)
[drill] reconnecting DIRECTLY to the gateway...
[drill] recovery report: adopted=['rcp-...'] resent=[] filled=[] missing=[]
[drill] open orders at gateway with our ref: 1 (must be exactly 1 — no duplicate, no loss)
[drill] reconciliation: clean=True divergences=[]
[drill] cleanup: cancelling orderId=...
[drill] done — account left flat
```

The proxy also runs standalone if you want to point your own client
through it:

```bash
python -m ibkr_reconnect_proof.chaos_proxy --listen-port 4003 \
    --target-port 4002 --fault after
```

## Safety

* Paper accounts only. The drill hard-aborts unless every account id
  starts with `DU`.
* No credentials anywhere in this repo: configuration is environment
  variables only (see `env.example`).
* The drill uses resting limit orders far from the market and cancels them
  on exit.

## Layout

```
ibkr_reconnect_proof/
  ledger.py        # durable order ledger: reserve-before-send, execId-keyed fills
  trader.py        # resilient client + interrogate-first recovery
  reconcile.py     # ledger view vs broker view, divergence report
  fake_broker.py   # wire-level fake IB Gateway (server v178) with fault injection
  chaos_proxy.py   # TCP proxy that cuts a REAL gateway connection around placeOrder
  wire.py          # TWS socket protocol framing
scripts/
  live_drill.py    # phase-2 drill against a real paper gateway
tests/
  test_scenario_1_inflight_order.py
  test_scenario_2_resync_open_orders.py
  test_scenario_3_reconciliation.py
```

Python 3.10+, [`ib_async`](https://github.com/ib-api-reloaded/ib_async)
(the maintained continuation of `ib_insync`). The fake broker's frame
layouts mirror `ib_async.decoder.Decoder` field by field for server
version 178.

## Scope, honestly

This proof covers connection-level failure handling: in-flight orders,
re-sync, replay idempotency, reconciliation. It deliberately does not
cover strategy logic, market data handling, IB's daily restart windows, or
multi-account routing — those are engineering, not proof. The fake broker
implements the protocol subset the scenarios exercise, nothing more.

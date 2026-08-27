"""Durable order ledger — the idempotency layer the TWS API does not give you.

The TWS API has no idempotency key. The only handle on an order is its
``orderId``, and its semantics are sharp: sending ``placeOrder`` with an id
the gateway already knows is a MODIFY; with an id it never saw, it is a new
order. The same retry does two different things depending on state you
cannot observe during an outage.

This ledger closes that gap with two rules:

1. **Reserve before send.** The (orderId, orderRef) pair is committed to
   disk BEFORE the first byte goes to the socket. If the process dies
   mid-send, recovery knows exactly which id to interrogate the broker
   about. Reserve after send and you have nothing to ask with.

2. **orderRef is the idempotency key.** IBKR echoes ``orderRef`` back in
   openOrder and execDetails, and — unlike orderId — it stays attached to
   the order across client sessions. Recovery matches by orderRef, so it
   works even where orderId bookkeeping gets murky.

States: RESERVED -> SENT -> ACKED -> FILLED | CANCELLED
An order that never leaves RESERVED/SENT after a disconnect is exactly the
in-flight case recovery must resolve against the broker.
"""

from __future__ import annotations

import sqlite3
import time
import uuid
from dataclasses import dataclass

RESERVED = "RESERVED"
SENT = "SENT"
ACKED = "ACKED"
FILLED = "FILLED"
CANCELLED = "CANCELLED"

PENDING_STATES = (RESERVED, SENT)


@dataclass
class LedgerOrder:
    order_ref: str
    order_id: int
    state: str
    symbol: str
    action: str
    quantity: float
    limit_price: float
    attempts: int


@dataclass
class LedgerFill:
    exec_id: str
    order_ref: str
    side: str
    shares: float
    price: float


class OrderLedger:
    """SQLite-backed ledger. WAL + synchronous=FULL: a committed reserve
    survives a process kill."""

    def __init__(self, path: str) -> None:
        self._db = sqlite3.connect(path)
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA synchronous=FULL")
        self._db.executescript(
            """
            CREATE TABLE IF NOT EXISTS orders (
                order_ref   TEXT PRIMARY KEY,
                order_id    INTEGER NOT NULL UNIQUE,
                state       TEXT NOT NULL,
                symbol      TEXT NOT NULL,
                action      TEXT NOT NULL,
                quantity    REAL NOT NULL,
                limit_price REAL NOT NULL,
                attempts    INTEGER NOT NULL DEFAULT 0,
                created_at  TEXT NOT NULL,
                updated_at  TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS fills (
                exec_id   TEXT PRIMARY KEY,
                order_ref TEXT NOT NULL REFERENCES orders(order_ref),
                side      TEXT NOT NULL,
                shares    REAL NOT NULL,
                price     REAL NOT NULL,
                filled_at TEXT NOT NULL
            );
            """
        )
        self._db.commit()

    def close(self) -> None:
        self._db.close()

    # ------------------------------------------------------------- lifecycle

    def reserve(
        self, order_id: int, symbol: str, action: str,
        quantity: float, limit_price: float,
    ) -> str:
        """Durably reserve an orderId BEFORE any socket write.

        Returns the generated orderRef (the idempotency key).
        """
        order_ref = f"rcp-{uuid.uuid4().hex[:12]}"
        now = _now()
        self._db.execute(
            "INSERT INTO orders (order_ref, order_id, state, symbol, action,"
            " quantity, limit_price, attempts, created_at, updated_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?, ?)",
            (order_ref, order_id, RESERVED, symbol, action,
             quantity, limit_price, now, now),
        )
        self._db.commit()
        return order_ref

    def mark_sent(self, order_ref: str) -> None:
        self._set_state(order_ref, SENT, bump_attempt=True)

    def mark_acked(self, order_ref: str) -> None:
        self._set_state(order_ref, ACKED)

    def mark_filled(self, order_ref: str) -> None:
        self._set_state(order_ref, FILLED)

    def mark_cancelled(self, order_ref: str) -> None:
        self._set_state(order_ref, CANCELLED)

    def record_fill(
        self, exec_id: str, order_ref: str, side: str, shares: float, price: float,
    ) -> bool:
        """Record an execution, idempotently keyed by execId.

        IB replays executions after reconnect; the primary key makes the
        replay harmless. Returns True only for a first-seen execution.
        """
        cursor = self._db.execute(
            "INSERT OR IGNORE INTO fills (exec_id, order_ref, side, shares,"
            " price, filled_at) VALUES (?, ?, ?, ?, ?, ?)",
            (exec_id, order_ref, side, shares, price, _now()),
        )
        self._db.commit()
        is_new = cursor.rowcount == 1
        if is_new:
            order = self.get(order_ref)
            if order and self.filled_quantity(order_ref) >= order.quantity:
                self.mark_filled(order_ref)
        return is_new

    # ---------------------------------------------------------------- queries

    def get(self, order_ref: str) -> LedgerOrder | None:
        row = self._db.execute(
            "SELECT order_ref, order_id, state, symbol, action, quantity,"
            " limit_price, attempts FROM orders WHERE order_ref = ?",
            (order_ref,),
        ).fetchone()
        return LedgerOrder(*row) if row else None

    def pending(self) -> list[LedgerOrder]:
        """Orders whose broker-side fate is unknown (RESERVED or SENT)."""
        rows = self._db.execute(
            "SELECT order_ref, order_id, state, symbol, action, quantity,"
            " limit_price, attempts FROM orders WHERE state IN (?, ?)"
            " ORDER BY order_id",
            PENDING_STATES,
        ).fetchall()
        return [LedgerOrder(*row) for row in rows]

    def working(self) -> list[LedgerOrder]:
        """Orders last known to be live at the broker (ACKED)."""
        rows = self._db.execute(
            "SELECT order_ref, order_id, state, symbol, action, quantity,"
            " limit_price, attempts FROM orders WHERE state = ?"
            " ORDER BY order_id",
            (ACKED,),
        ).fetchall()
        return [LedgerOrder(*row) for row in rows]

    def fills(self, order_ref: str) -> list[LedgerFill]:
        rows = self._db.execute(
            "SELECT exec_id, order_ref, side, shares, price FROM fills"
            " WHERE order_ref = ?",
            (order_ref,),
        ).fetchall()
        return [LedgerFill(*row) for row in rows]

    def filled_quantity(self, order_ref: str) -> float:
        (total,) = self._db.execute(
            "SELECT COALESCE(SUM(shares), 0) FROM fills WHERE order_ref = ?",
            (order_ref,),
        ).fetchone()
        return total

    def high_water(self) -> int:
        """Highest orderId ever reserved. The broker's nextValidId hint is
        advisory; this is the floor that prevents id reuse."""
        (max_id,) = self._db.execute(
            "SELECT COALESCE(MAX(order_id), 0) FROM orders"
        ).fetchone()
        return max_id

    def expected_positions(self) -> dict[str, float]:
        """Net signed position per symbol implied by recorded fills."""
        rows = self._db.execute(
            "SELECT o.symbol,"
            " SUM(CASE WHEN f.side = 'BOT' THEN f.shares ELSE -f.shares END)"
            " FROM fills f JOIN orders o ON o.order_ref = f.order_ref"
            " GROUP BY o.symbol"
        ).fetchall()
        return {symbol: net for symbol, net in rows if net}

    # ---------------------------------------------------------------- private

    def _set_state(self, order_ref: str, state: str, bump_attempt: bool = False):
        bump = ", attempts = attempts + 1" if bump_attempt else ""
        self._db.execute(
            f"UPDATE orders SET state = ?, updated_at = ?{bump}"
            " WHERE order_ref = ?",
            (state, _now(), order_ref),
        )
        self._db.commit()


def _now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S")

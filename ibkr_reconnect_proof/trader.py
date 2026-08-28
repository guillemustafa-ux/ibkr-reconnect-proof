"""Resilient trading client: reserve -> send -> confirm, and interrogate-first
recovery after a disconnect.

The recovery rule this module exists for: after an outage, NEVER blindly
resend an unconfirmed order. First ask the broker for the fate of the exact
orderId/orderRef you reserved (open orders + execution replay), and only
resend once the order is proven absent. Blind resends are the bug, not the
fix — with the TWS API a resend of a known orderId is a MODIFY, and a resend
under a fresh orderId is a duplicate position.

Each (re)connect builds a fresh ``IB`` instance on purpose: recovery must
work from the durable ledger alone, as after a process crash — in-memory
state is not allowed to help.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field

from ib_async import IB, Contract, Fill, LimitOrder, Trade

from .ledger import ACKED, FILLED, RESERVED, SENT, OrderLedger

log = logging.getLogger("reconnect_proof.trader")

ACK_STATUSES = {"PreSubmitted", "Submitted"}


@dataclass
class RecoveryReport:
    adopted: list[str] = field(default_factory=list)  # in-flight, found alive at broker
    filled: list[str] = field(default_factory=list)   # resolved as filled during outage
    resent: list[str] = field(default_factory=list)   # in-flight, proven absent, resent
    missing: list[str] = field(default_factory=list)  # ledger says working, broker has
    #                                                   no trace: escalate, never resend
    new_fills_recorded: int = 0


class ResilientTrader:
    def __init__(self, ledger: OrderLedger, host: str, port: int, client_id: int):
        self.ledger = ledger
        self.host = host
        self.port = port
        self.client_id = client_id
        self.ib: IB | None = None
        self._inflight_at_connect: list[str] = []
        self._working_at_connect: list[str] = []

    # ------------------------------------------------------------ connection

    async def connect(self, timeout: float = 4) -> None:
        """Connect with a fresh IB instance (crash-restart semantics)."""
        # Snapshot what is unresolved BEFORE the connection sync runs:
        # ack/fill events arriving during the sync legitimately advance the
        # ledger, and recovery must still report how each order got resolved.
        self._inflight_at_connect = [o.order_ref for o in self.ledger.pending()]
        self._working_at_connect = [o.order_ref for o in self.ledger.working()]
        self.ib = IB()
        self.ib.orderStatusEvent += self._on_order_status
        self.ib.execDetailsEvent += self._on_exec_details
        await self.ib.connectAsync(
            self.host, self.port, clientId=self.client_id, timeout=timeout
        )
        # The broker's nextValidId is only a hint. The ledger's high-water
        # mark is the floor: never hand out an id whose fate is unknown.
        self.ib.client.updateReqId(self.ledger.high_water() + 1)

    def disconnect(self) -> None:
        if self.ib is not None:
            self.ib.disconnect()

    @property
    def connected(self) -> bool:
        return self.ib is not None and self.ib.isConnected()

    # ----------------------------------------------------------------- orders

    async def place_limit(
        self, contract: Contract, action: str, quantity: float, price: float,
    ) -> tuple[str, Trade]:
        """Reserve durably, then send. Returns (orderRef, Trade)."""
        assert self.ib is not None
        order_id = self.ib.client.getReqId()
        order_ref = self.ledger.reserve(
            order_id, contract.symbol, action, quantity, price
        )
        log.info("reserved order_id=%s order_ref=%s (durable, pre-send)",
                 order_id, order_ref)
        # tif must be explicit: when the gateway fills it from an order
        # preset it emits notice 10349, which ib_async does not know as a
        # warning and mislabels the live trade as Cancelled.
        order = LimitOrder(
            action, quantity, price, orderId=order_id, orderRef=order_ref,
            tif="DAY",
        )
        trade = self.ib.placeOrder(contract, order)
        self.ledger.mark_sent(order_ref)
        return order_ref, trade

    # --------------------------------------------------------------- recovery

    async def recover(self, contract_by_symbol: dict[str, Contract]) -> RecoveryReport:
        """Resolve every order whose fate the outage left uncertain.

        For an order that was in flight (RESERVED/SENT at reconnect),
        strictly in this priority:
          1. execution replay shows it filled -> record, done
          2. broker shows it alive            -> adopt it, do NOT resend
          3. proven absent everywhere         -> resend, SAME id and ref

        For an order that was working (ACKED at reconnect):
          filled during the outage -> resolved by execution replay;
          still open at the broker -> nothing to do;
          vanished without a fill  -> report as missing, never resend.
        """
        assert self.ib is not None
        report = RecoveryReport()

        # Fold everything the connection sync already learned into the
        # ledger. Execution replay is idempotent thanks to execId keying.
        for fill in self.ib.fills():
            if self._record_fill(fill):
                report.new_fills_recorded += 1

        open_trades = await self.ib.reqAllOpenOrdersAsync()
        open_refs = {t.order.orderRef for t in open_trades}

        for ref in self._inflight_at_connect:
            order = self.ledger.get(ref)
            if order.state == FILLED:
                log.info("recovery: %s resolved by execution replay -> FILLED", ref)
                report.filled.append(ref)
            elif order.state == ACKED or ref in open_refs:
                log.info("recovery: %s found alive at broker -> ADOPT", ref)
                self.ledger.mark_acked(ref)
                report.adopted.append(ref)
            else:
                log.info("recovery: %s proven absent at broker -> RESEND"
                         " (same orderId=%s)", ref, order.order_id)
                contract = contract_by_symbol[order.symbol]
                resend = LimitOrder(
                    order.action, order.quantity, order.limit_price,
                    orderId=order.order_id, orderRef=ref, tif="DAY",
                )
                self.ib.placeOrder(contract, resend)
                self.ledger.mark_sent(ref)
                report.resent.append(ref)

        for ref in self._working_at_connect:
            order = self.ledger.get(ref)
            if order.state == FILLED:
                log.info("recovery: %s filled during outage -> FILLED", ref)
                report.filled.append(ref)
            elif order.state == ACKED and ref not in open_refs:
                # The ledger believes this order is live; the broker has no
                # trace and no fill explains it. Resending would be a guess —
                # this is a human decision, not a retry.
                log.warning("recovery: %s working per ledger, unknown to"
                            " broker -> MISSING", ref)
                report.missing.append(ref)

        # Recovery is not done until every resend is acknowledged again —
        # reporting success on an unconfirmed resend would recreate the
        # exact ambiguity this module exists to remove.
        if report.resent:
            await self._wait_for_acks(report.resent)
        return report

    async def _wait_for_acks(self, refs: list[str], timeout: float = 3.0) -> None:
        deadline = asyncio.get_event_loop().time() + timeout
        for ref in refs:
            while self.ledger.get(ref).state == SENT:
                if asyncio.get_event_loop().time() > deadline:
                    raise TimeoutError(f"resent order {ref} was never re-acked")
                await asyncio.sleep(0.01)

    # ----------------------------------------------------------------- events

    def _on_order_status(self, trade: Trade) -> None:
        ref = trade.order.orderRef
        if not ref or not self.ledger.get(ref):
            return
        status = trade.orderStatus.status
        state = self.ledger.get(ref).state
        if status in ACK_STATUSES and state in (SENT, RESERVED):
            self.ledger.mark_acked(ref)
        elif status == "Cancelled" and state != FILLED:
            self.ledger.mark_cancelled(ref)

    def _on_exec_details(self, trade: Trade, fill: Fill) -> None:
        self._record_fill(fill)

    def _record_fill(self, fill: Fill) -> bool:
        execution = fill.execution
        ref = execution.orderRef
        if not ref or not self.ledger.get(ref):
            return False
        return self.ledger.record_fill(
            execution.execId, ref, execution.side,
            execution.shares, execution.price,
        )

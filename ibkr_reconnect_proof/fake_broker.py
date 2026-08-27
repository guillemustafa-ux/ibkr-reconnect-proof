"""A wire-level fake IB Gateway with deterministic fault injection.

This is NOT a mock of ib_async. It is a TCP server that speaks the real TWS
API socket protocol (server version 178), so the client under test runs the
genuine ``ib_async`` stack end to end: handshake, startApi, order placement,
open-order/execution/position sync. The frame layouts mirror
``ib_async.decoder.Decoder`` field by field.

What makes it a chaos harness rather than a simulator:

* Faults are injected at the exact protocol step where real money gets lost —
  around ``placeOrder`` — via one-shot fault modes.
* Broker-side state (orders, executions, positions) survives connection drops,
  exactly like the real gateway: only the socket dies, not the order book.
* Orders can be filled *while the client is disconnected*; like the real IB,
  those fills only become visible to the client through execution replay
  after reconnecting.

Supported fault modes (one-shot, armed via :meth:`FakeBroker.arm_fault`):

``drop_before_process``
    The placeOrder frame is read off the socket and discarded, then the
    connection is dropped. Models the order dying in transit: the client's
    TCP write succeeded, the broker never saw the order.

``drop_after_process``
    The order is registered in the broker's book, then the connection is
    dropped before any acknowledgement is sent. Models the ack dying in
    transit: the broker is working the order, the client cannot know.
"""

from __future__ import annotations

import asyncio
import itertools
import time
from dataclasses import dataclass, field

from .wire import (
    API_HELLO,
    MSG_CANCEL_ORDER,
    MSG_PLACE_ORDER,
    MSG_REQ_ACCT_DATA,
    MSG_REQ_ACCT_UPDATES_MULTI,
    MSG_REQ_ALL_OPEN_ORDERS,
    MSG_REQ_COMPLETED_ORDERS,
    MSG_REQ_CURRENT_TIME,
    MSG_REQ_EXECUTIONS,
    MSG_REQ_IDS,
    MSG_REQ_OPEN_ORDERS,
    MSG_REQ_POSITIONS,
    MSG_START_API,
    decode_payload,
    encode_frame,
    read_frame,
)

SERVER_VERSION = 178
ACCOUNT = "DU1234567"


@dataclass
class BrokerOrder:
    order_id: int
    client_id: int
    perm_id: int
    contract: list[str]  # 12 raw wire fields as received in placeOrder
    action: str
    quantity: float
    order_type: str
    lmt_price: str
    aux_price: str
    tif: str
    account: str
    order_ref: str
    status: str = "Submitted"
    filled: float = 0.0

    @property
    def remaining(self) -> float:
        return self.quantity - self.filled

    # openOrder / execDetails / position echo 11 contract fields
    # (placeOrder carries 12; primaryExchange, index 8, is not echoed back).
    @property
    def contract11(self) -> list[str]:
        c = self.contract
        return c[:8] + c[9:]


@dataclass
class BrokerExecution:
    exec_id: str
    order: BrokerOrder
    shares: float
    price: float
    cum_qty: float
    avg_price: float
    time_str: str


@dataclass
class BrokerPosition:
    contract11: list[str]
    position: float = 0.0
    avg_cost: float = 0.0


class FakeBroker:
    """Deterministic in-process IB Gateway double."""

    def __init__(self) -> None:
        self.orders: dict[int, BrokerOrder] = {}  # keyed by orderId
        self.executions: list[BrokerExecution] = []
        self.positions: dict[str, BrokerPosition] = {}  # keyed by conId
        self.place_order_count: dict[str, int] = {}  # orderRef -> times received
        self._fault: str | None = None
        self._next_valid_id = 1
        self._perm_ids = itertools.count(1_000_000_001)
        self._exec_ids = itertools.count(1)
        self._server: asyncio.Server | None = None
        self._writers: dict[asyncio.StreamWriter, int] = {}  # writer -> clientId
        self.port: int = 0

    # ------------------------------------------------------------------ setup

    async def start(self) -> None:
        self._server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        self.port = self._server.sockets[0].getsockname()[1]

    async def stop(self) -> None:
        self.drop_connections()
        if self._server:
            self._server.close()
            # Note: Server.wait_closed() on Python 3.12+ waits for every
            # handler task; a handler blocked on a half-open socket would
            # hang test teardown. Closing the server and the writers is
            # enough for a test double.
            await asyncio.sleep(0)

    # ---------------------------------------------------------------- control

    def arm_fault(self, mode: str) -> None:
        """Arm a one-shot fault, consumed by the next placeOrder frame."""
        assert mode in ("drop_before_process", "drop_after_process")
        self._fault = mode

    def drop_connections(self) -> None:
        """Kill every live client socket with a hard RST. Broker state is
        untouched — exactly like a network drop against the real gateway."""
        for writer in list(self._writers):
            writer.transport.abort()
        self._writers.clear()

    def fill(self, order_ref: str, price: float) -> None:
        """Fully fill a working order at ``price``.

        If a client is connected, the execution and status are pushed live.
        If not, the fill exists only broker-side — like the real IB, the
        client learns about it through execution replay after reconnecting.
        """
        order = self._order_by_ref(order_ref)
        shares = order.remaining
        order.filled = order.quantity
        order.status = "Filled"
        execution = BrokerExecution(
            exec_id=f"0000e0d5.{next(self._exec_ids):08x}.01.01",
            order=order,
            shares=shares,
            price=price,
            cum_qty=order.quantity,
            avg_price=price,
            time_str=time.strftime("%Y%m%d %H:%M:%S"),
        )
        self.executions.append(execution)
        self._apply_fill_to_position(order, shares, price)
        for writer, client_id in list(self._writers.items()):
            if client_id == order.client_id:
                self._send(writer, self._exec_details_fields(-1, execution))
                self._send(writer, self._order_status_fields(order))

    def inject_position_drift(self, contract11: list[str], delta: float) -> None:
        """Mutate a broker position WITHOUT any execution backing it.

        Models the divergence class a reconciler must catch: a manual trade
        in TWS, a broker-side correction, a missed message — the broker's
        truth moved and no API event explains it.
        """
        pos = self._position_for(contract11)
        pos.position += delta

    # ----------------------------------------------------------- introspection

    def open_orders(self) -> list[BrokerOrder]:
        return [o for o in self.orders.values() if o.status == "Submitted"]

    def orders_by_ref(self, order_ref: str) -> list[BrokerOrder]:
        return [o for o in self.orders.values() if o.order_ref == order_ref]

    def _order_by_ref(self, order_ref: str) -> BrokerOrder:
        matches = self.orders_by_ref(order_ref)
        assert len(matches) == 1, f"expected 1 order for {order_ref}, got {len(matches)}"
        return matches[0]

    # ------------------------------------------------------------- connection

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        try:
            hello = await reader.readexactly(len(API_HELLO))
            if hello != API_HELLO:
                return
            await read_frame(reader)  # client version range, e.g. v157..178
            conn_time = time.strftime("%Y%m%d %H:%M:%S") + " UTC"
            self._send(writer, [SERVER_VERSION, conn_time])

            client_id = -1
            while True:
                payload = await read_frame(reader)
                fields = decode_payload(payload)
                msg_id = int(fields[0])

                if msg_id == MSG_START_API:
                    client_id = int(fields[2])
                    self._writers[writer] = client_id
                    self._send(writer, [9, 1, self._next_valid_id])
                    self._send(writer, [15, 1, ACCOUNT])

                elif msg_id == MSG_PLACE_ORDER:
                    if self._fault == "drop_before_process":
                        # Order dies in transit: read, discard, cut the wire.
                        self._fault = None
                        break
                    order = self._register_order(fields, client_id)
                    if self._fault == "drop_after_process":
                        # Broker works the order; the ack never leaves.
                        self._fault = None
                        break
                    self._send(writer, self._open_order_fields(order))
                    self._send(writer, self._order_status_fields(order))

                elif msg_id == MSG_CANCEL_ORDER:
                    order = self.orders.get(int(fields[2]))
                    if order and order.status == "Submitted":
                        order.status = "Cancelled"
                        self._send(writer, self._order_status_fields(order))

                elif msg_id == MSG_REQ_OPEN_ORDERS:
                    for order in self.open_orders():
                        if order.client_id == client_id:
                            self._send(writer, self._open_order_fields(order))
                            self._send(writer, self._order_status_fields(order))
                    self._send(writer, [53, 1])

                elif msg_id == MSG_REQ_ALL_OPEN_ORDERS:
                    for order in self.open_orders():
                        self._send(writer, self._open_order_fields(order))
                        self._send(writer, self._order_status_fields(order))
                    self._send(writer, [53, 1])

                elif msg_id == MSG_REQ_EXECUTIONS:
                    req_id = int(fields[2])
                    for execution in self.executions:
                        self._send(writer, self._exec_details_fields(req_id, execution))
                    self._send(writer, [55, 1, req_id])

                elif msg_id == MSG_REQ_POSITIONS:
                    for pos in self.positions.values():
                        if pos.position:
                            self._send(
                                writer,
                                [61, 3, ACCOUNT, *pos.contract11,
                                 pos.position, pos.avg_cost],
                            )
                    self._send(writer, [62, 1])

                elif msg_id == MSG_REQ_IDS:
                    self._send(writer, [9, 1, self._next_valid_id])

                elif msg_id == MSG_REQ_ACCT_DATA:
                    self._send(writer, [54, 1, ACCOUNT])

                elif msg_id == MSG_REQ_ACCT_UPDATES_MULTI:
                    self._send(writer, [74, 1, int(fields[2])])

                elif msg_id == MSG_REQ_COMPLETED_ORDERS:
                    self._send(writer, [102])

                elif msg_id == MSG_REQ_CURRENT_TIME:
                    self._send(writer, [49, 1, int(time.time())])

                # anything else: ignore, like a tolerant gateway
        except (asyncio.IncompleteReadError, ConnectionError, OSError):
            pass
        finally:
            self._writers.pop(writer, None)
            writer.close()

    # ---------------------------------------------------------------- helpers

    def _send(self, writer: asyncio.StreamWriter, fields: list) -> None:
        writer.write(encode_frame(fields))

    def _register_order(self, fields: list[str], client_id: int) -> BrokerOrder:
        order_id = int(fields[1])
        order = BrokerOrder(
            order_id=order_id,
            client_id=client_id,
            perm_id=next(self._perm_ids),
            contract=fields[2:14],
            action=fields[16],
            quantity=float(fields[17]),
            order_type=fields[18],
            lmt_price=fields[19],
            aux_price=fields[20],
            tif=fields[21],
            account=fields[23] or ACCOUNT,
            order_ref=fields[26],
        )
        # placeOrder with a known id is a MODIFY, not a new order —
        # the exact semantics that make blind retries dangerous.
        is_new = order_id not in self.orders
        self.orders[order_id] = order
        if is_new:
            self._next_valid_id = max(self._next_valid_id, order_id + 1)
        ref = order.order_ref or f"orderId:{order_id}"
        self.place_order_count[ref] = self.place_order_count.get(ref, 0) + 1
        return order

    def _apply_fill_to_position(self, order: BrokerOrder, shares: float, price: float):
        pos = self._position_for(order.contract11)
        signed = shares if order.action == "BUY" else -shares
        pos.position += signed
        pos.avg_cost = price

    def _position_for(self, contract11: list[str]) -> BrokerPosition:
        con_id = contract11[0]
        if con_id not in self.positions:
            self.positions[con_id] = BrokerPosition(contract11=list(contract11))
        return self.positions[con_id]

    # ------------------------------------------------------- frame builders
    # Field orders mirror ib_async.decoder.Decoder for server version 178.

    def _order_status_fields(self, o: BrokerOrder) -> list:
        last_exec = self._last_execution(o)
        avg_price = last_exec.avg_price if last_exec else 0
        last_price = last_exec.price if last_exec else 0
        return [
            3, o.order_id, o.status, o.filled, o.remaining, avg_price,
            o.perm_id, 0, last_price, o.client_id, "", 0,
        ]

    def _last_execution(self, order: BrokerOrder) -> BrokerExecution | None:
        for execution in reversed(self.executions):
            if execution.order is order:
                return execution
        return None

    def _exec_details_fields(self, req_id: int, e: BrokerExecution) -> list:
        o = e.order
        side = "BOT" if o.action == "BUY" else "SLD"
        return [
            11, req_id, o.order_id, *o.contract11,
            e.exec_id, e.time_str, o.account, o.contract[7] or "SMART", side,
            e.shares, e.price, o.perm_id, o.client_id, 0,
            e.cum_qty, e.avg_price, o.order_ref, "", "", "", 1,
            0,  # pendingPriceRevision (server version >= 178)
        ]

    def _open_order_fields(self, o: BrokerOrder) -> list:
        return [
            5, o.order_id, *o.contract11,
            o.action, o.quantity, o.order_type, o.lmt_price, o.aux_price, o.tif,
            "",           # ocaGroup
            o.account,
            "",           # openClose
            0,            # origin
            o.order_ref,
            o.client_id, o.perm_id,
            0, 0,         # outsideRth, hidden
            0,            # discretionaryAmt
            "",           # goodAfterTime
            "",           # skipped field
            "", "", "",   # faGroup, faMethod, faPercentage
            "",           # modelCode
            "",           # goodTillDate
            "",           # rule80A
            "",           # percentOffset
            "",           # settlingFirm
            0,            # shortSaleSlot
            "",           # designatedLocation
            -1,           # exemptCode
            0,            # auctionStrategy
            "", "", "", "", "",  # startingPrice..stockRangeUpper
            0,            # displaySize
            0, 0, 0,      # blockOrder, sweepToFill, allOrNone
            "",           # minQty
            0,            # ocaType
            0, 0, "",     # eTradeOnly, firmQuoteOnly, nbboPriceCap
            0,            # parentId
            0,            # triggerMethod
            "", "",       # volatility, volatilityType
            "",           # deltaNeutralOrderType (empty -> no dn block)
            "",           # deltaNeutralAuxPrice
            0,            # continuousUpdate
            "",           # referencePriceType
            "", "",       # trailStopPrice, trailingPercent
            "", "",       # basisPoints, basisPointsType
            "",           # comboLegsDescrip
            0,            # comboLegs count
            0,            # orderComboLegs count
            0,            # smartComboRoutingParams count
            "", "", "",   # scaleInitLevelSize, scaleSubsLevelSize, increment
            "",           # hedgeType (empty -> no hedgeParam)
            0,            # optOutSmartRouting
            "", "",       # clearingAccount, clearingIntent
            0,            # notHeld
            0,            # deltaNeutralContract present
            "",           # algoStrategy (empty -> no algo block)
            0, 0,         # solicited, whatIf
            o.status,
            "", "", "", "", "", "", "", "", "",  # margin before/change/after
            "", "", "", "",  # commission, min, max, currency
            "",           # warningText
            0, 0,         # randomizeSize, randomizePrice
            0,            # conditions count
            "",           # adjustedOrderType
            "", "", "", "", "", "",  # triggerPrice..adjustedTrailingAmount
            0,            # adjustableTrailingUnit
            "", "", "",   # softDollarTier name, val, displayName
            "",           # cashQty
            0, 0, 0,      # dontUseAutoPriceForHedge, isOmsContainer, discretionaryUpToLimitPrice
            "",           # usePriceMgmtAlgo
            "",           # duration            (server version >= 159)
            "",           # postToAts           (server version >= 160)
            0,            # autoCancelParent    (server version >= 162)
            "", "", "", "", "",  # minTradeQty..midOffsetAtHalf (>= 170)
        ]

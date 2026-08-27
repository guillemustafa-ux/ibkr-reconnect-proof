"""Position reconciliation: does the system's view match the broker's?

The ledger implies a net position per symbol (signed sum of recorded fills).
The broker states its own (reqPositions). This module diffs the two and
reports every divergence with its exact delta.

Design note: reconciliation runs on demand against a fresh position snapshot,
not on every transient event. ``reqPositions`` lags fills by design, so a
reconciler that alarms on every in-flight mismatch trains its operator to
ignore it. Run it at reconnect, after recovery, and on a slow clock.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from ib_async import IB

from .ledger import OrderLedger


@dataclass
class Divergence:
    symbol: str
    expected: float  # ledger view
    actual: float    # broker view

    @property
    def delta(self) -> float:
        return self.actual - self.expected


@dataclass
class ReconcileReport:
    matched: dict[str, float] = field(default_factory=dict)
    divergences: list[Divergence] = field(default_factory=list)

    @property
    def clean(self) -> bool:
        return not self.divergences


async def reconcile(ib: IB, ledger: OrderLedger) -> ReconcileReport:
    expected = ledger.expected_positions()
    broker_positions = await ib.reqPositionsAsync()
    actual: dict[str, float] = {}
    for pos in broker_positions:
        if pos.position:
            actual[pos.contract.symbol] = actual.get(pos.contract.symbol, 0) \
                + pos.position

    report = ReconcileReport()
    for symbol in sorted(set(expected) | set(actual)):
        want = expected.get(symbol, 0.0)
        have = actual.get(symbol, 0.0)
        if want == have:
            report.matched[symbol] = want
        else:
            report.divergences.append(Divergence(symbol, want, have))
    return report

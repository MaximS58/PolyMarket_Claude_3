#!/usr/bin/env python3
"""
Execution-cost regression tests
===============================

Synthetic order books only -- no network, so this runs anywhere and always
gives the same answer. Verifies that a paper fill actually pays the spread,
the depth it eats and the market fee, and that none of that leaks into the
resolution path (settlement is not a trade).

Usage:
    python test_execution.py
"""

from __future__ import annotations

import sys

import paper_trade as pt
from execution import OrderBook, OrderBookLevel, calculate_fee
from execution.orderbook import simulate_buy_fill, simulate_sell_fill

FAILURES: list[str] = []


def check(label: str, got, want, tol: float = 1e-6) -> None:
    ok = abs(got - want) <= tol if isinstance(want, (int, float)) else got == want
    print(f"  {'PASS' if ok else 'FAIL'}  {label}: got {got!r}, want {want!r}")
    if not ok:
        FAILURES.append(label)


def book(bids: list[tuple[float, float]], asks: list[tuple[float, float]]) -> OrderBook:
    return OrderBook(
        bids=[OrderBookLevel(p, s) for p, s in bids],
        asks=[OrderBookLevel(p, s) for p, s in asks],
    )


# A book with a 2c spread around a 0.50 mid, thinning out as you walk it.
TYPICAL = book(
    bids=[(0.49, 200), (0.48, 400), (0.46, 1000)],
    asks=[(0.51, 200), (0.52, 400), (0.54, 1000)],
)


def test_fee_formula() -> None:
    print("\nfee formula (bps/10000 * min(p, 1-p) * shares)")
    # 100 bps on 1000 shares at 0.50 -> 0.01 * 0.50 * 1000
    check("fee at mid", calculate_fee(100, 0.50, 1000), 5.0)
    # Cheap tails are cheaper to trade: min(0.05, 0.95) = 0.05
    check("fee in the tail", calculate_fee(100, 0.05, 1000), 0.5)
    check("zero rate is free", calculate_fee(0, 0.50, 1000), 0.0)


def test_buy_and_sell_fees_agree() -> None:
    print("\nbuy and sell fees agree on the same trade (the upstream fix)")
    b = book(bids=[(0.30, 10_000)], asks=[(0.30, 10_000)])
    buy = simulate_buy_fill(b, 300.0, 100, order_type="fak")
    sell = simulate_sell_fill(b, buy.total_shares, 100, order_type="fak")
    # Same shares, same price, same rate -> identical fee. Upstream charged the
    # buy on USD notional, making it avg_price times too small.
    check("same shares", round(buy.total_shares, 6), round(sell.total_shares, 6))
    check("buy fee == sell fee", round(buy.fee, 8), round(sell.fee, 8))
    check("fee is per-share", round(buy.fee, 8), round(calculate_fee(100, 0.30, 1000), 8))


def test_buy_pays_the_spread() -> None:
    print("\nbuying pays the ask, not the midpoint")
    r = simulate_buy_fill(TYPICAL, 51.0, 0, order_type="fak")
    check("fills at best ask", r.avg_price, 0.51)
    check("shares bought", round(r.total_shares, 4), 100.0)
    check("one level touched", r.levels_filled, 1)
    # 0.51 vs a 0.50 mid = 200 bps of immediate cost
    check("slippage bps", round(r.slippage_bps, 1), 200.0)


def test_big_order_walks_the_book() -> None:
    print("\na large order eats depth and pays a worse average")
    # 200 @ 0.51 = 102, then the rest out of 0.52
    r = simulate_buy_fill(TYPICAL, 302.0, 0, order_type="fak")
    check("two levels touched", r.levels_filled, 2)
    check("average is worse than best ask", r.avg_price > 0.51, True)
    check("average is inside level 2", r.avg_price < 0.52, True)
    check("slippage worse than a small order", r.slippage_bps > 200.0, True)


def test_round_trip_costs_money() -> None:
    print("\na round trip at an unchanged midpoint still loses money")
    buy = simulate_buy_fill(TYPICAL, 51.0, 0, order_type="fak")
    sell = simulate_sell_fill(TYPICAL, buy.total_shares, 0, order_type="fak")
    out, back = buy.total_cost + buy.fee, sell.total_cost - sell.fee
    print(f"        out ${out:.2f} -> back ${back:.2f}  (loss ${out - back:.2f})")
    check("round trip is a loss", back < out, True)
    # Crossing a 2c spread on a 0.50 mid costs about 4% of stake.
    check("loss is the spread", round((out - back) / out * 100, 1), 3.9)


def test_thin_book_partial_fill() -> None:
    print("\nfak fills what it can; fok refuses")
    thin = book(bids=[(0.49, 10)], asks=[(0.51, 10)])
    fak = simulate_buy_fill(thin, 100.0, 0, order_type="fak")
    check("fak filled something", fak.total_shares > 0, True)
    check("fak flagged partial", fak.is_partial, True)
    check("fak capped at book depth", round(fak.total_shares, 4), 10.0)
    fok = simulate_buy_fill(thin, 100.0, 0, order_type="fok")
    check("fok refused outright", fok.total_shares, 0.0)


# --------------------------------------------------------------------------
# End-to-end through paper_trade, with the network stubbed out
# --------------------------------------------------------------------------

def _stub_book(b: OrderBook | None, fee_bps: int = 0) -> None:
    pt.books.fetch_order_book = lambda token_id, getter: b
    pt.books.fetch_fee_rate_bps = lambda token_id, getter, default=0: fee_bps


CANDIDATE = {
    "condition_id": "0xtest",
    "token_id": "tok-1",
    "market_title": "Test market",
    "slug": "test-market",
    "outcome_to_buy": "Yes",
    "conviction_score": 90,
    "tier": "High",
    "cohort": [],
}


def test_open_position_pays_up() -> None:
    print("\nopen_position books the real fill, not the quote")
    _stub_book(TYPICAL)
    p = pt.Portfolio(starting_capital=1000.0, cash=1000.0)
    pt.open_position(p, CANDIDATE, price=0.50, size_usd=51.0)

    pos = p.open_positions["0xtest"]
    check("entry is the ask", pos.entry_price, 0.51)
    check("midpoint recorded", pos.mid_at_entry, 0.50)
    check("shares reflect the ask", round(pos.shares, 4), 100.0)
    check("cash left", round(p.cash, 2), 949.0)
    # Marked at the midpoint it will never get, so it opens underwater.
    check("opens at a loss", round(pos.unrealized_pnl, 2), -1.0)
    check("slippage attributed", round(p.total_slippage_cost, 2), 1.0)


def test_resolution_skips_the_book() -> None:
    print("\nresolution settles at face value, paying nothing")
    _stub_book(TYPICAL, fee_bps=500)
    p = pt.Portfolio(starting_capital=1000.0, cash=1000.0)
    pt.open_position(p, CANDIDATE, price=0.50, size_usd=51.0)
    fees_after_entry = p.total_fees
    pos = p.open_positions["0xtest"]

    pt.close_position(p, pos, 1.0, "resolved", "market resolved - Yes")
    closed = p.closed_positions[-1]
    check("redeemed at face value", closed.exit_price, 1.0)
    check("no exit fee on settlement", closed.exit_fee, 0.0)
    check("proceeds are shares x 1.0", round(closed.proceeds, 2), round(pos.shares, 2))
    check("settlement added no cost", round(p.total_fees, 6), round(fees_after_entry, 6))
    check("position closed out", "0xtest" in p.open_positions, False)


def test_partial_exit_keeps_the_remainder() -> None:
    print("\na thin bid side sells part and holds the rest")
    _stub_book(TYPICAL)
    p = pt.Portfolio(starting_capital=1000.0, cash=1000.0)
    pt.open_position(p, CANDIDATE, price=0.50, size_usd=51.0)
    pos = p.open_positions["0xtest"]
    opened_shares, opened_basis = pos.shares, pos.cost_basis

    # Only 40 shares of bid left to hit.
    _stub_book(book(bids=[(0.49, 40)], asks=[(0.51, 200)]))
    pt.close_position(p, pos, 0.50, "trader_majority_exit", "cohort left")

    check("still open", "0xtest" in p.open_positions, True)
    check("remainder held", round(p.open_positions["0xtest"].shares, 4),
          round(opened_shares - 40, 4))
    closed = p.closed_positions[-1]
    check("sold what the book took", round(closed.shares, 4), 40.0)
    # Cost basis has to split with the shares or P&L is nonsense.
    check("basis split proportionally",
          round(closed.cost_basis + p.open_positions["0xtest"].cost_basis, 4),
          round(opened_basis, 4))


def test_no_bid_means_no_exit() -> None:
    print("\nno bid at all: hold, do not invent a price")
    _stub_book(TYPICAL)
    p = pt.Portfolio(starting_capital=1000.0, cash=1000.0)
    pt.open_position(p, CANDIDATE, price=0.50, size_usd=51.0)
    pos = p.open_positions["0xtest"]

    _stub_book(book(bids=[], asks=[(0.51, 200)]))
    pt.close_position(p, pos, 0.50, "trader_majority_exit", "cohort left")
    check("position untouched", "0xtest" in p.open_positions, True)
    check("nothing was booked", len(p.closed_positions), 0)


def test_old_portfolio_still_loads() -> None:
    print("\na portfolio written before this change still loads")
    legacy = {
        "condition_id": "0xold", "token_id": "tok-0", "market_title": "Old",
        "slug": "old", "outcome": "Yes", "shares": 100.0, "entry_price": 0.5,
        "cost_basis": 50.0, "opened_at": "2026-01-01T00:00:00Z",
        "conviction_score": 80, "tier": "High", "cohort": [],
        "last_price": 0.5, "last_price_at": "2026-01-01T00:00:00Z",
    }
    pos = pt.PaperPosition.from_dict(legacy)
    check("loads without cost fields", pos.shares, 100.0)
    check("fee defaults to zero", pos.entry_fee, 0.0)
    check("slippage defaults to zero", pos.entry_slippage_bps, 0.0)


def main() -> int:
    print("=" * 72)
    print("  EXECUTION COST TESTS  (synthetic books, no network)")
    print("=" * 72)

    for fn in (test_fee_formula, test_buy_and_sell_fees_agree,
               test_buy_pays_the_spread, test_big_order_walks_the_book,
               test_round_trip_costs_money, test_thin_book_partial_fill,
               test_open_position_pays_up, test_resolution_skips_the_book,
               test_partial_exit_keeps_the_remainder, test_no_bid_means_no_exit,
               test_old_portfolio_still_loads):
        fn()

    print("\n" + "=" * 72)
    if FAILURES:
        print(f"  {len(FAILURES)} FAILED: " + ", ".join(FAILURES))
        return 1
    print("  ALL PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())

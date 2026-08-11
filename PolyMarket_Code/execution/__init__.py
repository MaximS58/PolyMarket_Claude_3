"""Realistic execution modelling for the paper trader.

Fills walk a live Polymarket order book level by level, so a simulated trade
pays the spread, the depth it consumes, and any market fee -- none of which
the raw midpoint price shows.

The fill simulator is vendored from polymarket-paper-trader (MIT); see NOTICE.
"""

from .books import (
    fetch_fee_rate_bps,
    fetch_order_book,
    midpoint,
    parse_order_book,
    spread_bps,
)
from .models import Fill, FillResult, OrderBook, OrderBookLevel
from .orderbook import calculate_fee, simulate_buy_fill, simulate_sell_fill

__all__ = [
    "Fill",
    "FillResult",
    "OrderBook",
    "OrderBookLevel",
    "calculate_fee",
    "simulate_buy_fill",
    "simulate_sell_fill",
    "fetch_order_book",
    "fetch_fee_rate_bps",
    "parse_order_book",
    "midpoint",
    "spread_bps",
]

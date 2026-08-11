"""Order-book and fill dataclasses.

Vendored from polymarket-paper-trader (MIT, Copyright (c) 2025 agent-next).
Only the four types the fill simulator needs were taken; see NOTICE.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class OrderBookLevel:
    """A single price level in the order book."""

    price: float
    size: float


@dataclass
class OrderBook:
    """Full order book for a token (one side of a market)."""

    bids: list[OrderBookLevel] = field(default_factory=list)
    asks: list[OrderBookLevel] = field(default_factory=list)


@dataclass
class Fill:
    """A per-level fill from walking the order book."""

    price: float
    shares: float
    cost: float
    level: int


@dataclass
class FillResult:
    """Result of walking the order book to fill an order."""

    filled: bool
    avg_price: float
    total_cost: float
    total_shares: float
    fee: float
    slippage_bps: float
    levels_filled: int
    is_partial: bool
    fills: list[Fill] = field(default_factory=list)

"""CLOB order-book and fee-rate fetching.

Written for this project (not vendored). The HTTP call itself is injected as
a `getter` callable so this module reuses paper_trade.py rate limiting,
retry/back-off and session headers rather than opening a second, untuned
HTTP path to the same API.

    from execution import books
    book = books.fetch_order_book(token_id, api_get)
"""

from __future__ import annotations

import logging

from typing import Any, Callable

from .models import OrderBook, OrderBookLevel

CLOB_API = "https://clob.polymarket.com"
BOOK_URL = f"{CLOB_API}/book"
FEE_RATE_URL = f"{CLOB_API}/fee-rate"

# A getter takes (url, params, label) and returns parsed JSON -- the shape of
# paper_trade.api_get.
Getter = Callable[..., Any]

# Fee rates change rarely; the book never gets cached.
_fee_cache: dict[str, int] = {}


def parse_order_book(data: Any) -> OrderBook:
    """Parse a CLOB /book response into an OrderBook."""
    if not isinstance(data, dict):
        return OrderBook()

    def levels(key: str) -> list[OrderBookLevel]:
        out: list[OrderBookLevel] = []
        for entry in data.get(key) or []:
            if not isinstance(entry, dict):
                continue
            try:
                price = float(entry.get("price", 0))
                size = float(entry.get("size", 0))
            except (TypeError, ValueError):
                continue
            if price > 0 and size > 0:
                out.append(OrderBookLevel(price=price, size=size))
        return out

    return OrderBook(bids=levels("bids"), asks=levels("asks"))


def fetch_order_book(token_id: str, getter: Getter) -> OrderBook | None:
    """Live order book for a token, or None if it cannot be fetched.

    None means the book could not be fetched. An empty-but-real book comes
    back as an OrderBook with no levels -- the caller has to tell those apart,
    because one is an API blip and the other is a market with no liquidity.

    Never cached -- a stale book would defeat the point of costing the fill.
    """
    if not token_id:
        return None
    try:
        data = getter(BOOK_URL, {"token_id": token_id},
                     label=f"book {token_id[:12]}")
    except Exception:
        return None

    if not isinstance(data, dict):
        return None
    return parse_order_book(data)


def fetch_fee_rate_bps(token_id: str, getter: Getter, default: int = 0) -> int:
    """Fee rate in basis points for a token, cached for the process lifetime.

    `default` is only used when the call fails. Most liquid markets currently
    report base_fee 1000; longshot markets report 0. See the note on
    FEE_BPS_OVERRIDE in paper_trade.py about that value being an unverified
    unit -- this function returns whatever the API says, unconverted.
    """
    if not token_id:
        return default
    if token_id in _fee_cache:
        return _fee_cache[token_id]

    try:
        data = getter(FEE_RATE_URL, {"token_id": token_id},
                      label=f"fee-rate {token_id[:12]}")
        bps = _read_fee_bps(data, default)
    except Exception:
        bps = default

    _fee_cache[token_id] = bps
    return bps


# The live endpoint answers {"base_fee": 0}. Upstream read "fee_rate_bps",
# which is not a key it returns, so every lookup silently fell through to the
# default -- right answer today only because Polymarket runs at zero fees
# (taker_base_fee 0, feesEnabled false). Read the key it actually sends, keep
# the others as alternates, and say something if none of them appear.
_FEE_KEYS = ("base_fee", "taker_base_fee", "fee_rate_bps")
_warned_unknown_schema = [False]


def _read_fee_bps(data: Any, default: int) -> int:
    """Pull the fee rate in bps out of a /fee-rate response."""
    if not isinstance(data, dict):
        return default
    for key in _FEE_KEYS:
        if key in data:
            try:
                return int(data[key])
            except (TypeError, ValueError):
                return default
    if not _warned_unknown_schema[0]:
        _warned_unknown_schema[0] = True
        logging.getLogger("paper_trade").warning(
            "fee-rate response has none of %s (got %s) - assuming %d bps. "
            "The endpoint schema may have changed.",
            ", ".join(_FEE_KEYS), sorted(data)[:6], default)
    return default


def best_bid(book: OrderBook) -> float | None:
    return max((lvl.price for lvl in book.bids), default=None)


def best_ask(book: OrderBook) -> float | None:
    return min((lvl.price for lvl in book.asks), default=None)


def midpoint(book: OrderBook) -> float | None:
    """Mid of best bid and best ask, or None when either side is empty."""
    bid, ask = best_bid(book), best_ask(book)
    if bid is None or ask is None:
        return None
    return (bid + ask) / 2.0


def spread_bps(book: OrderBook) -> float | None:
    """Quoted spread in basis points of the midpoint."""
    bid, ask = best_bid(book), best_ask(book)
    mid = midpoint(book)
    if bid is None or ask is None or not mid:
        return None
    return (ask - bid) / mid * 10_000


def depth_usd_within(book: OrderBook, bps: float, side: str = "ask") -> float:
    """
    Dollars resting within `bps` of the midpoint on one side of the book.

    This is the honest measure of whether a market can be traded: not how much
    the copied cohort staked, and not the headline volume, but how much stock
    is actually sitting there to hit before the price runs away. Buying more
    than this figure means walking past the band and paying for it.

    Returns 0.0 when the book has no midpoint (one side empty).
    """
    mid = midpoint(book)
    if not mid or mid <= 0:
        return 0.0

    limit = mid * (1.0 + bps / 10_000.0) if side == "ask" else mid * (1.0 - bps / 10_000.0)
    levels = book.asks if side == "ask" else book.bids

    total = 0.0
    for level in levels:
        within = level.price <= limit if side == "ask" else level.price >= limit
        if within:
            total += level.price * level.size
    return total


def tradeable_depth(book: OrderBook, bps: float) -> float:
    """
    The smaller of the two sides' depth within `bps`.

    A position has to be got out of as well as into, so a market with a deep
    ask and no bid is not tradeable at all -- taking the minimum stops a
    one-sided book from reading as liquid.
    """
    return min(depth_usd_within(book, bps, "ask"),
               depth_usd_within(book, bps, "bid"))


def clear_fee_cache() -> None:
    _fee_cache.clear()

#!/usr/bin/env python3
"""
Polymarket Paper-Trading Engine
================================

Takes the **High**-tier recommendations produced by trade_decision.py, opens a
simulated position in each, and marks the whole book to market against live
Polymarket prices every 30 seconds.

Position lifecycle:
  • Entry  — live CLOB price at the moment the position is first opened
  • Hold   — marked to market every REFRESH_SECONDS
  • Exit   — when a majority of the copied trader cohort has left the position
             (signalled by trader_exit_monitor.py), or when the market resolves

Sizing:
  All High trades are scaled proportionally so the book fills
  (1 - CASH_RESERVE_PCT) of the portfolio, leaving cash free to fund trades
  that appear in later pipeline runs.

Usage:
    python paper_trade.py              # live prices, loops until Ctrl+C
    python paper_trade.py --dry-run    # price from recorded current_price, one cycle
    python paper_trade.py --once       # live prices, single cycle then exit
    python paper_trade.py --reset      # discard any existing portfolio and start fresh

Author : Antigravity AI
Version: 1.0.0
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import os
import shutil
import signal
import sys
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from execution import books
from execution.orderbook import simulate_buy_fill, simulate_sell_fill

# ═══════════════════════════════════ CONFIG ═══════════════════════════════════

CLOB_API = "https://clob.polymarket.com"
GAMMA_API = "https://gamma-api.polymarket.com"

PRICES_URL = f"{CLOB_API}/prices"
MIDPOINT_URL = f"{CLOB_API}/midpoint"
CLOB_MARKETS_URL = f"{CLOB_API}/markets"
GAMMA_MARKETS_URL = f"{GAMMA_API}/markets"

PORTFOLIO_SIZE = 10_000.0
CASH_RESERVE_PCT = 0.10      # keep 10% free to fund newly-appearing trades
REFRESH_SECONDS = 30
RESOLUTION_CHECK_CYCLES = 20  # ~10 min at a 30s refresh

# ---- Selection: one pass, at seeding ----
# Rank the overlaps by the dollars the copied traders have committed, keep
# the top TOP_BY_VALUE, then open the TRADE_SOONEST of those that resolve
# first. Nothing is opened after seeding: a position is held until the
# market resolves or the exit monitor reports the cohort has left.
TOP_BY_VALUE = 25
TRADE_SOONEST = 10
SELECTION_LABEL = "top-value/soonest"

# When the cohort is split across both sides of a market, one side has to be
# ahead by more than this in committed dollars for the split to count as a
# signal. Anything closer is treated as no view at all and the market is
# dropped. Note this is an absolute figure, so a contested market whose whole
# cohort is smaller than this can never clear it, however lopsided the split.
SPLIT_DECISIVE_MARGIN_USD = 20_000.0

# Live dashboard
RECENT_ACTIVITY_LINES = 8   # log lines shown at the bottom of the frame
DASHBOARD_MIN_WIDTH = 72    # below this the table is unreadable anyway
DASHBOARD_MAX_WIDTH = 118   # full-width layout, as before

# Ignore exit signals older than this — a dead monitor must not drive decisions
MAX_SIGNAL_AGE_SECONDS = 1800

# Refuse to open a position outside this band (effectively resolved / illiquid)
MIN_TRADEABLE_PRICE = 0.01
MAX_TRADEABLE_PRICE = 0.99

# ---- Execution costs ----
# Fills walk the live order book instead of transacting at the midpoint, so a
# trade pays the spread, the depth it eats and any market fee. None of that
# shows up in the quoted price. Set False for frictionless midpoint fills.
EXECUTION_COSTS = True

# "fak" lets a thin book fill part of an order rather than rejecting it, which
# is what this strategy would really do -- size down, not walk away.
ORDER_TYPE = "fak"

# Fee rate is read per token from the CLOB /fee-rate endpoint; this is only
# the fallback when that call fails. Fees are NOT hypothetical: sampling the
# top-volume active markets, 9 of 12 report base_fee 1000 with feesEnabled
# true (feeType economics_fees), and only longshots came back 0.
DEFAULT_FEE_BPS = 0

# UNVERIFIED UNIT. The endpoint reports base_fee as a bare integer (1000) and
# upstream treats that field as basis points, which makes the charge
# 0.10 * min(price, 1-price) * shares -- about 10% of notional at a 0.38
# price. That is steep enough to be worth confirming against a real filled
# order before trusting the P&L. Set this to a number to pin every market to
# that rate instead (0 disables fees entirely); leave None to use the API.
FEE_BPS_OVERRIDE: int | None = None

# A flat percentage of the trade value, charged on the way in and again on
# the way out. Simpler and far easier to reason about than the per-market
# rate above, which varies with how close the price sits to 50c.
#
# Resolution is settlement, not a trade, so a position held to the end pays
# once. Selling early is what costs you twice.
#
# Set to None to fall back to the Polymarket rate (FEE_BPS_OVERRIDE, then the
# live /fee-rate endpoint). When this is set it wins over both.
FLAT_FEE_PCT: float | None = 5.0

# Walk away from an entry whose average fill is this far through the midpoint.
# At that point the book is too thin to trust at this size.
MAX_ENTRY_SLIPPAGE_BPS = 500.0

# Rate-limiting
RATE_RPS = 8
BACKOFF_BASE = 2.0
BACKOFF_MAX = 60.0
MAX_RETRIES = 5

OUTPUT_DIR = Path(__file__).resolve().parent / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

RECOMMENDED_TRADES_FILE = OUTPUT_DIR / "recommended_trades.json"
TOP_TRADERS_FILE = OUTPUT_DIR / "latest_top_traders.json"
OVERLAPS_FILE = OUTPUT_DIR / "latest_overlaps.json"
PORTFOLIO_FILE = OUTPUT_DIR / "paper_portfolio.json"
EQUITY_HISTORY_FILE = OUTPUT_DIR / "paper_equity_history.jsonl"
EXIT_SIGNALS_FILE = OUTPUT_DIR / "trader_exit_signals.json"

# ═══════════════════════════════════ LOGGING ══════════════════════════════════

# Box-drawing characters below need a UTF-8 console on Windows.
try:
    sys.stdout.reconfigure(encoding="utf-8")
except (AttributeError, OSError):
    pass

LOG_FMT = "%(asctime)s │ %(levelname)-8s │ %(message)s"
DATE_FMT = "%Y-%m-%d %H:%M:%S"

_console_handler = logging.StreamHandler(sys.stdout)
_file_handler = logging.FileHandler(OUTPUT_DIR / "paper_trade.log", encoding="utf-8")

logging.basicConfig(
    level=logging.INFO,
    format=LOG_FMT,
    datefmt=DATE_FMT,
    handlers=[_console_handler, _file_handler],
)
log = logging.getLogger("paper_trade")


class RecentActivityHandler(logging.Handler):
    """
    Keeps the last N log records so the live dashboard can show them.

    In live mode the screen is repainted every cycle, which would wipe anything
    written straight to stdout. Rather than lose the opens/closes/warnings, they
    are buffered here and drawn as part of the frame. The file handler still
    records everything, unabridged.
    """

    def __init__(self, capacity: int = RECENT_ACTIVITY_LINES):
        super().__init__()
        self.records: deque[tuple[str, str, str]] = deque(maxlen=capacity)

    def emit(self, record: logging.LogRecord) -> None:
        stamp = datetime.fromtimestamp(record.created, timezone.utc).strftime("%H:%M:%S")
        self.records.append((stamp, record.levelname, record.getMessage()))


_recent_activity = RecentActivityHandler()
_live_mode = False


def enable_live_mode() -> None:
    """Switch from scrolling log output to an in-place dashboard."""
    global _live_mode
    _live_mode = True
    _enable_ansi()
    root = logging.getLogger()
    root.removeHandler(_console_handler)  # nothing may print outside the frame
    root.addHandler(_recent_activity)


def disable_live_mode() -> None:
    """Hand stdout back to the logger so the shutdown summary is visible."""
    global _live_mode
    if not _live_mode:
        return
    _live_mode = False
    root = logging.getLogger()
    root.removeHandler(_recent_activity)
    root.addHandler(_console_handler)


def _enable_ansi() -> None:
    """
    Turn on ANSI escape handling for the console.

    Windows Terminal supports ANSI natively, but plain conhost needs
    ENABLE_VIRTUAL_TERMINAL_PROCESSING set explicitly or the escape codes print
    as literal garbage.
    """
    if os.name != "nt":
        return
    try:
        import ctypes

        kernel32 = ctypes.windll.kernel32
        handle = kernel32.GetStdHandle(-11)  # STD_OUTPUT_HANDLE
        mode = ctypes.c_ulong()
        if kernel32.GetConsoleMode(handle, ctypes.byref(mode)):
            kernel32.SetConsoleMode(handle, mode.value | 0x0004)
    except Exception:
        pass  # worst case the dashboard scrolls instead of repainting

# ════════════════════════════════ HTTP HELPERS ════════════════════════════════


def _build_session() -> requests.Session:
    s = requests.Session()
    s.verify = False
    retry = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update({
        "Accept": "application/json",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
        ),
    })
    return s


SESSION = _build_session()


class RateLimiter:
    """Simple token-bucket rate limiter."""

    def __init__(self, rps: float):
        self._interval = 1.0 / rps
        self._last = 0.0

    def wait(self) -> None:
        now = time.monotonic()
        gap = now - self._last
        if gap < self._interval:
            time.sleep(self._interval - gap)
        self._last = time.monotonic()


_limiter = RateLimiter(RATE_RPS)


def _request(method: str, url: str, label: str, **kwargs) -> Any:
    """Rate-limited request with exponential back-off. Returns parsed JSON."""
    _limiter.wait()
    backoff = BACKOFF_BASE
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = SESSION.request(method, url, timeout=30, **kwargs)
            if r.status_code == 429:
                wait = float(r.headers.get("Retry-After", backoff))
                log.warning("Rate-limited on %s (att %d/%d). Waiting %.1fs…",
                            label, attempt, MAX_RETRIES, wait)
                time.sleep(wait)
                backoff = min(backoff * 2, BACKOFF_MAX)
                continue
            if r.status_code >= 500:
                log.warning("Server %d on %s (att %d/%d). Waiting %.1fs…",
                            r.status_code, label, attempt, MAX_RETRIES, backoff)
                time.sleep(backoff)
                backoff = min(backoff * 2, BACKOFF_MAX)
                continue
            r.raise_for_status()
            return r.json()
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as exc:
            log.warning("Network error on %s (att %d/%d): %s", label, attempt, MAX_RETRIES, exc)
            time.sleep(backoff)
            backoff = min(backoff * 2, BACKOFF_MAX)
    raise RuntimeError(f"Failed {label} after {MAX_RETRIES} attempts")


def api_get(url: str, params: dict | None = None, label: str = "req") -> Any:
    return _request("GET", url, label, params=params)


def api_post(url: str, payload: Any, label: str = "req") -> Any:
    return _request("POST", url, label, json=payload)


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _as_float(value: Any) -> float | None:
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    return f if math.isfinite(f) else None


# ═══════════════════════════════ DATA MODELS ══════════════════════════════════


@dataclass
class CohortMember:
    """A top trader we are copying on a given market."""
    proxy_wallet: str
    username: str
    rank: int
    baseline_size: float

    def to_dict(self) -> dict:
        return {
            "proxy_wallet": self.proxy_wallet,
            "username": self.username,
            "rank": self.rank,
            "baseline_size": round(self.baseline_size, 4),
        }

    @staticmethod
    def from_dict(d: dict) -> CohortMember:
        return CohortMember(
            proxy_wallet=d.get("proxy_wallet", ""),
            username=d.get("username", ""),
            rank=int(d.get("rank", 999)),
            baseline_size=float(d.get("baseline_size", 0.0)),
        )


@dataclass
class PaperPosition:
    """An open simulated position."""
    condition_id: str
    token_id: str
    market_title: str
    slug: str
    outcome: str
    shares: float
    entry_price: float
    cost_basis: float
    opened_at: str
    conviction_score: int
    tier: str
    cohort: list[CohortMember] = field(default_factory=list)
    last_price: float = 0.0
    last_price_at: str = ""
    # What the fill actually cost, over and above the quoted midpoint.
    mid_at_entry: float = 0.0
    entry_fee: float = 0.0
    entry_slippage_bps: float = 0.0
    entry_levels: int = 0

    @property
    def market_value(self) -> float:
        return self.shares * self.last_price

    @property
    def unrealized_pnl(self) -> float:
        return self.market_value - self.cost_basis

    @property
    def return_pct(self) -> float:
        if self.cost_basis <= 0:
            return 0.0
        return self.unrealized_pnl / self.cost_basis * 100.0

    def to_dict(self) -> dict:
        return {
            "condition_id": self.condition_id,
            "token_id": self.token_id,
            "market_title": self.market_title,
            "slug": self.slug,
            "outcome": self.outcome,
            "shares": round(self.shares, 4),
            "entry_price": round(self.entry_price, 4),
            "cost_basis": round(self.cost_basis, 2),
            "opened_at": self.opened_at,
            "conviction_score": self.conviction_score,
            "tier": self.tier,
            "cohort": [c.to_dict() for c in self.cohort],
            "last_price": round(self.last_price, 4),
            "last_price_at": self.last_price_at,
            "mid_at_entry": round(self.mid_at_entry, 4),
            "entry_fee": round(self.entry_fee, 4),
            "entry_slippage_bps": round(self.entry_slippage_bps, 1),
            "entry_levels": self.entry_levels,
            "market_value": round(self.market_value, 2),
            "unrealized_pnl": round(self.unrealized_pnl, 2),
            "return_pct": round(self.return_pct, 2),
        }

    @staticmethod
    def from_dict(d: dict) -> PaperPosition:
        return PaperPosition(
            condition_id=d["condition_id"],
            token_id=d.get("token_id", ""),
            market_title=d.get("market_title", ""),
            slug=d.get("slug", ""),
            outcome=d.get("outcome", ""),
            shares=float(d.get("shares", 0.0)),
            entry_price=float(d.get("entry_price", 0.0)),
            cost_basis=float(d.get("cost_basis", 0.0)),
            opened_at=d.get("opened_at", ""),
            conviction_score=int(d.get("conviction_score", 0)),
            tier=d.get("tier", ""),
            cohort=[CohortMember.from_dict(c) for c in d.get("cohort", [])],
            last_price=float(d.get("last_price", 0.0)),
            last_price_at=d.get("last_price_at", ""),
            mid_at_entry=float(d.get("mid_at_entry", 0.0)),
            entry_fee=float(d.get("entry_fee", 0.0)),
            entry_slippage_bps=float(d.get("entry_slippage_bps", 0.0)),
            entry_levels=int(d.get("entry_levels", 0)),
        )


@dataclass
class ClosedPosition:
    """A settled or exited position."""
    condition_id: str
    market_title: str
    outcome: str
    shares: float
    entry_price: float
    exit_price: float
    cost_basis: float
    proceeds: float
    realized_pnl: float
    opened_at: str
    closed_at: str
    exit_reason: str
    detail: str = ""
    entry_fee: float = 0.0
    exit_fee: float = 0.0
    exit_slippage_bps: float = 0.0

    @property
    def return_pct(self) -> float:
        if self.cost_basis <= 0:
            return 0.0
        return self.realized_pnl / self.cost_basis * 100.0

    def to_dict(self) -> dict:
        return {
            "condition_id": self.condition_id,
            "market_title": self.market_title,
            "outcome": self.outcome,
            "shares": round(self.shares, 4),
            "entry_price": round(self.entry_price, 4),
            "exit_price": round(self.exit_price, 4),
            "cost_basis": round(self.cost_basis, 2),
            "proceeds": round(self.proceeds, 2),
            "realized_pnl": round(self.realized_pnl, 2),
            "return_pct": round(self.return_pct, 2),
            "opened_at": self.opened_at,
            "closed_at": self.closed_at,
            "exit_reason": self.exit_reason,
            "detail": self.detail,
            "entry_fee": round(self.entry_fee, 4),
            "exit_fee": round(self.exit_fee, 4),
            "exit_slippage_bps": round(self.exit_slippage_bps, 1),
        }

    @staticmethod
    def from_dict(d: dict) -> ClosedPosition:
        return ClosedPosition(
            condition_id=d["condition_id"],
            market_title=d.get("market_title", ""),
            outcome=d.get("outcome", ""),
            shares=float(d.get("shares", 0.0)),
            entry_price=float(d.get("entry_price", 0.0)),
            exit_price=float(d.get("exit_price", 0.0)),
            cost_basis=float(d.get("cost_basis", 0.0)),
            proceeds=float(d.get("proceeds", 0.0)),
            realized_pnl=float(d.get("realized_pnl", 0.0)),
            opened_at=d.get("opened_at", ""),
            closed_at=d.get("closed_at", ""),
            exit_reason=d.get("exit_reason", ""),
            detail=d.get("detail", ""),
            entry_fee=float(d.get("entry_fee", 0.0)),
            exit_fee=float(d.get("exit_fee", 0.0)),
            exit_slippage_bps=float(d.get("exit_slippage_bps", 0.0)),
        )


@dataclass
class Portfolio:
    """The simulated book."""
    starting_capital: float = PORTFOLIO_SIZE
    cash: float = PORTFOLIO_SIZE
    started_at: str = field(default_factory=_now_iso)
    cycles: int = 0
    per_trade_size: float = 0.0
    open_positions: dict[str, PaperPosition] = field(default_factory=dict)
    closed_positions: list[ClosedPosition] = field(default_factory=list)
    # Markets confirmed resolved before we ever entered — never worth re-pricing
    dead_markets: set[str] = field(default_factory=set)
    # What execution has taken out of the book so far, in dollars.
    total_fees: float = 0.0
    total_slippage_cost: float = 0.0

    # ── derived ──
    @property
    def deployed(self) -> float:
        return sum(p.market_value for p in self.open_positions.values())

    @property
    def equity(self) -> float:
        return self.cash + self.deployed

    @property
    def unrealized_pnl(self) -> float:
        return sum(p.unrealized_pnl for p in self.open_positions.values())

    @property
    def realized_pnl(self) -> float:
        return sum(c.realized_pnl for c in self.closed_positions)

    @property
    def execution_cost(self) -> float:
        """Fees plus slippage -- the drag the quoted price never showed."""
        return self.total_fees + self.total_slippage_cost

    @property
    def total_return_pct(self) -> float:
        if self.starting_capital <= 0:
            return 0.0
        return (self.equity - self.starting_capital) / self.starting_capital * 100.0

    # ── persistence ──
    def save(self) -> None:
        payload = {
            "metadata": {
                "timestamp": _now_iso(),
                "started_at": self.started_at,
                "cycles": self.cycles,
                "starting_capital": self.starting_capital,
                "per_trade_size": round(self.per_trade_size, 2),
                "selection": SELECTION_LABEL,
                "top_by_value": TOP_BY_VALUE,
                "trade_soonest": TRADE_SOONEST,
            },
            "summary": {
                "equity": round(self.equity, 2),
                "cash": round(self.cash, 2),
                "deployed": round(self.deployed, 2),
                "unrealized_pnl": round(self.unrealized_pnl, 2),
                "realized_pnl": round(self.realized_pnl, 2),
                "total_return_pct": round(self.total_return_pct, 4),
                "open_count": len(self.open_positions),
                "closed_count": len(self.closed_positions),
                "total_fees": round(self.total_fees, 2),
                "total_slippage_cost": round(self.total_slippage_cost, 2),
                "execution_cost": round(self.execution_cost, 2),
            },
            "open_positions": [p.to_dict() for p in self.open_positions.values()],
            "closed_positions": [c.to_dict() for c in self.closed_positions],
            "dead_markets": sorted(self.dead_markets),
        }
        tmp = PORTFOLIO_FILE.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
        tmp.replace(PORTFOLIO_FILE)

    @staticmethod
    def load() -> Portfolio | None:
        if not PORTFOLIO_FILE.exists():
            return None
        try:
            d = json.loads(PORTFOLIO_FILE.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as exc:
            log.warning("Could not read existing portfolio (%s) — starting fresh.", exc)
            return None
        meta = d.get("metadata", {})
        summary = d.get("summary", {})
        p = Portfolio(
            starting_capital=float(meta.get("starting_capital", PORTFOLIO_SIZE)),
            cash=float(summary.get("cash", PORTFOLIO_SIZE)),
            started_at=meta.get("started_at", _now_iso()),
            cycles=int(meta.get("cycles", 0)),
            per_trade_size=float(meta.get("per_trade_size", 0.0)),
            total_fees=float(summary.get("total_fees", 0.0)),
            total_slippage_cost=float(summary.get("total_slippage_cost", 0.0)),
        )
        for od in d.get("open_positions", []):
            pos = PaperPosition.from_dict(od)
            p.open_positions[pos.condition_id] = pos
        p.closed_positions = [ClosedPosition.from_dict(c) for c in d.get("closed_positions", [])]
        p.dead_markets = set(d.get("dead_markets", []))
        return p


# ════════════════════════════ RECOMMENDATION LOADING ══════════════════════════


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def build_token_index(top_traders: dict) -> dict[tuple[str, str], str]:
    """Map (condition_id, outcome) → CLOB token id, harvested from scraped positions."""
    index: dict[tuple[str, str], str] = {}
    for trader in top_traders.get("traders", []):
        for pos in trader.get("positions", []):
            asset = pos.get("asset")
            if not asset:
                continue
            index.setdefault((pos.get("condition_id", ""), pos.get("outcome", "")), str(asset))
    return index


def build_cohort_index(overlaps: dict, top_traders: dict) -> dict[tuple[str, str], list[CohortMember]]:
    """
    Map (condition_id, outcome) → the trader cohort behind that recommendation.

    Re-applies trade_decision.py's one-hit-wonder filter (open_position_count > 1)
    so the cohort matches the `valid_top_traders_count` stored on each trade.
    """
    position_counts = {
        t.get("proxy_wallet", ""): t.get("open_position_count", 0)
        for t in top_traders.get("traders", [])
    }
    ranks = {
        t.get("proxy_wallet", ""): t.get("rank", 999)
        for t in top_traders.get("traders", [])
    }

    index: dict[tuple[str, str], list[CohortMember]] = {}
    for group in overlaps.get("overlaps", []):
        members = []
        for entry in group.get("traders", []):
            wallet = entry.get("proxy_wallet", "")
            if position_counts.get(wallet, 0) <= 1:
                continue  # one-hit wonder — excluded from the signal
            members.append(CohortMember(
                proxy_wallet=wallet,
                username=entry.get("username", ""),
                rank=ranks.get(wallet, entry.get("rank", 999)),
                baseline_size=float(entry.get("size", 0.0)),
            ))
        if members:
            index[(group.get("condition_id", ""), group.get("outcome", ""))] = members
    return index


def _parse_iso(value: Any) -> datetime | None:
    """Parse an ISO timestamp, tolerating the string "None" the scraper emits."""
    if not value or str(value) == "None":
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None


def _as_money(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _side_weight(candidate: dict) -> tuple[float, int]:
    """
    How much the cohort has behind one side: money first, head count second.

    conviction_score is the surviving cohort size, i.e. the trader count.
    """
    return (candidate["cohort_value_usd"], candidate["conviction_score"])


def _pick_side(group: list[dict]) -> dict | None:
    """
    Choose which side of a market to trade, or None to trade none of it.

    The overlaps file lists each outcome separately, so a market the cohort is
    split on appears more than once. Buying both Over at 0.62 and Under at 0.38
    pays $1.00 for $1.00 back, minus two spreads and two fees -- and since
    open_positions is keyed by condition_id the second fill would overwrite the
    first, leaving cash gone twice and one position tracked.

    A side is only taken when it is ahead by more than
    SPLIT_DECISIVE_MARGIN_USD in committed dollars. A narrower gap is not a
    view on direction, it is noise, so None is returned and the market is
    skipped rather than guessed at.
    """
    if not group:
        return None
    if len(group) == 1:
        return group[0]

    ranked = sorted(group, key=_side_weight, reverse=True)
    top, runner_up = ranked[0], ranked[1]

    margin = top["cohort_value_usd"] - runner_up["cohort_value_usd"]
    if margin > SPLIT_DECISIVE_MARGIN_USD:
        return top
    return None


def load_candidates() -> list[dict]:
    """
    Choose the book in one pass: rank the overlaps by the dollars the copied
    traders have committed, keep the top TOP_BY_VALUE, then open the
    TRADE_SOONEST of those that resolve first.

    Ranking on total_value rather than share count is deliberate. A sub-cent
    market routinely shows half a million shares for a couple of hundred
    dollars, and ranking by size would let those crowd out every position with
    real money behind it.
    """
    for path in (TOP_TRADERS_FILE, OVERLAPS_FILE):
        if not path.exists():
            log.error("Required file not found: %s", path)
            log.error("Run the pipeline first:  python run_all.py")
            sys.exit(1)

    top_traders = _load_json(TOP_TRADERS_FILE)
    overlaps = _load_json(OVERLAPS_FILE)

    token_index = build_token_index(top_traders)
    cohort_index = build_cohort_index(overlaps, top_traders)

    now = datetime.now(timezone.utc)
    pool: list[dict] = []
    dropped = {"under_way": 0, "no_end_date": 0, "no_token": 0, "no_cohort": 0}

    for group in overlaps.get("overlaps", []):
        # Never enter a market already under way or settled.
        state = group.get("market_state")
        if state is not None and state != "upcoming":
            dropped["under_way"] += 1
            continue

        end_date = _parse_iso(group.get("end_date"))
        if end_date is None or end_date <= now:
            # No end date means it cannot be ranked by "resolving soonest".
            dropped["no_end_date"] += 1
            continue

        key = (group.get("condition_id", ""), group.get("outcome", ""))
        token_id = token_index.get(key)
        if not token_id:
            dropped["no_token"] += 1
            continue

        cohort = cohort_index.get(key, [])
        if not cohort:
            # No cohort means no one to follow back out of the trade.
            dropped["no_cohort"] += 1
            continue

        pool.append({
            "condition_id": group.get("condition_id", ""),
            "token_id": token_id,
            "market_title": group.get("market_title", ""),
            "slug": group.get("slug", ""),
            "outcome_to_buy": group.get("outcome", ""),
            "current_price": _as_money(group.get("cur_price")),
            "cohort_value_usd": _as_money(group.get("total_value")),
            "end_date": group.get("end_date"),
            "days_to_expiry": (end_date - now).days,
            "cohort": cohort,
            "conviction_score": len(cohort),
            "tier": SELECTION_LABEL,
        })

    log.info("Overlaps: %d read, %d eligible  (dropped %d under way, "
             "%d no end date, %d no token id, %d no cohort)",
             len(overlaps.get("overlaps", [])), len(pool), dropped["under_way"],
             dropped["no_end_date"], dropped["no_token"], dropped["no_cohort"])

    # One side per market, or none at all. The overlaps file lists each outcome
    # separately, so a market the cohort is split on shows up twice -- and
    # buying both Over at 0.62 and Under at 0.38 pays $1.00 for $1.00 back,
    # minus two spreads and two fees. Worse, open_positions is keyed by
    # condition_id, so the second fill would overwrite the first: cash gone
    # twice, one position tracked.
    #
    # When the cohort is split, follow the weight of money first and the head
    # count second. If both are level the cohort has expressed no view on which
    # way to lean, so the market is dropped rather than guessed at.
    sides: dict[str, list[dict]] = {}
    for candidate in pool:
        sides.setdefault(candidate["condition_id"], []).append(candidate)

    best_side: dict[str, dict] = {}
    contested = deadlocked = 0

    for cid, group in sides.items():
        if len(group) == 1:
            best_side[cid] = group[0]
            continue

        contested += 1
        ranked = sorted(group, key=_side_weight, reverse=True)
        top, runner_up = ranked[0], ranked[1]

        if _pick_side(group) is None:
            deadlocked += 1
            log.info("  SPLIT  %-42s too close: $%s v $%s (gap $%s) - skipped",
                     top["market_title"][:42],
                     "{:,.0f}".format(top["cohort_value_usd"]),
                     "{:,.0f}".format(runner_up["cohort_value_usd"]),
                     "{:,.0f}".format(top["cohort_value_usd"]
                                      - runner_up["cohort_value_usd"]))
            continue

        best_side[cid] = top
        log.info("  SPLIT  %-42s %-12s over %-12s  $%s v $%s, %d v %d traders",
                 top["market_title"][:42], top["outcome_to_buy"][:12],
                 runner_up["outcome_to_buy"][:12],
                 "{:,.0f}".format(top["cohort_value_usd"]),
                 "{:,.0f}".format(runner_up["cohort_value_usd"]),
                 top["conviction_score"], runner_up["conviction_score"])

    if contested:
        log.info("Cohort split on %d market(s): kept %d with a decisive margin, "
                 "dropped %d as too close to call (< $%s).",
                 contested, contested - deadlocked, deadlocked,
                 "{:,.0f}".format(SPLIT_DECISIVE_MARGIN_USD))

    by_value = sorted(best_side.values(), key=lambda c: c["cohort_value_usd"],
                      reverse=True)[:TOP_BY_VALUE]
    chosen = sorted(by_value, key=lambda c: c["end_date"])[:TRADE_SOONEST]

    if by_value:
        log.info("Top %d by cohort money: $%s at the top, $%s at the cut",
                 len(by_value), "{:,.0f}".format(by_value[0]["cohort_value_usd"]),
                 "{:,.0f}".format(by_value[-1]["cohort_value_usd"]))

    # Equal weight: nothing is opened after seeding, so there is no reason to
    # hold anything back beyond the standing cash reserve.
    investable = PORTFOLIO_SIZE * (1 - CASH_RESERVE_PCT)
    per_trade = investable / len(chosen) if chosen else 0.0
    for candidate in chosen:
        candidate["recommended_trade_amount_usd"] = per_trade

    log.info("Trading the %d resolving soonest, $%s each:",
             len(chosen), "{:,.2f}".format(per_trade))
    for i, c in enumerate(chosen, 1):
        log.info("  %2d. %-42s %-14s %4dd  $%-12s %d traders", i,
                 c["market_title"][:42], c["outcome_to_buy"][:14],
                 c["days_to_expiry"], "{:,.0f}".format(c["cohort_value_usd"]),
                 c["conviction_score"])

    return chosen


# ════════════════════════════════ PRICE FEED ══════════════════════════════════


def _prices_via_clob_batch(token_ids: list[str]) -> dict[str, float]:
    """Primary: CLOB batch /prices. Mid-price from best bid and best ask."""
    payload = []
    for tid in token_ids:
        payload.append({"token_id": tid, "side": "BUY"})
        payload.append({"token_id": tid, "side": "SELL"})
    data = api_post(PRICES_URL, payload, label=f"clob /prices x{len(token_ids)}")
    if not isinstance(data, dict):
        return {}

    out: dict[str, float] = {}
    for tid, sides in data.items():
        if not isinstance(sides, dict):
            continue
        buy = _as_float(sides.get("BUY"))
        sell = _as_float(sides.get("SELL"))
        quotes = [q for q in (buy, sell) if q is not None and q > 0]
        if quotes:
            out[str(tid)] = sum(quotes) / len(quotes)
    return out


def _prices_via_clob_midpoint(token_ids: list[str]) -> dict[str, float]:
    """Fallback: one /midpoint call per token."""
    out: dict[str, float] = {}
    for tid in token_ids:
        try:
            data = api_get(MIDPOINT_URL, {"token_id": tid}, label=f"midpoint {tid[:12]}…")
        except Exception as exc:
            log.debug("Midpoint failed for %s…: %s", tid[:12], exc)
            continue
        mid = _as_float(data.get("mid")) if isinstance(data, dict) else None
        if mid is not None and mid > 0:
            out[tid] = mid
    return out


def _prices_via_gamma(positions: list[PaperPosition]) -> dict[str, float]:
    """Last resort: Gamma /markets, matching outcome name → outcomePrices."""
    out: dict[str, float] = {}
    condition_ids = [p.condition_id for p in positions]
    by_condition = {p.condition_id: p for p in positions}

    for i in range(0, len(condition_ids), 20):
        chunk = condition_ids[i:i + 20]
        try:
            data = api_get(GAMMA_MARKETS_URL, {"condition_ids": chunk},
                           label=f"gamma markets x{len(chunk)}")
        except Exception as exc:
            log.debug("Gamma price fetch failed: %s", exc)
            continue
        if not isinstance(data, list):
            continue

        for market in data:
            pos = by_condition.get(market.get("conditionId", ""))
            if pos is None:
                continue
            outcomes = _decode_json_field(market.get("outcomes"))
            prices = _decode_json_field(market.get("outcomePrices"))
            if not outcomes or len(outcomes) != len(prices):
                continue
            for name, price in zip(outcomes, prices):
                if str(name).strip().lower() == pos.outcome.strip().lower():
                    val = _as_float(price)
                    if val is not None and val > 0:
                        out[pos.token_id] = val
                    break
    return out


def _decode_json_field(value: Any) -> list:
    """Gamma returns some list fields as JSON-encoded strings."""
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, list) else []
        except json.JSONDecodeError:
            return []
    return []


def fetch_prices(positions: list[PaperPosition]) -> dict[str, float]:
    """
    Live price per token id, walking a fallback chain so one flaky endpoint
    cannot blank the whole book.
    """
    if not positions:
        return {}
    token_ids = [p.token_id for p in positions]

    try:
        prices = _prices_via_clob_batch(token_ids)
    except Exception as exc:
        log.warning("CLOB batch prices unavailable (%s) — falling back to /midpoint.", exc)
        prices = {}

    missing = [t for t in token_ids if t not in prices]
    if missing:
        prices.update(_prices_via_clob_midpoint(missing))

    missing = [t for t in token_ids if t not in prices]
    if missing:
        stragglers = [p for p in positions if p.token_id in set(missing)]
        prices.update(_prices_via_gamma(stragglers))

    missing = [t for t in token_ids if t not in prices]
    if missing:
        log.warning("No live price for %d/%d positions — holding last known price.",
                    len(missing), len(token_ids))
    return prices


def fetch_price_for_token(token_id: str, condition_id: str, outcome: str) -> float | None:
    """Single live price, used when opening a position."""
    probe = PaperPosition(
        condition_id=condition_id, token_id=token_id, market_title="", slug="",
        outcome=outcome, shares=0.0, entry_price=0.0, cost_basis=0.0,
        opened_at="", conviction_score=0, tier="",
    )
    return fetch_prices([probe]).get(token_id)


# ═════════════════════════════ RESOLUTION CHECK ═══════════════════════════════


def check_resolution(position: PaperPosition) -> tuple[bool, float, str] | None:
    """
    Ask the CLOB whether the market has resolved.

    Returns (resolved, settlement_price, detail), or None if the status could
    not be determined. Settlement price is 1.0 for the winning outcome, 0.0 for
    a loser — the position is worth exactly par or nothing.
    """
    try:
        data = api_get(f"{CLOB_MARKETS_URL}/{position.condition_id}",
                       label=f"market {position.condition_id[:12]}…")
    except Exception as exc:
        log.debug("Resolution check failed for %s…: %s", position.condition_id[:12], exc)
        return None
    if not isinstance(data, dict):
        return None

    closed = bool(data.get("closed", False))
    if not closed:
        return (False, 0.0, "")

    for token in data.get("tokens", []):
        if str(token.get("token_id", "")) != position.token_id:
            continue
        winner = token.get("winner")
        if winner is None:
            return None  # closed but not yet adjudicated — wait
        return (True, 1.0 if winner else 0.0,
                "won" if winner else "lost")

    return None


# ═══════════════════════════════ EXIT SIGNALS ═════════════════════════════════


def load_exit_signals() -> dict[str, dict]:
    """Read trader_exit_monitor.py's output, ignoring it if it has gone stale."""
    if not EXIT_SIGNALS_FILE.exists():
        return {}
    try:
        data = json.loads(EXIT_SIGNALS_FILE.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        log.warning("Could not read exit signals: %s", exc)
        return {}

    stamp = data.get("metadata", {}).get("timestamp", "")
    try:
        written = datetime.strptime(stamp, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except ValueError:
        return {}

    age = (datetime.now(timezone.utc) - written).total_seconds()
    if age > MAX_SIGNAL_AGE_SECONDS:
        log.warning("Exit signals are %.0f min old — ignoring. Is trader_exit_monitor.py running?",
                    age / 60)
        return {}

    return data.get("signals", {})


# ========================= EXECUTION AGAINST THE BOOK ==========================


@dataclass
class ExecutedFill:
    """What an order actually did once it had walked the book."""
    shares: float
    avg_price: float
    cash: float            # USD out on a buy, USD in on a sell (net of fee)
    fee: float
    slippage_bps: float
    levels: int
    partial: bool
    note: str = ""


def _midpoint_fill(shares: float, price: float, cash: float, note: str) -> ExecutedFill:
    """A frictionless fill, for when there is no book to walk."""
    return ExecutedFill(shares, price, cash, 0.0, 0.0, 0, False, note)


def _execute_buy(token_id: str, size_usd: float, mid_price: float,
                 dry_run: bool) -> ExecutedFill | None:
    """
    Spend size_usd walking the ask side, cheapest level first.

    Returns None when the trade should not be taken at all. When costing is
    switched off, or the book cannot be fetched, this falls back to a
    frictionless midpoint fill -- a costless fill beats silently dropping a
    trade the strategy asked for.
    """
    if not EXECUTION_COSTS or dry_run:
        return _midpoint_fill(size_usd / mid_price, mid_price, size_usd, "midpoint")

    book = books.fetch_order_book(token_id, api_get)
    if book is None:
        # The API blinked. Do not drop a trade the strategy asked for over
        # a failed request -- fill it flat and carry on.
        log.warning("    book unavailable for %s... - filling at the midpoint",
                    token_id[:12])
        return _midpoint_fill(size_usd / mid_price, mid_price, size_usd,
                              "midpoint (book unavailable)")
    if not book.asks:
        # The book came back and there is genuinely nothing offered.
        return None

    if FLAT_FEE_PCT is not None:
        # size_usd is the all-in budget, so solve for the notional exactly:
        #   notional + notional * pct = size_usd
        # No second pass needed, unlike the per-share formula below.
        notional = size_usd / (1.0 + FLAT_FEE_PCT / 100.0)
        r = simulate_buy_fill(book, notional, 0, order_type=ORDER_TYPE)
        if r.total_shares <= 0:
            return None
        fee = r.total_cost * FLAT_FEE_PCT / 100.0
    else:
        fee_bps = (FEE_BPS_OVERRIDE if FEE_BPS_OVERRIDE is not None
                   else books.fetch_fee_rate_bps(token_id, api_get, DEFAULT_FEE_BPS))
        r = simulate_buy_fill(book, size_usd, fee_bps, order_type=ORDER_TYPE)
        if r.total_shares <= 0:
            return None

        # simulate_buy_fill spends the whole notional on shares and adds the
        # fee on top, so a $900 order actually costs $900 + fee. Budget
        # size_usd as the all-in number instead: shrink the notional by the
        # fee ratio the first pass measured, then fill again. One correction
        # is enough -- avg_price barely moves for a few percent less size.
        if r.fee > 0 and r.total_cost + r.fee > size_usd:
            adjusted = simulate_buy_fill(
                book, size_usd * (size_usd / (r.total_cost + r.fee)),
                fee_bps, order_type=ORDER_TYPE)
            if adjusted.total_shares > 0:
                r = adjusted
        fee = r.fee
    if r.slippage_bps > MAX_ENTRY_SLIPPAGE_BPS:
        log.warning("    entry slippage %.0f bps exceeds the %.0f bps limit",
                    r.slippage_bps, MAX_ENTRY_SLIPPAGE_BPS)
        return None

    return ExecutedFill(r.total_shares, r.avg_price, r.total_cost + fee,
                        fee, r.slippage_bps, r.levels_filled, r.is_partial)


def _execute_sell(token_id: str, shares: float, mid_price: float,
                  dry_run: bool) -> ExecutedFill | None:
    """Sell shares into the bid side. None when there is no bid to hit."""
    if not EXECUTION_COSTS or dry_run:
        return _midpoint_fill(shares, mid_price, shares * mid_price, "midpoint")

    book = books.fetch_order_book(token_id, api_get)
    if book is None:
        log.warning("    book unavailable for %s... - marking out at the midpoint",
                    token_id[:12])
        return _midpoint_fill(shares, mid_price, shares * mid_price,
                              "midpoint (book unavailable)")
    if not book.bids:
        # Nobody is bidding. Inventing a midpoint exit here would hide the
        # exact liquidity risk this whole module exists to measure.
        return None

    if FLAT_FEE_PCT is not None:
        r = simulate_sell_fill(book, shares, 0, order_type=ORDER_TYPE)
        if r.total_shares <= 0:
            return None
        fee = r.total_cost * FLAT_FEE_PCT / 100.0
    else:
        fee_bps = (FEE_BPS_OVERRIDE if FEE_BPS_OVERRIDE is not None
                   else books.fetch_fee_rate_bps(token_id, api_get, DEFAULT_FEE_BPS))
        r = simulate_sell_fill(book, shares, fee_bps, order_type=ORDER_TYPE)
        if r.total_shares <= 0:
            return None
        fee = r.fee

    return ExecutedFill(r.total_shares, r.avg_price, r.total_cost - fee,
                        fee, r.slippage_bps, r.levels_filled, r.is_partial)


def _charge(portfolio: Portfolio, fill: ExecutedFill, mid_price: float) -> None:
    """Split a fill shortfall against the midpoint into fees and slippage."""
    portfolio.total_fees += fill.fee
    if mid_price > 0 and fill.shares > 0:
        portfolio.total_slippage_cost += abs(fill.avg_price - mid_price) * fill.shares


# ============================= PORTFOLIO ACTIONS ==============================


def open_position(portfolio: Portfolio, candidate: dict, price: float,
                  size_usd: float, dry_run: bool = False) -> None:
    """Buy size_usd of the candidate outcome, filled against the live book."""
    fill = _execute_buy(candidate["token_id"], size_usd, price, dry_run)
    if fill is None:
        log.warning("  SKIP   %-46s book too thin to enter",
                    candidate.get("market_title", "?")[:46])
        return

    pos = PaperPosition(
        condition_id=candidate["condition_id"],
        token_id=candidate["token_id"],
        market_title=candidate.get("market_title", ""),
        slug=candidate.get("slug", ""),
        outcome=candidate.get("outcome_to_buy", ""),
        shares=fill.shares,
        entry_price=fill.avg_price,
        cost_basis=fill.cash,
        opened_at=_now_iso(),
        conviction_score=int(candidate.get("conviction_score", 0)),
        tier=candidate.get("tier", ""),
        cohort=candidate["cohort"],
        # Marked at the midpoint, so a position opens down by exactly what
        # getting into it just cost.
        last_price=price,
        last_price_at=_now_iso(),
        mid_at_entry=price,
        entry_fee=fill.fee,
        entry_slippage_bps=fill.slippage_bps,
        entry_levels=fill.levels,
    )
    portfolio.cash -= fill.cash
    portfolio.open_positions[pos.condition_id] = pos
    _charge(portfolio, fill, price)

    drag = ""
    if fill.slippage_bps or fill.fee:
        drag = "  [%+.0fbps, fee $%.2f, %d lvl]" % (
            fill.slippage_bps, fill.fee, fill.levels)
    if fill.partial:
        drag += "  PARTIAL"
    log.info("  OPEN   %-46s %-18s %6.1f¢  %8.2f sh  $%s%s",
             pos.market_title[:46], pos.outcome[:18], fill.avg_price * 100,
             fill.shares, "{:,.2f}".format(fill.cash), drag)


def close_position(portfolio: Portfolio, position: PaperPosition, exit_price: float,
                   reason: str, detail: str = "", dry_run: bool = False) -> None:
    """
    Sell the position and book the realized P&L.

    Resolution is settlement, not a trade: the shares redeem at face value with
    no spread and no fee, so it bypasses the book entirely. Every other exit
    has to find a bid, and may only find part of one.
    """
    settled = reason == "resolved"
    if settled:
        fill = _midpoint_fill(position.shares, exit_price,
                              position.shares * exit_price, "settlement")
    else:
        fill = _execute_sell(position.token_id, position.shares, exit_price, dry_run)
        if fill is None:
            log.warning("  HOLD   %-46s no bid to exit into - retrying next cycle",
                        position.market_title[:46])
            return

    sold = min(fill.shares, position.shares)
    frac = sold / position.shares if position.shares > 0 else 1.0
    cost_share = position.cost_basis * frac
    proceeds = fill.cash

    closed = ClosedPosition(
        condition_id=position.condition_id,
        market_title=position.market_title,
        outcome=position.outcome,
        shares=sold,
        entry_price=position.entry_price,
        exit_price=fill.avg_price,
        cost_basis=cost_share,
        proceeds=proceeds,
        realized_pnl=proceeds - cost_share,
        opened_at=position.opened_at,
        closed_at=_now_iso(),
        exit_reason=reason,
        detail=detail,
        entry_fee=position.entry_fee * frac,
        exit_fee=fill.fee,
        exit_slippage_bps=fill.slippage_bps,
    )
    portfolio.cash += proceeds
    portfolio.closed_positions.append(closed)
    if not settled:
        _charge(portfolio, fill, exit_price)

    remaining = position.shares - sold
    if remaining > 1e-6:
        # The bids could only absorb part of it -- keep the rest on the book
        # and try again next cycle rather than pretending it all cleared.
        position.shares = remaining
        position.cost_basis -= cost_share
        position.entry_fee *= (1.0 - frac)
        log.warning("  PART   %-46s sold %.2f of %.2f sh - holding the rest",
                    position.market_title[:46], sold, sold + remaining)
    else:
        portfolio.open_positions.pop(position.condition_id, None)

    log.info("  CLOSE  %-46s %-18s %6.1f¢  P&L $%-10s (%s)",
             position.market_title[:46], position.outcome[:18], fill.avg_price * 100,
             "{:+,.2f}".format(closed.realized_pnl), detail or reason)


def _probe_for(candidate: dict) -> PaperPosition:
    """A throwaway position used only to reuse the price/resolution plumbing."""
    return PaperPosition(
        condition_id=candidate["condition_id"], token_id=candidate["token_id"],
        market_title=candidate.get("market_title", ""), slug=candidate.get("slug", ""),
        outcome=candidate.get("outcome_to_buy", ""), shares=0.0, entry_price=0.0,
        cost_basis=0.0, opened_at="", conviction_score=0, tier="",
    )


def price_candidates(candidates: list[dict], dry_run: bool) -> dict[str, float | None]:
    """Live price per candidate, batched. None where the market cannot be priced."""
    if dry_run:
        return {c["condition_id"]: float(c.get("current_price", 0)) for c in candidates}
    by_token = fetch_prices([_probe_for(c) for c in candidates])
    return {c["condition_id"]: by_token.get(c["token_id"]) for c in candidates}


def triage_candidates(candidates: list[dict], dry_run: bool) -> tuple[list, list, list]:
    """
    Split candidates into (tradeable, resolved, unpriceable).

    A recommendation whose market has already resolved is dead — sports markets
    in particular settle within hours of the pipeline run that produced them.
    """
    prices = price_candidates(candidates, dry_run)
    tradeable, resolved, unpriceable = [], [], []

    for candidate in candidates:
        price = prices.get(candidate["condition_id"])
        if price is not None and MIN_TRADEABLE_PRICE <= price <= MAX_TRADEABLE_PRICE:
            tradeable.append((candidate, price))
        elif dry_run:
            unpriceable.append((candidate, "no recorded price"))
        else:
            # Distinguish "already settled" from "temporarily unpriceable" so the
            # log says something actionable.
            status = check_resolution(_probe_for(candidate))
            if status is not None and status[0]:
                resolved.append((candidate, status[2]))
            else:
                unpriceable.append((candidate, "no live price / no order book"))

    return tradeable, resolved, unpriceable


def seed_positions(portfolio: Portfolio, candidates: list[dict], dry_run: bool) -> None:
    """Open the initial book, scaled to fit the portfolio's investable capital."""
    if not candidates:
        log.warning("No candidates to trade - the overlap file yielded none.")
        return

    log.info("Pricing %d selected candidates…", len(candidates))
    tradeable, resolved, unpriceable = triage_candidates(candidates, dry_run)

    for candidate, detail in resolved:
        portfolio.dead_markets.add(candidate["condition_id"])
        log.warning("  SKIP   %-46s already resolved (%s)",
                    candidate.get("market_title", "?")[:46], detail)
    for candidate, reason in unpriceable:
        log.warning("  SKIP   %-46s %s",
                    candidate.get("market_title", "?")[:46], reason)

    if not tradeable:
        log.error("None of the %d selected markets are still tradeable.",
                  len(candidates))
        log.error("The pipeline output has gone stale — refresh it first:")
        log.error("    python run_all.py")
        return

    # Scale across only the trades we can actually open, so a stale batch does
    # not leave the portfolio sitting in cash.
    requested = sum(float(c.get("recommended_trade_amount_usd", 0)) for c, _ in tradeable)
    investable = portfolio.starting_capital * (1 - CASH_RESERVE_PCT)
    scale = min(1.0, investable / requested) if requested > 0 else 0.0

    log.info("Sizing: %d tradeable of %d requesting $%s, investable $%s → scale %.4f",
             len(tradeable), len(candidates), f"{requested:,.2f}",
             f"{investable:,.2f}", scale)

    for candidate, price in tradeable:
        size = float(candidate.get("recommended_trade_amount_usd", 0)) * scale
        if size <= 0 or size > portfolio.cash:
            log.warning("Insufficient cash for %s — skipping.",
                        candidate.get("market_title", "?")[:50])
            continue
        portfolio.per_trade_size = size
        open_position(portfolio, candidate, price, size, dry_run)

    if resolved:
        log.warning("%d of %d recommendations had already resolved — "
                    "re-run `python run_all.py` for a fresh set.",
                    len(resolved), len(candidates))


# ══════════════════════════════════ CYCLE ═════════════════════════════════════


def run_cycle(portfolio: Portfolio, dry_run: bool) -> None:
    """One mark-to-market pass: settle, exit, re-price, top up, report."""
    portfolio.cycles += 1

    # ── 1. Settle resolved markets FIRST ──
    # A resolved market makes traders redeem, which empties their positions and
    # would otherwise read as a unanimous cohort exit rather than a settlement.
    if not dry_run and portfolio.cycles % RESOLUTION_CHECK_CYCLES == 1:
        for position in list(portfolio.open_positions.values()):
            outcome = check_resolution(position)
            if outcome is None:
                continue
            resolved, settlement, detail = outcome
            if resolved:
                close_position(portfolio, position, settlement, "resolved",
                               f"market resolved — {detail}")

    # ── 2. Honour trader-majority exit signals ──
    signals = load_exit_signals()
    for position in list(portfolio.open_positions.values()):
        signal_data = signals.get(position.condition_id)
        if not signal_data or not signal_data.get("should_exit"):
            continue
        price = position.last_price
        if not dry_run:
            price = fetch_price_for_token(
                position.token_id, position.condition_id, position.outcome) or price
        close_position(
            portfolio, position, price, "trader_majority_exit",
            f"{signal_data.get('exited', '?')}/{signal_data.get('cohort_size', '?')} traders exited",
            dry_run=dry_run,
        )

    # ── 3. Mark the remaining book to market ──
    open_positions = list(portfolio.open_positions.values())
    if open_positions and not dry_run:
        prices = fetch_prices(open_positions)
        stamp = _now_iso()
        for position in open_positions:
            price = prices.get(position.token_id)
            if price is None:
                continue  # keep last known price rather than marking to zero
            position.last_price = price
            position.last_price_at = stamp

    # ── 4. Report & persist ──
    print_dashboard(portfolio)
    append_equity_snapshot(portfolio)
    portfolio.save()


def append_equity_snapshot(portfolio: Portfolio) -> None:
    """One JSONL line per cycle — this is the growth/fall curve."""
    snapshot = {
        "timestamp": _now_iso(),
        "cycle": portfolio.cycles,
        "equity": round(portfolio.equity, 2),
        "cash": round(portfolio.cash, 2),
        "deployed": round(portfolio.deployed, 2),
        "unrealized_pnl": round(portfolio.unrealized_pnl, 2),
        "realized_pnl": round(portfolio.realized_pnl, 2),
        "total_return_pct": round(portfolio.total_return_pct, 4),
        "open_count": len(portfolio.open_positions),
        "closed_count": len(portfolio.closed_positions),
        "total_fees": round(portfolio.total_fees, 2),
        "total_slippage_cost": round(portfolio.total_slippage_cost, 2),
        "execution_cost": round(portfolio.execution_cost, 2),
    }
    with open(EQUITY_HISTORY_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(snapshot) + "\n")


# ════════════════════════════════ REPORTING ═══════════════════════════════════


def terminal_width() -> int:
    """
    Usable console width.

    Matters for the two-pane layout: a half-screen pane is often ~80 columns,
    and a fixed-width frame would wrap every row, wrecking the in-place repaint.
    """
    try:
        columns = shutil.get_terminal_size((DASHBOARD_MAX_WIDTH, 24)).columns
    except Exception:
        columns = DASHBOARD_MAX_WIDTH
    return max(DASHBOARD_MIN_WIDTH, min(DASHBOARD_MAX_WIDTH, columns - 1))


def print_dashboard(portfolio: Portfolio) -> None:
    width = terminal_width()
    sep = "═" * width
    thin = "─" * width
    pnl_total = portfolio.equity - portfolio.starting_capital
    arrow = "▲" if pnl_total > 0 else ("▼" if pnl_total < 0 else "•")

    # Drop the Value column and tighten names when the pane is narrow.
    compact = width < 100
    outcome_w = 14 if compact else 20
    numeric_w = 7 + 1 + 7 + 1 + 11 + 1 + 8 + (0 if compact else 11)
    title_w = max(14, width - 3 - outcome_w - 1 - numeric_w)

    if _live_mode:
        # Home the cursor and clear, so the frame repaints in place instead of
        # scrolling a new block every cycle.
        print("\033[H\033[J", end="")
    print()
    print(sep)
    print(f"  PAPER PORTFOLIO  │  cycle {portfolio.cycles}  │  {_now_iso()}")
    print(sep)
    print(f"  Equity  ${portfolio.equity:>12,.2f}   "
          f"{arrow} {pnl_total:+,.2f}  ({portfolio.total_return_pct:+.2f}%)")
    print(f"  Cash    ${portfolio.cash:>12,.2f}   "
          f"Deployed ${portfolio.deployed:,.2f}   "
          f"started ${portfolio.starting_capital:,.2f}")
    print(f"  P&L     unrealized ${portfolio.unrealized_pnl:+,.2f}   "
          f"realized ${portfolio.realized_pnl:+,.2f}   "
          f"open {len(portfolio.open_positions)}   "
          f"closed {len(portfolio.closed_positions)}")
    if portfolio.execution_cost:
        bled = portfolio.execution_cost / portfolio.starting_capital * 100
        print(f"  Costs   fees ${portfolio.total_fees:,.2f}   "
              f"slippage ${portfolio.total_slippage_cost:,.2f}   "
              f"total ${portfolio.execution_cost:,.2f} ({bled:.2f}% of start)")
    print(thin)

    def row(title: str, outcome: str, left: float, right: float,
            value: float, pnl: float, ret: float, suffix: str = "") -> str:
        cells = (f"  {title[:title_w]:<{title_w}} {outcome[:outcome_w]:<{outcome_w}} "
                 f"{left * 100:>6.1f}¢ {right * 100:>6.1f}¢")
        if not compact:
            cells += f" {value:>10,.2f}"
        cells += f" {pnl:>+11,.2f} {ret:>+7.1f}%"
        # Clamp: the exit-reason suffix can otherwise push a closed row past the
        # frame, and one wrapped line desynchronises the whole in-place repaint.
        return (cells + suffix)[:width]

    if portfolio.open_positions:
        header = (f"  {'Market':<{title_w}} {'Outcome':<{outcome_w}} "
                  f"{'Entry':>7} {'Now':>7}")
        if not compact:
            header += f" {'Value':>10}"
        header += f" {'P&L':>11} {'Ret':>8}"
        print(header)
        print("  " + thin[2:])
        ranked = sorted(portfolio.open_positions.values(),
                        key=lambda p: p.unrealized_pnl, reverse=True)
        for p in ranked:
            print(row(p.market_title, p.outcome, p.entry_price, p.last_price,
                      p.market_value, p.unrealized_pnl, p.return_pct))

    if portfolio.closed_positions:
        print(thin)
        print(f"  CLOSED ({len(portfolio.closed_positions)})")
        for c in portfolio.closed_positions[-10:]:
            print(row(c.market_title, c.outcome, c.entry_price, c.exit_price,
                      c.proceeds, c.realized_pnl, c.return_pct,
                      "" if compact else f"   {c.exit_reason}"))

    if _live_mode and _recent_activity.records:
        print(thin)
        print("  RECENT ACTIVITY   (full history in output/paper_trade.log)")
        for stamp, level, message in _recent_activity.records:
            marker = "!" if level in ("WARNING", "ERROR", "CRITICAL") else " "
            print(f"  {marker} {stamp}  {message[:width - 14]}")

    print(sep)


def print_countdown(seconds_left: int) -> None:
    """Redraw just the footer line while waiting for the next cycle."""
    if not _live_mode:
        return
    # \r returns to column 0; the trailing spaces clear the previous, longer line.
    print(f"\r  next refresh in {seconds_left:>2}s — Ctrl+C to stop      ",
          end="", flush=True)


# ═══════════════════════════════════ MAIN ═════════════════════════════════════


_shutdown = False


def _handle_signal(signum, frame) -> None:  # noqa: ARG001
    global _shutdown
    _shutdown = True
    print()
    log.info("Shutdown requested — finishing current cycle…")


def run() -> None:
    parser = argparse.ArgumentParser(description="Polymarket paper-trading engine")
    parser.add_argument("--dry-run", action="store_true",
                        help="price from recorded current_price, run one cycle, make no network calls")
    parser.add_argument("--once", action="store_true", help="run a single cycle then exit")
    parser.add_argument("--reset", action="store_true",
                        help="discard any existing portfolio and start fresh")
    parser.add_argument("--plain", action="store_true",
                        help="scrolling log output instead of the in-place live dashboard")
    args = parser.parse_args()

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    # Live repaint only makes sense for the continuous loop on a real terminal.
    # --once/--dry-run stay plain so their output pipes cleanly into tests.
    live = (not args.plain and not args.once and not args.dry_run
            and sys.stdout.isatty())

    log.info("=" * 118)
    log.info("Polymarket Paper-Trading Engine — %s", _now_iso())
    log.info("=" * 118)

    if args.reset and PORTFOLIO_FILE.exists():
        PORTFOLIO_FILE.unlink()
        log.info("Existing portfolio discarded (--reset).")

    portfolio = None if args.reset else Portfolio.load()
    if portfolio is None:
        log.info("Selecting the book: top %d by cohort money, "
                 "then the %d resolving soonest…", TOP_BY_VALUE, TRADE_SOONEST)
        portfolio = Portfolio()
        seed_positions(portfolio, load_candidates(), args.dry_run)
        portfolio.save()
    else:
        log.info("Resumed existing portfolio — %d open, %d closed, $%s cash, started %s",
                 len(portfolio.open_positions), len(portfolio.closed_positions),
                 f"{portfolio.cash:,.2f}", portfolio.started_at)

    if args.dry_run or args.once:
        run_cycle(portfolio, args.dry_run)
        log.info("Single cycle complete.")
        return

    log.info("Refreshing every %ds. Press Ctrl+C to stop.", REFRESH_SECONDS)
    if live:
        enable_live_mode()

    while not _shutdown:
        try:
            run_cycle(portfolio, dry_run=False)
        except Exception:
            log.exception("Cycle failed — retrying next interval.")

        for remaining in range(REFRESH_SECONDS, 0, -1):
            if _shutdown:
                break
            print_countdown(remaining)
            time.sleep(1)

    disable_live_mode()
    print()
    portfolio.save()
    log.info("Portfolio saved to %s", PORTFOLIO_FILE)
    log.info("Final equity $%s (%+.2f%%)",
             f"{portfolio.equity:,.2f}", portfolio.total_return_pct)


if __name__ == "__main__":
    run()

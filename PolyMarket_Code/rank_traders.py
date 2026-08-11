#!/usr/bin/env python3
"""
Polymarket Trader Ranking System  (Script 3)
=============================================

Fetches historical trade data for the top-100 weekly traders and ranks
them on a **0–100 score** based on:

    Win rate              ×  40 %
    ROI                   ×  30 %
    Capital-weighted PnL  ×  30 %

Designed to run **concurrently** with polymarket_top_traders_scraper.py.
Both scripts independently fetch the leaderboard, then diverge: the
scraper pulls open positions while this script pulls full trade history.

Data sources (public, no auth):
    • data-api.polymarket.com/v1/leaderboard
    • data-api.polymarket.com/activity   (trade history, up to offset 5 000)
    • data-api.polymarket.com/positions  (current open positions for PnL)

Author : Antigravity AI
Version: 1.0.0
"""

from __future__ import annotations

import json
import logging
import sys
import time
from collections import defaultdict
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ═══════════════════════════════════ CONFIG ═══════════════════════════════════

DATA_API = "https://data-api.polymarket.com"
LEADERBOARD_URL = f"{DATA_API}/v1/leaderboard"
ACTIVITY_URL = f"{DATA_API}/activity"
POSITIONS_URL = f"{DATA_API}/positions"

TOP_N = 100
LB_PAGE = 50
LB_CATEGORY = "OVERALL"
LB_PERIOD = "WEEK"

ACTIVITY_PAGE = 500
ACTIVITY_MAX_OFFSET = 500  # limits to latest 1000 trades

POSITIONS_PAGE = 500
POSITIONS_MAX_OFFSET = 5_000

# Rate-limiting — slightly conservative so both scripts can coexist
RATE_RPS = 5
BACKOFF_BASE = 2.0
BACKOFF_MAX = 60.0
MAX_RETRIES = 5

# Scoring weights (must sum to 100)
W_WIN_RATE = 40
W_ROI = 30
W_CAPITAL_PNL = 30

OUTPUT_DIR = Path(__file__).resolve().parent / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ═══════════════════════════════════ LOGGING ══════════════════════════════════

LOG_FMT = "%(asctime)s │ %(levelname)-8s │ %(message)s"
DATE_FMT = "%Y-%m-%d %H:%M:%S"
logging.basicConfig(
    level=logging.INFO,
    format=LOG_FMT,
    datefmt=DATE_FMT,
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(OUTPUT_DIR / "ranker.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("trader_ranker")

# ════════════════════════════════ HTTP HELPERS ════════════════════════════════


def _build_session() -> requests.Session:
    s = requests.Session()
    s.verify = False
    s.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"})
    retry = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update({
        "Accept": "application/json",
        "User-Agent": "PolymarketTraderRanker/1.0",
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


def api_get(url: str, params: dict | None = None, label: str = "req") -> Any:
    """GET with rate-limiting, exponential back-off, and retries."""
    _limiter.wait()
    backoff = BACKOFF_BASE
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = SESSION.get(url, params=params, timeout=30)
            if r.status_code == 429:
                wait = float(r.headers.get("Retry-After", backoff))
                log.warning(
                    "Rate-limited on %s (att %d/%d). Waiting %.1fs…",
                    label, attempt, MAX_RETRIES, wait,
                )
                time.sleep(wait)
                backoff = min(backoff * 2, BACKOFF_MAX)
                continue
            if r.status_code >= 500:
                log.warning(
                    "Server %d on %s (att %d/%d). Waiting %.1fs…",
                    r.status_code, label, attempt, MAX_RETRIES, backoff,
                )
                time.sleep(backoff)
                backoff = min(backoff * 2, BACKOFF_MAX)
                continue
            r.raise_for_status()
            return r.json()
        except requests.exceptions.ConnectionError as exc:
            log.warning("Conn error on %s (att %d/%d): %s", label, attempt, MAX_RETRIES, exc)
            time.sleep(backoff)
            backoff = min(backoff * 2, BACKOFF_MAX)
        except requests.exceptions.Timeout:
            log.warning("Timeout on %s (att %d/%d)", label, attempt, MAX_RETRIES)
            time.sleep(backoff)
            backoff = min(backoff * 2, BACKOFF_MAX)
    raise RuntimeError(f"Failed {label} after {MAX_RETRIES} attempts")


# ═══════════════════════════════ DATA FETCHERS ═══════════════════════════════


def fetch_leaderboard() -> list[dict]:
    """Fetch the top-N traders from the weekly leaderboard."""
    traders: list[dict] = []
    offset = 0
    while len(traders) < TOP_N:
        page = min(LB_PAGE, TOP_N - len(traders))
        params = {
            "category": LB_CATEGORY,
            "timePeriod": LB_PERIOD,
            "orderBy": "PNL",
            "limit": page,
            "offset": offset,
        }
        log.info("Fetching leaderboard (offset=%d, limit=%d)…", offset, page)
        data = api_get(LEADERBOARD_URL, params, f"leaderboard off={offset}")
        if not data:
            break
        for e in data:
            traders.append({
                "rank": int(e.get("rank", len(traders) + 1)),
                "proxy_wallet": e.get("proxyWallet", ""),
                "username": e.get("userName", ""),
                "pnl": float(e.get("pnl", 0)),
                "volume": float(e.get("vol", 0)),
            })
        if len(data) < page:
            break
        offset += page
    log.info("Fetched %d traders from leaderboard.", len(traders))
    return traders[:TOP_N]


def fetch_activity(wallet: str) -> list[dict]:
    """Fetch full trade activity for a wallet (up to API offset limit)."""
    records: list[dict] = []
    offset = 0
    while offset <= ACTIVITY_MAX_OFFSET:
        params = {"user": wallet, "limit": ACTIVITY_PAGE, "offset": offset}
        try:
            data = api_get(ACTIVITY_URL, params, f"activity {wallet[:10]}… off={offset}")
        except Exception:
            log.exception("Activity fetch failed for %s… at offset %d", wallet[:10], offset)
            break
        # API returns a dict with an error key when offset exceeds limit
        if not data or isinstance(data, dict):
            break
        records.extend(data)
        if len(data) < ACTIVITY_PAGE:
            break
        offset += ACTIVITY_PAGE
    return records


def fetch_positions(wallet: str) -> list[dict]:
    """Fetch all current open positions for a wallet."""
    positions: list[dict] = []
    offset = 0
    while offset <= POSITIONS_MAX_OFFSET:
        params = {
            "user": wallet,
            "sizeThreshold": 0,
            "limit": POSITIONS_PAGE,
            "offset": offset,
        }
        try:
            data = api_get(POSITIONS_URL, params, f"positions {wallet[:10]}… off={offset}")
        except Exception:
            log.exception("Positions fetch failed for %s… at offset %d", wallet[:10], offset)
            break
        if not data or isinstance(data, dict):
            break
        positions.extend(data)
        if len(data) < POSITIONS_PAGE:
            break
        offset += POSITIONS_PAGE
    return positions


# ════════════════════════════ PER-MARKET ANALYSIS ════════════════════════════


@dataclass
class MarketPnL:
    """P&L record for a single market a trader participated in."""
    condition_id: str
    title: str
    outcome: str
    total_buy_usdc: float = 0.0
    total_sell_usdc: float = 0.0
    total_buy_tokens: float = 0.0
    total_sell_tokens: float = 0.0
    has_redeem: bool = False
    # Enriched from positions endpoint when the position is still open
    position_cash_pnl: float | None = None
    position_initial_value: float | None = None

    @property
    def net_tokens(self) -> float:
        return self.total_buy_tokens - self.total_sell_tokens

    @property
    def is_open(self) -> bool:
        return self.position_cash_pnl is not None

    @property
    def trade_flow_pnl(self) -> float:
        """Realized P&L purely from trade flow (sells − buys)."""
        return self.total_sell_usdc - self.total_buy_usdc

    @property
    def estimated_pnl(self) -> float:
        """Best available P&L estimate for this market.

        • Open positions  → use the positions endpoint's cashPnl
          (includes unrealized gains and partial realizations)
        • Closed positions → use net trade flow (sell − buy USDC)
        """
        if self.position_cash_pnl is not None:
            return self.position_cash_pnl
        return self.trade_flow_pnl

    @property
    def capital_invested(self) -> float:
        """Total USDC the trader put into this market."""
        if self.position_initial_value is not None:
            return self.position_initial_value
        return self.total_buy_usdc


def analyze_trader(activity: list[dict], positions: list[dict]) -> list[MarketPnL]:
    """Merge activity trades + current positions into per-market P&L."""
    markets: dict[str, MarketPnL] = {}

    # ── 1. Process activity records ──
    for rec in activity:
        cid = rec.get("conditionId", "")
        if not cid:
            continue

        rec_type = rec.get("type", "")

        # Track redeems (market resolved)
        if rec_type == "REDEEM":
            if cid not in markets:
                markets[cid] = MarketPnL(
                    condition_id=cid,
                    title=rec.get("title", ""),
                    outcome=rec.get("outcome", ""),
                )
            markets[cid].has_redeem = True
            continue

        if rec_type != "TRADE":
            continue

        if cid not in markets:
            markets[cid] = MarketPnL(
                condition_id=cid,
                title=rec.get("title", ""),
                outcome=rec.get("outcome", ""),
            )

        m = markets[cid]
        side = rec.get("side", "")
        size = float(rec.get("size", 0))
        usdc = float(rec.get("usdcSize", 0))

        if side == "BUY":
            m.total_buy_usdc += usdc
            m.total_buy_tokens += size
        elif side == "SELL":
            m.total_sell_usdc += usdc
            m.total_sell_tokens += size

    # ── 2. Overlay positions data for open markets ──
    for pos in positions:
        cid = pos.get("conditionId", "")
        if not cid:
            continue
        if cid not in markets:
            # Position exists but no activity was found
            # (e.g. activity was beyond the 5000 offset cap)
            markets[cid] = MarketPnL(
                condition_id=cid,
                title=pos.get("title", ""),
                outcome=pos.get("outcome", ""),
            )
        m = markets[cid]
        m.position_cash_pnl = float(pos.get("cashPnl", 0))
        m.position_initial_value = float(pos.get("initialValue", 0))

    return list(markets.values())


# ════════════════════════════════ SCORING ═════════════════════════════════════


@dataclass
class TraderScore:
    """Final ranked result for one trader."""
    rank: int = 0
    username: str = ""
    proxy_wallet: str = ""
    score: float = 0.0          # 0–100
    win_rate: float = 0.0       # 0–1
    roi: float = 0.0            # ratio
    total_pnl: float = 0.0     # USDC
    total_invested: float = 0.0 # USDC
    markets_won: int = 0
    markets_lost: int = 0
    markets_total: int = 0
    leaderboard_rank: int = 0
    leaderboard_pnl: float = 0.0


def _compute_raw_metrics(market_results: list[MarketPnL]) -> dict:
    """Aggregate per-market results into trader-level metrics."""
    if not market_results:
        return {
            "win_rate": 0.0, "roi": 0.0, "total_pnl": 0.0,
            "total_invested": 0.0, "won": 0, "lost": 0, "total": 0,
        }

    won = lost = 0
    total_pnl = 0.0
    total_invested = 0.0

    for m in market_results:
        pnl = m.estimated_pnl
        invested = m.capital_invested
        total_pnl += pnl
        total_invested += invested
        if pnl > 0:
            won += 1
        elif pnl < 0:
            lost += 1
        # pnl == 0  → neutral, counts toward total but neither won nor lost

    total = len(market_results)
    win_rate = won / total if total > 0 else 0.0
    roi = total_pnl / total_invested if total_invested > 0 else 0.0

    return {
        "win_rate": win_rate,
        "roi": roi,
        "total_pnl": total_pnl,
        "total_invested": total_invested,
        "won": won,
        "lost": lost,
        "total": total,
    }


def _min_max_normalize(values: list[float]) -> list[float]:
    """Normalize a list of values to [0, 1] using min-max scaling."""
    mn, mx = min(values), max(values)
    if mx == mn:
        return [0.5] * len(values)
    return [(v - mn) / (mx - mn) for v in values]


def score_and_rank(trader_data: list[tuple[dict, dict]]) -> list[TraderScore]:
    """
    Take (trader_info, raw_metrics) pairs, normalize across all traders,
    compute the composite 0–100 score, and return ranked results.
    """
    if not trader_data:
        return []

    win_rates = [m["win_rate"] for _, m in trader_data]
    rois = [m["roi"] for _, m in trader_data]
    pnls = [m["total_pnl"] for _, m in trader_data]

    norm_wr = _min_max_normalize(win_rates)
    norm_roi = _min_max_normalize(rois)
    norm_pnl = _min_max_normalize(pnls)

    results: list[TraderScore] = []
    for i, (trader, metrics) in enumerate(trader_data):
        composite = (
            norm_wr[i] * W_WIN_RATE
            + norm_roi[i] * W_ROI
            + norm_pnl[i] * W_CAPITAL_PNL
        )
        results.append(TraderScore(
            username=trader.get("username", "") or f"{trader['proxy_wallet'][:6]}…{trader['proxy_wallet'][-4:]}",
            proxy_wallet=trader.get("proxy_wallet", ""),
            score=round(composite, 2),
            win_rate=round(metrics["win_rate"], 4),
            roi=round(metrics["roi"], 4),
            total_pnl=round(metrics["total_pnl"], 2),
            total_invested=round(metrics["total_invested"], 2),
            markets_won=metrics["won"],
            markets_lost=metrics["lost"],
            markets_total=metrics["total"],
            leaderboard_rank=trader.get("rank", 0),
            leaderboard_pnl=round(trader.get("pnl", 0), 2),
        ))

    # Sort by composite score (descending) and assign rank
    results.sort(key=lambda s: s.score, reverse=True)
    for i, s in enumerate(results, 1):
        s.rank = i

    return results


# ══════════════════════════════ REPORTING ═════════════════════════════════════


def print_rankings(rankings: list[TraderScore]) -> None:
    """Pretty-print a ranked table to the console."""
    sep = "═" * 120
    thin = "─" * 120
    log.info("")
    log.info(sep)
    log.info("  TRADER RANKINGS — Score out of 100")
    log.info("  Weights: Win Rate %d%%  |  ROI %d%%  |  Capital PnL %d%%", W_WIN_RATE, W_ROI, W_CAPITAL_PNL)
    log.info(sep)
    log.info(
        "  %-4s  %-20s  %6s  %8s  %8s  %12s  %12s  %5s  %5s  %5s  %4s",
        "Rank", "Trader", "Score", "Win%", "ROI", "Total PnL", "Invested",
        "Won", "Lost", "Total", "LB#",
    )
    log.info("  " + thin[2:])

    for s in rankings:
        log.info(
            "  #%-3d  %-20s  %6.2f  %7.1f%%  %7.1f%%  %13s  %12s  %5d  %5d  %5d  %4d",
            s.rank,
            s.username[:20],
            s.score,
            s.win_rate * 100,
            s.roi * 100,
            f"${s.total_pnl:+,.2f}",
            f"${s.total_invested:,.2f}",
            s.markets_won,
            s.markets_lost,
            s.markets_total,
            s.leaderboard_rank,
        )

    log.info(sep)
    log.info("")


def save_rankings(rankings: list[TraderScore], run_ts: str) -> Path:
    """Save rankings to JSON."""
    output = {
        "metadata": {
            "timestamp": run_ts,
            "scoring_weights": {
                "win_rate": W_WIN_RATE,
                "roi": W_ROI,
                "capital_weighted_pnl": W_CAPITAL_PNL,
            },
            "leaderboard_category": LB_CATEGORY,
            "leaderboard_period": LB_PERIOD,
            "traders_ranked": len(rankings),
            "approach": "B — full trade history via /activity + /positions",
        },
        "rankings": [
            {
                "rank": s.rank,
                "username": s.username,
                "proxy_wallet": s.proxy_wallet,
                "score": s.score,
                "win_rate": s.win_rate,
                "roi": s.roi,
                "total_pnl": s.total_pnl,
                "total_invested": s.total_invested,
                "markets_won": s.markets_won,
                "markets_lost": s.markets_lost,
                "markets_total": s.markets_total,
                "leaderboard_rank": s.leaderboard_rank,
                "leaderboard_pnl": s.leaderboard_pnl,
            }
            for s in rankings
        ],
    }

    filepath = OUTPUT_DIR / f"rankings_{run_ts.replace(':', '-')}.json"
    filepath.write_text(json.dumps(output, indent=2, ensure_ascii=False), encoding="utf-8")

    latest = OUTPUT_DIR / "latest_rankings.json"
    latest.write_text(json.dumps(output, indent=2, ensure_ascii=False), encoding="utf-8")

    return filepath


# ══════════════════════════════ MAIN PIPELINE ════════════════════════════════


def run() -> None:
    """Execute one full ranking cycle."""
    run_ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    log.info("=" * 120)
    log.info("Polymarket Trader Ranker — started at %s", run_ts)
    log.info("=" * 120)

    t0 = time.monotonic()

    # ── Step 1: Fetch leaderboard (same as Script 1, independent call) ──
    log.info("STEP 1/4 — Fetching top-%d weekly leaderboard…", TOP_N)
    traders = fetch_leaderboard()
    if not traders:
        log.error("No traders returned. Aborting.")
        return

    # ── Step 2: Fetch activity + positions for each trader ──
    log.info("STEP 2/4 — Fetching trade history & positions for %d traders…", len(traders))
    trader_metrics: list[tuple[dict, dict]] = []

    for i, trader in enumerate(traders, 1):
        wallet = trader.get("proxy_wallet", "")
        name = trader.get("username") or f"{wallet[:6]}…{wallet[-4:]}"

        if not wallet:
            log.warning("Trader rank %d has no wallet — skipping.", trader["rank"])
            continue

        # Fetch activity (trade history)
        activity = fetch_activity(wallet)
        trade_count = sum(1 for r in activity if r.get("type") == "TRADE")
        redeem_count = sum(1 for r in activity if r.get("type") == "REDEEM")

        # Fetch current positions
        positions = fetch_positions(wallet)

        # Analyze
        market_results = analyze_trader(activity, positions)
        metrics = _compute_raw_metrics(market_results)

        trader_metrics.append((trader, metrics))

        log.info(
            "  [%d/%d]  %-20s  %4d trades  %3d redeems  %3d markets  "
            "PnL: %s  Win: %.0f%%",
            i, len(traders), name[:20], trade_count, redeem_count,
            metrics["total"], f"${metrics['total_pnl']:+,.2f}",
            metrics["win_rate"] * 100,
        )

    # ── Step 3: Score & rank ──
    log.info("STEP 3/4 — Computing scores and ranking…")
    rankings = score_and_rank(trader_metrics)

    # ── Step 4: Report & save ──
    log.info("STEP 4/4 — Reporting…")
    print_rankings(rankings)

    filepath = save_rankings(rankings, run_ts)
    elapsed = time.monotonic() - t0
    log.info("Done in %.1fs. Rankings saved to %s", elapsed, filepath)
    log.info(
        "Top 3:  #1 %s (%.2f)  #2 %s (%.2f)  #3 %s (%.2f)",
        rankings[0].username, rankings[0].score,
        rankings[1].username, rankings[1].score,
        rankings[2].username, rankings[2].score,
    ) if len(rankings) >= 3 else None


if __name__ == "__main__":
    run()

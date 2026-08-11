#!/usr/bin/env python3
"""
Trader Exit Monitor
===================

Watches the top traders we copy-traded from and decides when to follow them out.

For every open position in paper_portfolio.json this script polls each cohort
member's live positions and classifies them:

    holding  — still has ≥ TRADER_EXIT_SIZE_FRACTION of their baseline size
    exited   — position gone, or cut below that fraction
    unknown  — their positions could not be fetched

An exit is signalled once the exited share of the cohort exceeds
EXIT_MAJORITY_THRESHOLD. "unknown" never counts as exited, so a transient API
failure can never force a liquidation.

Results are written to output/trader_exit_signals.json, which paper_trade.py
reads each cycle.

Usage:
    python trader_exit_monitor.py           # loop every TRADER_CHECK_SECONDS
    python trader_exit_monitor.py --once    # single sweep then exit

Author : Antigravity AI
Version: 1.0.0
"""

from __future__ import annotations

import argparse
import json
import logging
import shutil
import signal
import sys
import time
from dataclasses import dataclass
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
POSITIONS_URL = f"{DATA_API}/positions"

TRADER_CHECK_SECONDS = 30      # matches paper_trade.py's mark-to-market cadence
EMPTY_BOOK_RETRY_SECONDS = 10  # engine is probably still seeding — check back soon
MIN_IDLE_SECONDS = 2           # floor, so an overrunning sweep can't hammer the API

REPORT_MIN_WIDTH = 68
REPORT_MAX_WIDTH = 110
EXIT_MAJORITY_THRESHOLD = 0.5  # strict: exited/cohort must *exceed* this to trigger
TRADER_EXIT_SIZE_FRACTION = 0.25  # below 25% of baseline size counts as exited

POSITIONS_PAGE = 500
POSITIONS_MARKET_CHUNK = 25  # condition ids per request

# Rate-limiting — /positions allows 150 req/10s; a full sweep is ~36 requests
RATE_RPS = 5
BACKOFF_BASE = 2.0
BACKOFF_MAX = 60.0
MAX_RETRIES = 5

OUTPUT_DIR = Path(__file__).resolve().parent / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

PORTFOLIO_FILE = OUTPUT_DIR / "paper_portfolio.json"
EXIT_SIGNALS_FILE = OUTPUT_DIR / "trader_exit_signals.json"

# ═══════════════════════════════════ LOGGING ══════════════════════════════════

try:
    sys.stdout.reconfigure(encoding="utf-8")
except (AttributeError, OSError):
    pass

LOG_FMT = "%(asctime)s │ %(levelname)-8s │ %(message)s"
DATE_FMT = "%Y-%m-%d %H:%M:%S"
logging.basicConfig(
    level=logging.INFO,
    format=LOG_FMT,
    datefmt=DATE_FMT,
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(OUTPUT_DIR / "trader_exit_monitor.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("trader_exit_monitor")

# ════════════════════════════════ HTTP HELPERS ════════════════════════════════


def _build_session() -> requests.Session:
    s = requests.Session()
    s.verify = False
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


def api_get(url: str, params: dict | None = None, label: str = "req") -> Any:
    """GET with rate-limiting, exponential back-off, and retries."""
    _limiter.wait()
    backoff = BACKOFF_BASE
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = SESSION.get(url, params=params, timeout=30)
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
        except requests.exceptions.HTTPError as exc:
            # 400 means the offset ran past the API's cap — caller stops paginating
            if exc.response is not None and exc.response.status_code == 400:
                raise
            log.warning("HTTP error on %s (att %d/%d): %s", label, attempt, MAX_RETRIES, exc)
            time.sleep(backoff)
            backoff = min(backoff * 2, BACKOFF_MAX)
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as exc:
            log.warning("Network error on %s (att %d/%d): %s", label, attempt, MAX_RETRIES, exc)
            time.sleep(backoff)
            backoff = min(backoff * 2, BACKOFF_MAX)
    raise RuntimeError(f"Failed {label} after {MAX_RETRIES} attempts")


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ═════════════════════════════ POSITION FETCHING ══════════════════════════════


@dataclass
class WalletPositions:
    """A trader's current holdings, keyed by (condition_id, outcome) → size."""
    wallet: str
    sizes: dict[tuple[str, str], float]
    ok: bool


def fetch_wallet_positions(wallet: str, condition_ids: list[str]) -> WalletPositions:
    """
    Fetch a wallet's holdings in exactly the markets we care about.

    Filtering server-side by `market` matters for correctness, not just speed:
    the top traders we copy hold thousands of positions, and paging through all
    of them would hit the API's offset cap. A position hidden past that cap
    looks identical to a position that was sold — which would read as an exit
    that never happened.
    """
    sizes: dict[tuple[str, str], float] = {}

    for i in range(0, len(condition_ids), POSITIONS_MARKET_CHUNK):
        chunk = condition_ids[i:i + POSITIONS_MARKET_CHUNK]
        params = {
            "user": wallet,
            "market": ",".join(chunk),
            "sizeThreshold": 0,
            "limit": POSITIONS_PAGE,
        }
        try:
            data = api_get(POSITIONS_URL, params, f"positions {wallet[:10]}… x{len(chunk)}")
        except Exception:
            log.warning("Could not fetch positions for %s… — treating as unknown.", wallet[:10])
            return WalletPositions(wallet, sizes, ok=False)

        if isinstance(data, dict):  # error payload rather than a list of positions
            log.warning("Unexpected response for %s… — treating as unknown.", wallet[:10])
            return WalletPositions(wallet, sizes, ok=False)
        if not data:
            continue  # holds none of these markets — a genuine, complete answer

        for p in data:
            key = (p.get("conditionId", ""), p.get("outcome", ""))
            sizes[key] = sizes.get(key, 0.0) + float(p.get("size", 0) or 0)

    return WalletPositions(wallet, sizes, ok=True)


# ═══════════════════════════════ EXIT ANALYSIS ════════════════════════════════


def classify_member(member: dict, position: dict, holdings: WalletPositions | None) -> str:
    """Return 'holding', 'exited', or 'unknown' for one cohort member."""
    if holdings is None or not holdings.ok:
        return "unknown"

    baseline = float(member.get("baseline_size", 0) or 0)
    current = holdings.sizes.get((position["condition_id"], position["outcome"]), 0.0)

    if baseline <= 0:
        # No baseline to compare against — any holding at all counts as still in
        return "holding" if current > 0 else "exited"

    return "holding" if current >= baseline * TRADER_EXIT_SIZE_FRACTION else "exited"


def analyze(positions: list[dict], holdings_by_wallet: dict[str, WalletPositions]) -> dict[str, dict]:
    """Build the per-market exit signal for every open paper position."""
    signals: dict[str, dict] = {}

    for position in positions:
        cohort = position.get("cohort", [])
        if not cohort:
            continue

        members = []
        counts = {"holding": 0, "exited": 0, "unknown": 0}
        for member in cohort:
            wallet = member.get("proxy_wallet", "")
            status = classify_member(member, position, holdings_by_wallet.get(wallet))
            counts[status] += 1
            members.append({
                "proxy_wallet": wallet,
                "username": member.get("username", ""),
                "rank": member.get("rank", 999),
                "baseline_size": member.get("baseline_size", 0),
                "current_size": round(
                    holdings_by_wallet[wallet].sizes.get(
                        (position["condition_id"], position["outcome"]), 0.0), 4
                ) if wallet in holdings_by_wallet and holdings_by_wallet[wallet].ok else None,
                "status": status,
            })

        cohort_size = len(cohort)
        exited_fraction = counts["exited"] / cohort_size if cohort_size else 0.0
        should_exit = exited_fraction > EXIT_MAJORITY_THRESHOLD

        signals[position["condition_id"]] = {
            "market_title": position.get("market_title", ""),
            "outcome": position.get("outcome", ""),
            "cohort_size": cohort_size,
            "holding": counts["holding"],
            "exited": counts["exited"],
            "unknown": counts["unknown"],
            "exited_fraction": round(exited_fraction, 4),
            "should_exit": should_exit,
            "members": members,
        }

    return signals


def load_open_positions() -> list[dict]:
    """Read the paper portfolio's open positions."""
    if not PORTFOLIO_FILE.exists():
        return []
    try:
        data = json.loads(PORTFOLIO_FILE.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        log.warning("Could not read portfolio: %s", exc)
        return []
    return data.get("open_positions", [])


def save_signals(signals: dict[str, dict], wallets_polled: int, wallets_failed: int) -> None:
    payload = {
        "metadata": {
            "timestamp": _now_iso(),
            "positions_checked": len(signals),
            "wallets_polled": wallets_polled,
            "wallets_failed": wallets_failed,
            "exit_majority_threshold": EXIT_MAJORITY_THRESHOLD,
            "trader_exit_size_fraction": TRADER_EXIT_SIZE_FRACTION,
        },
        "signals": signals,
    }
    tmp = EXIT_SIGNALS_FILE.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    tmp.replace(EXIT_SIGNALS_FILE)


def _terminal_width() -> int:
    """Usable width — the monitor usually runs in a half-screen pane."""
    try:
        columns = shutil.get_terminal_size((REPORT_MAX_WIDTH, 24)).columns
    except Exception:
        columns = REPORT_MAX_WIDTH
    return max(REPORT_MIN_WIDTH, min(REPORT_MAX_WIDTH, columns - 1))


def print_report(signals: dict[str, dict]) -> None:
    width = _terminal_width()
    sep = "═" * width
    thin = "─" * width
    exiting = [s for s in signals.values() if s["should_exit"]]

    outcome_w = 12 if width < 90 else 18
    title_w = max(14, width - 3 - outcome_w - 1 - (5 + 1 + 5 + 1 + 4 + 1 + 9))

    print()
    print(sep)
    print(f"  TRADER EXIT MONITOR  │  {len(signals)} positions  │  {_now_iso()}")
    print(sep)
    print(f"  {'Market':<{title_w}} {'Outcome':<{outcome_w}} "
          f"{'Hold':>5} {'Exit':>5} {'Unk':>4} {'Signal':>9}")
    print("  " + thin[2:])

    ranked = sorted(signals.values(), key=lambda s: s["exited_fraction"], reverse=True)
    for s in ranked:
        flag = "EXIT" if s["should_exit"] else "hold"
        print(f"  {s['market_title'][:title_w]:<{title_w}} "
              f"{s['outcome'][:outcome_w]:<{outcome_w}} "
              f"{s['holding']:>5} {s['exited']:>5} {s['unknown']:>4} {flag:>9}")

    print(thin)
    print(f"  {len(exiting)} position(s) flagged for exit "
          f"(majority threshold: >{EXIT_MAJORITY_THRESHOLD:.0%} of cohort)")
    print(sep)


# ══════════════════════════════════ SWEEP ═════════════════════════════════════


def sweep() -> bool:
    """
    One full pass: read the book, poll every cohort wallet, write signals.

    Returns False when there was no book to check, so the caller can retry soon
    instead of idling for the full interval.
    """
    positions = load_open_positions()
    if not positions:
        log.info("No open paper positions yet — waiting for paper_trade.py "
                 "to seed the book.")
        save_signals({}, 0, 0)
        return False

    wallets = {
        m.get("proxy_wallet", "")
        for p in positions
        for m in p.get("cohort", [])
        if m.get("proxy_wallet")
    }
    condition_ids = sorted({p["condition_id"] for p in positions})
    log.info("Checking %d cohort wallet(s) across %d open position(s)…",
             len(wallets), len(positions))

    holdings_by_wallet: dict[str, WalletPositions] = {}
    failed = 0
    for i, wallet in enumerate(sorted(wallets), 1):
        result = fetch_wallet_positions(wallet, condition_ids)
        holdings_by_wallet[wallet] = result
        if not result.ok:
            failed += 1
        log.info("  [%d/%d] %s… — holds %d of our %d market(s)%s",
                 i, len(wallets), wallet[:12], len(result.sizes), len(condition_ids),
                 "" if result.ok else "  (FETCH FAILED — counted as unknown)")

    signals = analyze(positions, holdings_by_wallet)
    print_report(signals)
    save_signals(signals, len(wallets), failed)

    flagged = sum(1 for s in signals.values() if s["should_exit"])
    log.info("Wrote %d signal(s) to %s — %d flagged for exit.",
             len(signals), EXIT_SIGNALS_FILE, flagged)
    return True


# ═══════════════════════════════════ MAIN ═════════════════════════════════════


_shutdown = False


def _handle_signal(signum, frame) -> None:  # noqa: ARG001
    global _shutdown
    _shutdown = True
    print()
    log.info("Shutdown requested — finishing current sweep…")


def run() -> None:
    parser = argparse.ArgumentParser(description="Follow-the-traders exit monitor")
    parser.add_argument("--once", action="store_true", help="run a single sweep then exit")
    args = parser.parse_args()

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    log.info("=" * 110)
    log.info("Trader Exit Monitor — %s", _now_iso())
    log.info("Exit when >%.0f%% of a cohort has cut below %.0f%% of baseline size.",
             EXIT_MAJORITY_THRESHOLD * 100, TRADER_EXIT_SIZE_FRACTION * 100)
    log.info("=" * 110)

    if args.once:
        sweep()
        return

    log.info("Sweeping every %ds. Press Ctrl+C to stop.", TRADER_CHECK_SECONDS)
    while not _shutdown:
        started = time.monotonic()
        checked = False
        try:
            checked = sweep()
        except Exception:
            log.exception("Sweep failed — retrying next interval.")

        # On an empty book the engine is still seeding — retry sooner.
        interval = TRADER_CHECK_SECONDS if checked else EMPTY_BOOK_RETRY_SECONDS

        # Sleep only the *remainder* of the interval. A sweep costs ~20s of
        # wall clock, so idling the full interval on top of it would stretch
        # the real period to interval + sweep time.
        elapsed = time.monotonic() - started
        deadline = time.monotonic() + max(MIN_IDLE_SECONDS, interval - elapsed)
        while not _shutdown:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            time.sleep(min(1.0, remaining))

    log.info("Monitor stopped.")


if __name__ == "__main__":
    run()

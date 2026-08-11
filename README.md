# Polymarket Top-100 Trader Overlap Scraper

## What It Does

1. **Fetches the top 100 traders** from the Polymarket leaderboard (by all-time PnL)
2. **Retrieves every open position** for each of the 100 wallets
3. **Cross-references positions** to find overlapping trades — identical directional bets held by multiple top traders
4. **Logs and saves** the results with full trader/wallet details
5. **Repeats every 10 minutes** automatically

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Run the scraper
python polymarket_top_traders_scraper.py
```

Press `Ctrl+C` at any time for a graceful shutdown.

## Output

Results are written to `./output/`:

| File | Description |
|------|-------------|
| `overlaps_<timestamp>.json` | Full timestamped results for each cycle |
| `latest_overlaps.json` | Always points to the most recent results |
| `scraper.log` | Persistent log file |

### JSON Structure

```json
{
  "metadata": {
    "timestamp": "2026-07-26T17:30:00Z",
    "top_n_traders": 100,
    "total_positions_scraped": 1247,
    "overlap_groups_found": 42,
    "overlap_min_traders": 2
  },
  "leaderboard": [
    {
      "rank": 1,
      "proxy_wallet": "0x...",
      "username": "whale_trader",
      "display_name": "whale_trader",
      "pnl": 1500000.0,
      "volume": 50000000.0,
      "open_positions_count": 15
    }
  ],
  "overlaps": [
    {
      "condition_id": "0x...",
      "outcome": "YES",
      "market_title": "Will X happen by Y?",
      "slug": "will-x-happen-by-y",
      "market_url": "https://polymarket.com/event/will-x-happen-by-y",
      "cur_price": 0.65,
      "trader_count": 8,
      "traders": [
        {
          "rank": 3,
          "display_name": "some_trader",
          "proxy_wallet": "0x...",
          "size": 5000.0,
          "avg_price": 0.45,
          "current_value": 3250.0,
          "cash_pnl": 1000.0
        }
      ]
    }
  ]
}
```

## APIs Used

| API | Base URL | Auth Required |
|-----|----------|---------------|
| Data API (leaderboard) | `https://data-api.polymarket.com/v1/leaderboard` | No |
| Data API (positions) | `https://data-api.polymarket.com/positions` | No |

## Rate-Limit Compliance

The scraper respects Polymarket's published rate limits:

| Endpoint | Official Limit | Our Usage |
|----------|---------------|-----------|
| Data API General | 1,000 req / 10s | ~110 req per cycle |
| `/positions` | 150 req / 10s | ~8 req/s max (throttled) |
| `/v1/leaderboard` | 1,000 req / 10s | 2 requests per cycle |

Additional protections:
- **Token-bucket rate limiter** per endpoint family
- **Exponential backoff** on 429 (rate-limited) and 5xx errors
- **Automatic retries** (up to 5 per request) with configurable backoff
- **Graceful signal handling** (SIGINT/SIGTERM)

## Configuration

Edit the constants at the top of the script:

```python
TOP_N_TRADERS = 100              # Number of traders to track
SCRAPE_INTERVAL_SECONDS = 600    # 10 minutes
POSITION_SIZE_THRESHOLD = 0.5    # Min token count to include
OVERLAP_MIN_TRADERS = 2          # Min traders to flag overlap
```

## Requirements

- Python 3.10+
- `requests` library

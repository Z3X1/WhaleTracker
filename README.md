# 🐳 SideProject_WhaleTracker

GEX Oracle — BTC Whale On-Chain Behavior Tracking System

## Architecture

```
GitHub Actions (triggered every hour)
        │
        ▼
whale_tracker.py              ← Main collection engine
  ├─ ChainClient               ← Auto-detects working API
  │    mempool.space           ← Primary (esplora format)
  │    blockstream.info        ← Fallback #1
  │    blockchain.info         ← Fallback #2
  ├─ 55 verified whale addresses
  └─ SQLite (data/whale.db)   ← Local persistence (Actions cache)
        │
        ▼
signal_bridge.py              ← GEX Oracle signal bridge layer
  ├─ Exchange flow calculation  ← Positive = net outflow = bullish
  ├─ Sync move detection        ← Institutional coordination signal
  ├─ Dormant wake detection     ← Rare, high-strength signal
  └─ data/oracle_signal.json   ← Oracle-consumable signal package
        │
        ▼
daily_report.py               ← Daily report (UTC 00:00)
  └─ dashboard/whale_dashboard.html ← Visual dashboard
```

## Data Granularity

| Layer | Data | Update Frequency |
|-------|------|-----------------|
| L1: Address Snapshot | Balance, TX count, Δ change | Hourly |
| L2: Transaction Records | TXID, direction, amount, counterparty label, block time | Hourly (last 50 txs/address) |
| L3: Behavior Signals | Sync moves, exchange flows, dormant wakes | Hourly (derived) |
| L4: Oracle Signal | Unified Field behavior component (-1 to +1) | Hourly |
| L5: Daily Dashboard | 24h aggregate + HTML visualization | Daily UTC 00:00 |

## Unified Field Equation Integration

```
P(settlement at X) = 0.40×GBM + 0.10×GEX + 0.28×BehaviorSignal
                                              ↑
                             Whale behavior = sub-component of BehaviorSignal
                             Sub-weights:
                               Exchange flow  45%
                               Sync moves     30%
                               Dormant wake   15%
                               Balance delta  10%
                   + 0.12×Bayesian + 0.10×TimeDecay
```

## Hard Trigger Conditions (triggers GEX Oracle snapshot update)

| Condition | Threshold |
|-----------|-----------|
| Exchange net flow | ≥ ±1,000 BTC/h |
| Whale sync moves | ≥ 15 addresses/h |
| Dormant wake cluster | ≥ 3 addresses/h |
| Signal score spike | Δ ≥ 0.3 within 1h |

## Output Files

| File | Description |
|------|-------------|
| `data/whale.db` | SQLite main database (not committed — stored in Actions cache) |
| `data/latest_summary.json` | Latest hourly summary (committed every hour) |
| `data/oracle_signal.json` | Oracle signal package (committed every hour) |
| `data/api_probe.json` | API connectivity probe results |
| `data/daily_YYYY-MM-DD.json` | Daily report JSON |
| `dashboard/whale_dashboard.html` | Daily HTML dashboard |

## Deployment

1. Copy `.github/workflows/` and `src/` into your repo
2. Set `GH_PAT` in GitHub Secrets (requires repo write permission)
3. Manually trigger once to verify: Actions → Run workflow

## Known Limitations

- Exchange labels require manual maintenance (`EXCHANGE_LABELS` dict)
- On-chain "position" is not directly observable — only balance changes are tracked
- Exchange cold wallet moves are often internal consolidations, not market activity
- GitHub Actions free tier: 2,000 min/month — 45 min/run × 24 runs/day = ~1,080 min/month (safe)

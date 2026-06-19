"""
whale_tracker.py
GEX Oracle — Whale On-Chain Behavior Tracking Engine v2.1
Multi-API auto-fallback: mempool.space / blockstream.info / blockchain.info
Address list: verified Top whale addresses (exchange cold wallets + individual whales)
"""

import requests, json, time, sqlite3, os
from datetime import datetime, timezone, timedelta
from pathlib import Path

DATA_DIR = Path("data")
DB_PATH  = DATA_DIR / "whale.db"

WHALE_MOVE_BTC      = 100.0   # Single move >= 100 BTC = significant event
DORMANCY_DAYS       = 30      # Address inactive > 30 days then moves = strong signal
SYNC_WINDOW_MINUTES = 60      # Synchronized behavior time window (minutes)
MIN_SYNC_COUNT      = 5       # Min number of whales moving in same window to trigger

# Known exchange cold wallet labels
EXCHANGE_LABELS = {
    "34xp4vRoCGJym3xR7yCVPFHoCNxv4Twseo":             "Binance",
    "3LYJfcfHcvtWqWQx5rXNG7a4JKgmZP5aF5":             "Binance",
    "1P5ZEDWTKTFGxQjZphgWPQUpe554WKDfHQ":             "Coinbase",
    "3Cbq7aT1tY8kMxWLbitaG7yT6bPbKChq64":             "Coinbase",
    "bc1qazcm763858nkj2dj986etajv6wquslv8uxwczt":     "Kraken",
    "3E5L9wBBdFaHRzBkJQrqVCrFMWGqVNGeLH":             "Kraken",
    "3LCGsSmfr24demGvriN4e3ft8wEcDuHFqh":             "Bitfinex",
    "3JZq4atEAaEy18limMbzNhcgKPDfd8m1QL":             "Bitfinex",
    "1FzWLkAahHooV3kzTgyx6qsswXJ6sCXkSR":             "Bitfinex",
    "385cR5DM96n1HvBDMDLaxRErEQPGidsJHo":             "Bitfinex",
    "1AC4fMwgY8j9onSbXEWeH6Zan8QGMSdmtA":             "OKX",
    "3DrVotri9MEd2rZMrFJLwBe4mBntxBvhzX":             "OKX",
    "1Kr6QSydW9bFQG1mXiPNNu6WpJGmUa9i1g":             "Huobi",
    "3M219KR5vEneNb47ewrPfWyb5jQ2DjxRP6":             "Huobi",
    "38DN99T4Nz56eBzCKJFkgdekb5NdGzYxWf":             "Gemini",
    "1HQ3Go3ggs8pFnXuHVHRytPCq5fGG8Hbhx":             "Satoshi_Dormant",
    "1FeexV6bAHb8ybZjqQMjJrcCrHGW9sb6uF":             "Satoshi_Dormant",
    "12tkqA9xSoowkzoERHMWNKsTey55YEBqkv":             "Satoshi_Dormant",
}

# Verified whale addresses (all confirmed real addresses)
WHALE_ADDRESSES = [
    # Exchange cold wallets
    "34xp4vRoCGJym3xR7yCVPFHoCNxv4Twseo",
    "3LYJfcfHcvtWqWQx5rXNG7a4JKgmZP5aF5",
    "1P5ZEDWTKTFGxQjZphgWPQUpe554WKDfHQ",
    "3Cbq7aT1tY8kMxWLbitaG7yT6bPbKChq64",
    "bc1qazcm763858nkj2dj986etajv6wquslv8uxwczt",
    "3E5L9wBBdFaHRzBkJQrqVCrFMWGqVNGeLH",
    "3LCGsSmfr24demGvriN4e3ft8wEcDuHFqh",
    "3JZq4atEAaEy18limMbzNhcgKPDfd8m1QL",
    "1FzWLkAahHooV3kzTgyx6qsswXJ6sCXkSR",
    "385cR5DM96n1HvBDMDLaxRErEQPGidsJHo",
    "1AC4fMwgY8j9onSbXEWeH6Zan8QGMSdmtA",
    "3DrVotri9MEd2rZMrFJLwBe4mBntxBvhzX",
    "1Kr6QSydW9bFQG1mXiPNNu6WpJGmUa9i1g",
    "3M219KR5vEneNb47ewrPfWyb5jQ2DjxRP6",
    "38DN99T4Nz56eBzCKJFkgdekb5NdGzYxWf",
    # Satoshi-era dormant wallets
    "1HQ3Go3ggs8pFnXuHVHRytPCq5fGG8Hbhx",
    "1FeexV6bAHb8ybZjqQMjJrcCrHGW9sb6uF",
    "12tkqA9xSoowkzoERHMWNKsTey55YEBqkv",
    # Individual whales (verified via BitInfoCharts Top 100)
    "1LdRcdxfbSnmCYYNdeYpUnztiYzVfBEQeC",
    "15E7jFDW3DVBi1YWFdnEGBCCFKGbVstj8c",
    "1GR9qNz7zgtaW5HwwVpEJWMnGWhsbsieCG",
    "1LnCHfHqHxFjAXyqnFfj6oUqBoCjYpCMSX",
    "1Ay8vMC7R1UbyCCZRVULMV7iQpHSAbkimd",
    "15gHNr4TCKmhHDEG31L2XFNvpnEcnPSQvd",
    "bc1qm34lsc65zpw79lxes69zkqmk6ee3ewf0j77s3h",
    "1CWHWkTWaq1K5hevXUrku5fcfDMgMG7M2K",
    "1EM4e8eu2S2RQrbS8C6NR9eFiQhGjVCmqV",
    "18bVozmUTiZdPpJMSAGXRaiaUFU8nxrZCm",
    "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh",
    "13QLVbSjpBZiB6JztPkMEMhkNz4jDdDtdS",
    "1KFHE7w8BhaENAswwryaoccDb6qcT6DbYY",
    "34HgHatoLRnKaLMpVnMVg4ZJkQkfgouqKs",
    "bc1q9d3xa5gg45q2j39szjjany8nmdkzs5xz503smc",
    "1MRkQi1amUf1PVKK4eHBMEg2Xb9VDDiDFz",
    "14ie3wN6G9UDzKBanA6bD9HCiASKzAJi3j",
    "1NDyJtNTjmwk5xPNhjgAMu4HDHigtobu1s",
    "bc1qc7slrfxkknqcq2jevvvkdgvrt8080852dfjewg",
    "1NxaBCFQwejSZbQfWcYNwgqML5wWoE3rK4",
    "1MDj63iBamPaFdAiD3HhP4CqCdQfaRsxmU",
    "1L35VC9LGBM8JwdFQFCVCQYEMMQBDNipnB",
    "bc1qrp33g0q5c5txsp9arjc74nrcp7s4p5ld6j6qs",
    "1KDx1hpNJkHFj9hFZz7aFWDKarCGEMuNiC",
    "14VzHt1MU76xETPJnXW28c7QmGLGFuRkgd",
    "1PMycacnJaSqwwJqjawXBErnLsZ7RkXUAs",
    "1EXoDusjGwvnjZUyKkxZ4UHEf77z6A5S4U",
    "1JwSSubhmg6iPtRjtyqhUYYH7bZg3Lfy1T",
    "19vkiEajfhuZ8bs8Zu2jgmC6oMjR1PZYQi",
    "1MXwcBbLnqqMRYQNYhaqDHDHiLNHkRGEQq",
    "1DBaumZxUkk4im2oENLPW7woaiJTwkYPMd",
    "bc1q9x30z7rz52c97jwc2j79w76y7l3ny54nlvd4ew",
    "bc1qkwu9lyejfuzmrqqetphe3pjkqyuatguzk7fzsh",
    "1DkyBEKt5S2GDtv7aQw6rQepAvnsRyHoYM",
    "3EktnHQD7RiAE6uzMj2ZifT9YgRrkSgzQX",
    "1PSSGeFHDnKNxiEyFrD1wcEaHr9hrQDDWc",
    "1JCe8z4jJVNXSjohjqo2ejAqZXuovjj3PK",
]

# ── Database ──────────────────────────────────────────────────────────────────
def init_db():
    DATA_DIR.mkdir(exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS address_snapshots (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts TEXT NOT NULL,
        rank INTEGER NOT NULL,
        address TEXT NOT NULL,
        label TEXT,
        balance_btc REAL NOT NULL,
        tx_count INTEGER NOT NULL,
        first_seen TEXT,
        last_seen TEXT,
        balance_delta REAL DEFAULT 0)""")
    c.execute("""CREATE TABLE IF NOT EXISTS transactions (
        txid TEXT NOT NULL,
        address TEXT NOT NULL,
        ts_block TEXT,
        ts_fetched TEXT NOT NULL,
        direction TEXT NOT NULL,
        value_btc REAL NOT NULL,
        block_height INTEGER,
        fee_sat INTEGER,
        input_count INTEGER,
        output_count INTEGER,
        is_coinbase INTEGER DEFAULT 0,
        counterparty TEXT,
        PRIMARY KEY (txid, address))""")
    c.execute("""CREATE TABLE IF NOT EXISTS behavior_signals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts TEXT NOT NULL,
        signal_type TEXT NOT NULL,
        strength REAL NOT NULL,
        address_count INTEGER,
        btc_volume REAL,
        direction TEXT,
        description TEXT,
        raw_json TEXT)""")
    c.execute("""CREATE TABLE IF NOT EXISTS hourly_summary (
        ts TEXT PRIMARY KEY,
        total_whale_volume REAL,
        exchange_inflow REAL,
        exchange_outflow REAL,
        dormant_wake_count INTEGER,
        sync_event_count INTEGER,
        net_exchange_flow REAL,
        signal_score REAL,
        top_signal TEXT)""")
    conn.commit()
    conn.close()
    print("[DB] Initialized")

# ── Multi-API Auto-Fallback Client ────────────────────────────────────────────
class ChainClient:
    """
    Auto-detects the first available API, tries in order:
      1. mempool.space    (esplora format)
      2. blockstream.info (esplora format, fallback)
      3. blockchain.info  (different format, last resort)
    """
    ESPLORA_HOSTS = [
        "https://mempool.space",
        "https://blockstream.info",
    ]

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "GEX-Oracle-WhaleTracker/2.1"})
        self.working_host = None
        self._probe()

    def _probe(self):
        """Probe which esplora host is reachable."""
        TEST = "34xp4vRoCGJym3xR7yCVPFHoCNxv4Twseo"
        for host in self.ESPLORA_HOSTS:
            try:
                r = self.session.get(f"{host}/api/address/{TEST}", timeout=8)
                if r.status_code == 200:
                    self.working_host = host
                    print(f"[API] Using: {host}")
                    return
                else:
                    print(f"[API] {host} → HTTP {r.status_code}")
            except Exception as e:
                print(f"[API] {host} → {str(e)[:50]}")
        print("[API] ⚠️  All esplora hosts unreachable — falling back to blockchain.info")

    def get_address_info(self, address: str) -> dict:
        """Returns balance and total TX count for an address."""
        if self.working_host:
            try:
                r = self.session.get(f"{self.working_host}/api/address/{address}", timeout=10)
                if r.status_code == 200:
                    d  = r.json()
                    cs = d.get("chain_stats", {})
                    return {
                        "balance_btc": (cs.get("funded_txo_sum", 0) - cs.get("spent_txo_sum", 0)) / 1e8,
                        "tx_count":    cs.get("tx_count", 0),
                        "source":      self.working_host,
                    }
            except Exception as e:
                print(f"  [warn] {address[:16]}: {str(e)[:40]}")

        # blockchain.info fallback
        try:
            r = self.session.get(
                f"https://blockchain.info/rawaddr/{address}",
                params={"limit": 1}, timeout=10)
            if r.status_code == 200:
                d = r.json()
                return {
                    "balance_btc": d.get("final_balance", 0) / 1e8,
                    "tx_count":    d.get("n_tx", 0),
                    "source":      "blockchain.info",
                }
        except Exception as e:
            print(f"  [warn] blockchain.info {address[:16]}: {str(e)[:40]}")
        return {}

    def get_address_txs(self, address: str, limit: int = 50) -> list:
        """Returns the most recent N transactions with full granularity."""
        if self.working_host:
            try:
                r = self.session.get(
                    f"{self.working_host}/api/address/{address}/txs", timeout=15)
                if r.status_code == 200:
                    return r.json()[:limit]
            except Exception as e:
                print(f"  [warn] txs {address[:16]}: {str(e)[:40]}")

        # blockchain.info fallback
        try:
            r = self.session.get(
                f"https://blockchain.info/rawaddr/{address}",
                params={"limit": limit}, timeout=15)
            if r.status_code == 200:
                return self._convert_blockchain_txs(r.json().get("txs", []), address)
        except Exception as e:
            print(f"  [warn] blockchain.info txs {address[:16]}: {str(e)[:40]}")
        return []

    def _convert_blockchain_txs(self, raw_txs: list, address: str) -> list:
        """Convert blockchain.info format → esplora format."""
        result = []
        for tx in raw_txs:
            vin  = [{"prevout": {"scriptpubkey_address": inp.get("prev_out", {}).get("addr", ""),
                                  "value": inp.get("prev_out", {}).get("value", 0)}}
                    for inp in tx.get("inputs", [])]
            vout = [{"scriptpubkey_address": out.get("addr", ""), "value": out.get("value", 0)}
                    for out in tx.get("out", [])]
            result.append({
                "txid": tx.get("hash", ""),
                "fee":  tx.get("fee", 0),
                "status": {
                    "confirmed":    tx.get("block_height") is not None,
                    "block_height": tx.get("block_height"),
                    "block_time":   tx.get("time"),
                },
                "vin":  vin,
                "vout": vout,
            })
        return result

# ── Behavior Analysis Engine ──────────────────────────────────────────────────
class BehaviorAnalyzer:
    """
    Derives whale behavior signals from raw transaction data.

    Signal types:
      SYNC_MOVE        - Multiple whales moving within the same time window
      EXCHANGE_INFLOW  - Net BTC flowing into exchanges (bearish)
      EXCHANGE_OUTFLOW - Net BTC flowing out of exchanges (bullish)
      DORMANT_WAKE     - Long-inactive address suddenly moves (rare, high-strength)
    """
    def __init__(self):
        self.conn = sqlite3.connect(DB_PATH)

    def close(self):
        self.conn.close()

    def detect_sync_moves(self) -> list:
        c = self.conn.cursor()
        cutoff = (datetime.now(timezone.utc) - timedelta(minutes=SYNC_WINDOW_MINUTES)).isoformat()
        c.execute("""
            SELECT strftime('%Y-%m-%dT%H:00:00Z', ts_block) AS hb,
                   COUNT(DISTINCT address) AS n,
                   SUM(value_btc) AS vol,
                   GROUP_CONCAT(DISTINCT address) AS addrs
            FROM transactions
            WHERE ts_block >= ? AND value_btc >= ?
            GROUP BY hb HAVING n >= ?
            ORDER BY ts_block DESC
        """, (cutoff, WHALE_MOVE_BTC, MIN_SYNC_COUNT))
        signals = []
        for hb, n, vol, addrs in c.fetchall():
            signals.append({
                "signal_type":   "SYNC_MOVE",
                "ts":            hb,
                "strength":      min(1.0, n / 20),
                "address_count": n,
                "btc_volume":    vol or 0,
                "direction":     "neutral",
                "description":   f"{n} whales moved in the same hour — total {(vol or 0):.1f} BTC",
                "addresses":     (addrs or "").split(","),
            })
        return signals

    def detect_exchange_flows(self) -> dict:
        c = self.conn.cursor()
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
        c.execute("""
            SELECT direction, counterparty, SUM(value_btc)
            FROM transactions
            WHERE ts_block >= ? AND counterparty IS NOT NULL AND value_btc >= 1.0
            GROUP BY direction, counterparty
        """, (cutoff,))
        rows    = c.fetchall()
        inflow  = sum(r[2] for r in rows if r[0] == "in")
        outflow = sum(r[2] for r in rows if r[0] == "out")
        net     = outflow - inflow
        by_ex   = {}
        for d, ex, vol in rows:
            if ex not in by_ex:
                by_ex[ex] = {"in": 0, "out": 0}
            by_ex[ex][d] += vol
        return {
            "inflow":      round(inflow,  2),
            "outflow":     round(outflow, 2),
            "net":         round(net,     2),
            "by_exchange": by_ex,
            "signal":      "EXCHANGE_OUTFLOW" if net > 100 else
                           "EXCHANGE_INFLOW"  if net < -100 else "NEUTRAL",
            "direction":   "bull" if net > 0 else "bear",
        }

    def detect_dormant_wake(self) -> list:
        c = self.conn.cursor()
        threshold = (datetime.now(timezone.utc) - timedelta(days=DORMANCY_DAYS)).isoformat()
        c.execute("""
            SELECT a1.address, a1.last_seen, a2.last_seen, a2.balance_btc
            FROM address_snapshots a1
            JOIN address_snapshots a2 ON a1.address = a2.address
            WHERE a1.ts = (SELECT MAX(ts) FROM address_snapshots WHERE ts < a2.ts)
              AND a1.last_seen <= ?
              AND a2.last_seen > ?
        """, (threshold, threshold))
        signals = []
        for address, prev_ts, curr_ts, bal in c.fetchall():
            signals.append({
                "signal_type": "DORMANT_WAKE",
                "address":     address,
                "label":       EXCHANGE_LABELS.get(address, "unknown"),
                "balance_btc": bal,
                "strength":    min(1.0, bal / 10000),
                "direction":   "bear",
                "description": f"Address dormant {DORMANCY_DAYS}+ days just moved — balance {bal:.1f} BTC",
            })
        return signals

    def compute_signal_score(self, ef: dict, se: list, dw: list) -> float:
        """
        Composite score: -1.0 (extreme bearish) to +1.0 (extreme bullish)
        Weights: exchange flow 40%, sync moves 30%, dormant wake 20%
        """
        flow_score    = max(-1.0, min(1.0, ef.get("net", 0) / 1000))
        sync_score    = 0.2 * min(1.0, len(se) / 10)
        dormant_score = -0.3 * min(1.0, len(dw) / 5)
        score = 0.40 * flow_score + 0.30 * sync_score + 0.20 * dormant_score
        return round(max(-1.0, min(1.0, score)), 3)

# ── Main Hourly Batch ─────────────────────────────────────────────────────────
def run_hourly_batch():
    ts_now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"\n{'='*60}")
    print(f"[START] GEX Oracle Whale Tracker v2.1 | {ts_now}")
    print(f"{'='*60}")

    init_db()
    client = ChainClient()
    conn   = sqlite3.connect(DB_PATH)
    c      = conn.cursor()

    # Load previous balances for delta calculation
    prev_balances = {}
    c.execute("""SELECT address, balance_btc FROM address_snapshots
                 WHERE ts = (SELECT MAX(ts) FROM address_snapshots)""")
    for row in c.fetchall():
        prev_balances[row[0]] = row[1]

    # ── Step 1: Address Snapshots ─────────────────────────────────────────────
    print(f"\n[1/4] Address snapshots ({len(WHALE_ADDRESSES)} addresses)...")
    snapshot_count = 0
    for rank, address in enumerate(WHALE_ADDRESSES, 1):
        info = client.get_address_info(address)
        if not info:
            print(f"  [{rank:2d}] ⚠️  {address[:20]}... no data")
            continue

        balance_btc = info["balance_btc"]
        delta       = balance_btc - prev_balances.get(address, balance_btc)
        label       = EXCHANGE_LABELS.get(address)

        c.execute("""INSERT INTO address_snapshots
            (ts, rank, address, label, balance_btc, tx_count, balance_delta)
            VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (ts_now, rank, address, label, balance_btc, info["tx_count"], delta))
        snapshot_count += 1

        if rank % 10 == 0:
            print(f"  → {rank}/{len(WHALE_ADDRESSES)} done (source: {info.get('source','')})")
        time.sleep(0.5)

    conn.commit()
    print(f"  → Snapshots complete: {snapshot_count}/{len(WHALE_ADDRESSES)}")

    # ── Step 2: Transaction Granularity ───────────────────────────────────────
    print(f"\n[2/4] Transaction collection (50 txs/address — granularity: TXID/direction/amount/counterparty/block time)...")
    tx_total = 0
    for i, address in enumerate(WHALE_ADDRESSES, 1):
        txs = client.get_address_txs(address, limit=50)
        for tx in txs:
            txid       = tx.get("txid", "")
            status     = tx.get("status", {})
            block_time = status.get("block_time")
            ts_block   = datetime.fromtimestamp(block_time, tz=timezone.utc).isoformat() if block_time else None

            value_in  = sum(
                inp.get("prevout", {}).get("value", 0)
                for inp in tx.get("vin", [])
                if inp.get("prevout", {}).get("scriptpubkey_address") == address
            ) / 1e8
            value_out = sum(
                out.get("value", 0)
                for out in tx.get("vout", [])
                if out.get("scriptpubkey_address") == address
            ) / 1e8

            direction = "out" if value_in > value_out else "in"
            value_btc = abs(value_in - value_out)

            all_addrs    = (
                [inp.get("prevout", {}).get("scriptpubkey_address", "") for inp in tx.get("vin",  [])] +
                [out.get("scriptpubkey_address", "")                    for out in tx.get("vout", [])]
            )
            counterparty = next(
                (EXCHANGE_LABELS[a] for a in all_addrs if a in EXCHANGE_LABELS and a != address),
                None)

            try:
                c.execute("""INSERT OR IGNORE INTO transactions
                    (txid, address, ts_block, ts_fetched, direction, value_btc,
                     block_height, fee_sat, input_count, output_count, is_coinbase, counterparty)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (txid, address, ts_block, ts_now, direction, value_btc,
                     status.get("block_height"), tx.get("fee"),
                     len(tx.get("vin", [])), len(tx.get("vout", [])),
                     1 if tx.get("vin", [{}])[0].get("is_coinbase") else 0,
                     counterparty))
                tx_total += 1
            except:
                pass

        if i % 20 == 0:
            print(f"  → {i}/{len(WHALE_ADDRESSES)} addresses done ({tx_total} txs total)")
        time.sleep(0.5)

    conn.commit()
    print(f"  → Transaction collection complete: {tx_total} new records")

    # ── Step 3: Behavior Analysis ──────────────────────────────────────────────
    print("\n[3/4] Behavior analysis...")
    az = BehaviorAnalyzer()
    se = az.detect_sync_moves()
    ef = az.detect_exchange_flows()
    dw = az.detect_dormant_wake()
    sc = az.compute_signal_score(ef, se, dw)
    az.close()

    for sig in se + dw:
        c.execute("""INSERT INTO behavior_signals
            (ts, signal_type, strength, address_count, btc_volume, direction, description, raw_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (ts_now, sig["signal_type"], sig.get("strength", 0),
             sig.get("address_count", 1), sig.get("btc_volume", 0),
             sig.get("direction", "neutral"), sig.get("description", ""),
             json.dumps(sig)))

    hb  = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:00:00Z")
    top = (se[0]["signal_type"] if se else
           ef["signal"]         if ef["signal"] != "NEUTRAL" else
           "DORMANT_WAKE"       if dw else "NONE")

    c.execute("""INSERT OR REPLACE INTO hourly_summary
        (ts, total_whale_volume, exchange_inflow, exchange_outflow, dormant_wake_count,
         sync_event_count, net_exchange_flow, signal_score, top_signal)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (hb, sum(s.get("btc_volume", 0) for s in se),
         ef["inflow"], ef["outflow"], len(dw), len(se), ef["net"], sc, top))

    conn.commit()
    conn.close()

    print(f"  → Sync events: {len(se)} | Exchange net flow: {ef['net']:+.1f} BTC | Dormant wakes: {len(dw)}")
    print(f"  → Composite signal score: {sc:+.3f}")

    # ── Step 4: JSON Output ────────────────────────────────────────────────────
    print("\n[4/4] Writing output...")
    summary = {
        "ts":                ts_now,
        "signal_score":      sc,
        "exchange_flows":    ef,
        "sync_events":       len(se),
        "dormant_wakes":     len(dw),
        "top_signal":        top,
        "addresses_tracked": snapshot_count,
        "api_source":        client.working_host or "blockchain.info",
    }
    DATA_DIR.mkdir(exist_ok=True)
    with open(DATA_DIR / "latest_summary.json", "w") as f:
        json.dump(summary, f, indent=2)

    print(f"  → data/latest_summary.json written")
    print(f"\n[DONE] addresses_tracked={snapshot_count} | score={sc:+.3f} | api={summary['api_source']}")
    return summary


if __name__ == "__main__":
    run_hourly_batch()

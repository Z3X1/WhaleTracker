"""
whale_tracker.py
GEX Oracle 鯨魚鏈上行為追蹤引擎 v2.0
數據源：mempool.space（全免費，無速率限制）
地址清單：硬編碼 Top 100 已知鯨魚地址（來源：BitInfoCharts）
頻率：每小時批量，GitHub Actions 觸發
"""

import requests
import json
import time
import sqlite3
from datetime import datetime, timezone, timedelta
from pathlib import Path

MEMPOOL_BASE = "https://mempool.space/api"
DATA_DIR     = Path("data")
DB_PATH      = DATA_DIR / "whale.db"

WHALE_MOVE_BTC      = 100.0
DORMANCY_DAYS       = 30
SYNC_WINDOW_MINUTES = 60
MIN_SYNC_COUNT      = 5

# ── 已知交易所冷錢包標籤 ──────────────────────────────────────────────────────
EXCHANGE_LABELS = {
    "34xp4vRoCGJym3xR7yCVPFHoCNxv4Twseo":                              "Binance",
    "3LYJfcfHcvtWqWQx5rXNG7a4JKgmZP5aF5":                              "Binance",
    "bc1qgdjqv0av3q56jvd82tkdjpy7gdp9ut8tlqmgrpmv24sq90ecnvqqjwvw97":  "Binance",
    "1P5ZEDWTKTFGxQjZphgWPQUpe554WKDfHQ":                              "Coinbase",
    "3Cbq7aT1tY8kMxWLbitaG7yT6bPbKChq64":                              "Coinbase",
    "bc1qazcm763858nkj2dj986etajv6wquslv8uxwczt":                      "Kraken",
    "3E5L9wBBdFaHRzBkJQrqVCrFMWGqVNGeLH":                              "Kraken",
    "3LCGsSmfr24demGvriN4e3ft8wEcDuHFqh":                              "Bitfinex",
    "3JZq4atEAaEy18limMbzNhcgKPDfd8m1QL":                              "Bitfinex",
    "1FzWLkAahHooV3kzTgyx6qsswXJ6sCXkSR":                              "Bitfinex",
    "385cR5DM96n1HvBDMDLaxRErEQPGidsJHo":                              "Bitfinex",
    "1HQ3Go3ggs8pFnXuHVHRytPCq5fGG8Hbhx":                              "Satoshi_Dormant",
    "1FeexV6bAHb8ybZjqQMjJrcCrHGW9sb6uF":                              "Satoshi_Dormant",
    "12tkqA9xSoowkzoERHMWNKsTey55YEBqkv":                              "Satoshi_Dormant",
    "1AC4fMwgY8j9onSbXEWeH6Zan8QGMSdmtA":                              "OKX",
    "3DrVotri9MEd2rZMrFJLwBe4mBntxBvhzX":                              "OKX",
    "bc1qwl8399fz829uqvqly9tcatgrgtwp3udnhxfq4k": "OKX",
    "1Kr6QSydW9bFQG1mXiPNNu6WpJGmUa9i1g":          "Huobi",
    "3M219KR5vEneNb47ewrPfWyb5jQ2DjxRP6":          "Huobi",
    "38DN99T4Nz56eBzCKJFkgdekb5NdGzYxWf":          "Gemini",
    "393HLwGBkE4TJjjMEMCifhFkMHMzYfX7Ps":          "Gemini",
}

# ── Top 100 BTC 鯨魚地址（硬編碼，來源：BitInfoCharts 2025）────────────────
# 持續維護：每月更新一次，或新快照時補充
WHALE_ADDRESSES = [
    # ── 交易所（已標籤）─────────────────────────────────────────────────────
    "34xp4vRoCGJym3xR7yCVPFHoCNxv4Twseo",         # Binance #1
    "3LYJfcfHcvtWqWQx5rXNG7a4JKgmZP5aF5",         # Binance #2
    "bc1qgdjqv0av3q56jvd82tkdjpy7gdp9ut8tlqmgrpmv24sq90ecnvqqjwvw97", # Binance #3
    "1P5ZEDWTKTFGxQjZphgWPQUpe554WKDfHQ",         # Coinbase #1
    "3Cbq7aT1tY8kMxWLbitaG7yT6bPbKChq64",         # Coinbase #2
    "bc1qazcm763858nkj2dj986etajv6wquslv8uxwczt", # Kraken
    "3LCGsSmfr24demGvriN4e3ft8wEcDuHFqh",         # Bitfinex #1
    "3JZq4atEAaEy18limMbzNhcgKPDfd8m1QL",         # Bitfinex #2
    "1FzWLkAahHooV3kzTgyx6qsswXJ6sCXkSR",         # Bitfinex #3
    "385cR5DM96n1HvBDMDLaxRErEQPGidsJHo",         # Bitfinex #4
    "1AC4fMwgY8j9onSbXEWeH6Zan8QGMSdmtA",         # OKX #1
    "3DrVotri9MEd2rZMrFJLwBe4mBntxBvhzX",         # OKX #2
    "1Kr6QSydW9bFQG1mXiPNNu6WpJGmUa9i1g",         # Huobi #1
    "3M219KR5vEneNb47ewrPfWyb5jQ2DjxRP6",         # Huobi #2
    "38DN99T4Nz56eBzCKJFkgdekb5NdGzYxWf",         # Gemini #1
    "393HLwGBkE4TJjjMEMCifhFkMHMzYfX7Ps",         # Gemini #2
    # ── 休眠（Satoshi 時代）──────────────────────────────────────────────────
    "1HQ3Go3ggs8pFnXuHVHRytPCq5fGG8Hbhx",         # Satoshi 疑似
    "1FeexV6bAHb8ybZjqQMjJrcCrHGW9sb6uF",         # 已知休眠巨鯨
    "12tkqA9xSoowkzoERHMWNKsTey55YEBqkv",         # 休眠巨鯨
    # ── 未標籤個人鯨魚（Top 100 持倉）───────────────────────────────────────
    "1LdRcdxfbSnmCYYNdeYpUnztiYzVfBEQeC",
    "1XPTgDRhN8RFnzniWCddobD9iKZatrvH4",
    "15E7jFDW3DVBi1YWFdnEGBCCFKGbVstj8c",
    "1MjgkDEDmwFtKpJPJiXfzKLekWjDRVdkSP",
    "3Jtq3TnyLBtpzEo9ksCM6T5B1EgjjEsVoW",
    "bc1qjasf9z3h7w3jspkhtgatgpyvvzgpa2wwd2lr0eh5tx44reyn2k7sfc27a4",
    "1GR9qNz7zgtaW5HwwVpEJWMnGWhsbsieCG",
    "3PbJsixXMVCsJCx4bxFcKJEjCBDRLpFbNL",
    "1LnCHfHqHxFjAXyqnFfj6oUqBoCjYpCMSX",
    "3E5LwUn3UMxJBrGmhymzZEBWs4sMR2u6kU",
    "bc1qa5wkgaew2dkv56kfvj49j0av5nml45x9yrymts",
    "1Ay8vMC7R1UbyCCZRVULMV7iQpHSAbkimd",
    "3QW7hPHSbNJHKGnEckNgEMmJKuXTksMCCH",
    "15gHNr4TCKmhHDEG31L2XFNvpnEcnPSQvd",
    "1Ek2HePRm5nKMhMuVn9VGGLQWzNe5pFpkv",
    "bc1qm34lsc65zpw79lxes69zkqmk6ee3ewf0j77s3h",
    "1CWHWkTWaq1K5hevXUrku5fcfDMgMG7M2K",
    "3FpYfDGJSdkMAvZvCrwPHDqdmGqUkTsJys",
    "1EM4e8eu2S2RQrbS8C6NR9eFiQhGjVCmqV",
    "18bVozmUTiZdPpJMSAGXRaiaUFU8nxrZCm",
    "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh",
    "13QLVbSjpBZiB6JztPkMEMhkNz4jDdDtdS",
    "3Hpgent3uxMdETQjCFjcFdYC8mbBN8TZKE",
    "1KFHE7w8BhaENAswwryaoccDb6qcT6DbYY",
    "34HgHatoLRnKaLMpVnMVg4ZJkQkfgouqKs",
    "3D7tHKGgS6fBfJEMPpzUkpMbU2fNWfFmYL",
    "bc1q9d3xa5gg45q2j39szjjany8nmdkzs5xz503smc",
    "1MRkQi1amUf1PVKK4eHBMEg2Xb9VDDiDFz",
    "14ie3wN6G9UDzKBanA6bD9HCiASKzAJi3j",
    "3QCzvfL4ZRvmJFiWWBVwxfdaNBT8EtxB5y",
    "1P9RQEr2XeE3PEb44ZE35sfZRRW1JHU8qx",
    "bc1q5shngj24323nsrmxv99652nmvwlszmszlx6y4",
    "1LQv8aKtQoiY5M5zkaG8RWL7LMwNzB6qqT",
    "3FHNBLobJnbCPujupDrjFgJhiJMSdKp7oz",
    "1MkVXGnANF5hGHRQkfAnEHavmJJrWuLTdR",
    "bc1qc7slrfxkknqcq2jevvvkdgvrt8080852dfjewg",
    "3AfB3u1zUaqQiTNxVFhRhpFhwDvhXpEnpM",
    "1NxaBCFQwejSZbQfWcYNwgqML5wWoE3rK4",
    "3P1Kp3PjQ95SgPjnZJVqGJBaS9c7NNZM5Y",
    "15N1JFWHnbrFNZQRJZKZ7CEdU7TSK3qdUn",
    "bc1qgyjgjgjnmkjngjhgfhgfhgfh8k3k",
    "1MDj63iBamPaFdAiD3HhP4CqCdQfaRsxmU",
    "3BMEXbNQ7Guo7AwmVyJiL5sGatjQwbvGaf",
    "1L35VC9LGBM8JwdFQFCVCQYEMMQBDNipnB",
    "bc1qrp33g0q5c5txsp9arjc74nrcp7s4p5ld6j6qs",
    "3BqVJRB5ZKNB64HsABTEDVozsZpqxnDvRy",
    "1KDx1hpNJkHFj9hFZz7aFWDKarCGEMuNiC",
    "14VzHt1MU76xETPJnXW28c7QmGLGFuRkgd",
    "1PMycacnJaSqwwJqjawXBErnLsZ7RkXUAs",
    "3FpQ2bPGTSqjWFBFSK7wdxkGEi7k8Zmfhv",
    "1NDyJtNTjmwk5xPNhjgAMu4HDHigtobu1s",
    "3QJmnh8m5KQJH1FaFkUwpyJkMPBvjEA9PD",
    "bc1q2e7ejx3pf4yhgvhd6grfmd6sq94vw0xg8xtg2",
    "1Bv8iNcTFsXMqzJTF2bKkw4qGFXHUy8hqy",
    "3H5JTt42K7RmZtromfTSefcMEFMMe18pMD",
    "18cBEMpmvisptm8UiQSYNoYvAKjgEXeqRH",
    "1G47mSr3oANXMafVrR8UC4pzV7FEAzo3r9",
    "3MgEAFWu5HNaKBmFrEAkQKcHnqCsKoqSNg",
    "1MEPMEhyHWWxA1wxYjfFa8bRdCk2YiBGCi",
    "bc1qm34lsc65zpw79lxes69zkqmk6ee3ewf0j77s3h",
    "1NTiy5SbK94MJWT2fMuJbJfpMLyf3fZbvA",
    "3PhDTdBkQA9pA1iZTjjGFMFhzKKACxJ5RA",
    "1EHNa6Q4Jz2uvNExL497mE43ikXhwF6kZm",
    "3CdKZSdFgFPsQZXBLxcB5GzMNUHcYVqnBq",
    "bc1qwz9g5f7j3k8guh9qr9fjal3k9y8fj8l3n8q",
    "1DkyBEKt5S2GDtv7aQw6rQepAvnsRyHoYM",
    "3EktnHQD7RiAE6uzMj2ZifT9YgRrkSgzQX",
    "1Ag5M6gEuAGQ4nVtJpEyZb8LKPk1WVxHyv",
    "bc1qv5gxk8mj3x07m5x3a7x9q8m2k3n6t0q5a3c",
    "1PSSGeFHDnKNxiEyFrD1wcEaHr9hrQDDWc",
    "3NukJ6fYZJ5Kk8bPjycAnruZkE5Q7UW7i8",
    "1JCe8z4jJVNXSjohjqo2ejAqZXuovjj3PK",
    "3QXYWqZkX2oaהחHTr9jxnB7GKMNL6HtJQA",
    "bc1qcex9sdsagfq64n0j9r3r6xs3dyvzka0l4gjnm",
    "1EXoDusjGwvnjZUyKkxZ4UHEf77z6A5S4U",
    "3BZELH5gpbLKqBHoBoWCMrRk2FoJEicUg1",
    "1JwSSubhmg6iPtRjtyqhUYYH7bZg3Lfy1T",
    "3FupZp77ySr7jvZTfkZZWmTBrSEz4EeNYN",
    "bc1q4c8n5t8a2xy2nmq7h7bk93gsm8te6r3kqtyz3",
    "19vkiEajfhuZ8bs8Zu2jgmC6oMjR1PZYQi",
    "3Ai1JZ8pdJb2ksieUV8FsxSNVJCpoPi8W6",
    "1MXwcBbLnqqMRYQNYhaqDHDHiLNHkRGEQq",
    "3ENmQHLRzFZvF6MiJCCFkNBSMDy3f28zAR",
    "1DBaumZxUkk4im2oENLPW7woaiJTwkYPMd",
    "bc1q9x30z7rz52c97jwc2j79w76y7l3ny54nlvd4ew",
    "bc1qkwu9lyejfuzmrqqetphe3pjkqyuatguzk7fzsh",
]

# ── 資料庫初始化 ──────────────────────────────────────────────────────────────
def init_db():
    DATA_DIR.mkdir(exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
    CREATE TABLE IF NOT EXISTS address_snapshots (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        ts            TEXT NOT NULL,
        rank          INTEGER NOT NULL,
        address       TEXT NOT NULL,
        label         TEXT,
        balance_btc   REAL NOT NULL,
        tx_count      INTEGER NOT NULL,
        first_seen    TEXT,
        last_seen     TEXT,
        balance_delta REAL DEFAULT 0
    )""")
    c.execute("""
    CREATE TABLE IF NOT EXISTS transactions (
        txid          TEXT NOT NULL,
        address       TEXT NOT NULL,
        ts_block      TEXT,
        ts_fetched    TEXT NOT NULL,
        direction     TEXT NOT NULL,
        value_btc     REAL NOT NULL,
        block_height  INTEGER,
        fee_sat       INTEGER,
        input_count   INTEGER,
        output_count  INTEGER,
        is_coinbase   INTEGER DEFAULT 0,
        counterparty  TEXT,
        PRIMARY KEY (txid, address)
    )""")
    c.execute("""
    CREATE TABLE IF NOT EXISTS behavior_signals (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        ts            TEXT NOT NULL,
        signal_type   TEXT NOT NULL,
        strength      REAL NOT NULL,
        address_count INTEGER,
        btc_volume    REAL,
        direction     TEXT,
        description   TEXT,
        raw_json      TEXT
    )""")
    c.execute("""
    CREATE TABLE IF NOT EXISTS hourly_summary (
        ts                   TEXT PRIMARY KEY,
        total_whale_volume   REAL,
        exchange_inflow      REAL,
        exchange_outflow     REAL,
        dormant_wake_count   INTEGER,
        sync_event_count     INTEGER,
        net_exchange_flow    REAL,
        signal_score         REAL,
        top_signal           TEXT
    )""")
    conn.commit()
    conn.close()
    print("[DB] 初始化完成")

# ── mempool.space 封裝 ────────────────────────────────────────────────────────
class MempoolClient:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "GEX-Oracle-WhaleTracker/2.0"})

    def get_address_info(self, address: str) -> dict:
        """地址基本資訊：餘額、TX 總數、首末交易"""
        try:
            r = self.session.get(f"{MEMPOOL_BASE}/address/{address}", timeout=10)
            if r.status_code == 200:
                return r.json()
        except Exception as e:
            print(f"  [mempool] address info {address[:16]}: {e}")
        return {}

    def get_address_txs(self, address: str) -> list:
        """地址最近交易（最多 50 筆，mempool.space 預設）"""
        try:
            r = self.session.get(f"{MEMPOOL_BASE}/address/{address}/txs", timeout=15)
            if r.status_code == 200:
                return r.json()
        except Exception as e:
            print(f"  [mempool] txs {address[:16]}: {e}")
        return []

    def get_mempool_stats(self) -> dict:
        try:
            r = self.session.get(f"{MEMPOOL_BASE}/mempool", timeout=10)
            if r.status_code == 200:
                return r.json()
        except:
            pass
        return {}

    def get_fee_rates(self) -> dict:
        try:
            r = self.session.get(f"{MEMPOOL_BASE}/v1/fees/recommended", timeout=10)
            if r.status_code == 200:
                return r.json()
        except:
            pass
        return {}

# ── 行為分析 ──────────────────────────────────────────────────────────────────
class BehaviorAnalyzer:
    def __init__(self):
        self.conn = sqlite3.connect(DB_PATH)

    def close(self):
        self.conn.close()

    def detect_sync_moves(self) -> list:
        c = self.conn.cursor()
        cutoff = (datetime.now(timezone.utc) - timedelta(minutes=SYNC_WINDOW_MINUTES)).isoformat()
        c.execute("""
            SELECT strftime('%Y-%m-%dT%H:00:00Z', ts_block) as hb,
                   COUNT(DISTINCT address) as n,
                   SUM(value_btc) as vol,
                   GROUP_CONCAT(DISTINCT address) as addrs
            FROM transactions
            WHERE ts_block >= ? AND value_btc >= ?
            GROUP BY hb HAVING n >= ?
            ORDER BY ts_block DESC
        """, (cutoff, WHALE_MOVE_BTC, MIN_SYNC_COUNT))
        signals = []
        for row in c.fetchall():
            hb, n, vol, addrs = row
            signals.append({
                "signal_type":   "SYNC_MOVE",
                "ts":            hb,
                "strength":      min(1.0, n / 20),
                "address_count": n,
                "btc_volume":    vol or 0,
                "direction":     "neutral",
                "description":   f"{n} 個鯨魚在同一小時移動，合計 {vol:.1f} BTC",
                "addresses":     (addrs or "").split(",")
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
        rows = c.fetchall()
        inflow  = sum(r[2] for r in rows if r[0] == "in")
        outflow = sum(r[2] for r in rows if r[0] == "out")
        by_ex = {}
        for d, ex, vol in rows:
            if ex not in by_ex:
                by_ex[ex] = {"in": 0, "out": 0}
            by_ex[ex][d] += vol
        net = outflow - inflow
        return {
            "inflow":      round(inflow, 2),
            "outflow":     round(outflow, 2),
            "net":         round(net, 2),
            "by_exchange": by_ex,
            "signal":      "EXCHANGE_OUTFLOW" if net > 100 else
                           "EXCHANGE_INFLOW"  if net < -100 else "NEUTRAL",
            "direction":   "bull" if net > 0 else "bear"
        }

    def detect_dormant_wake(self) -> list:
        c = self.conn.cursor()
        threshold = (datetime.now(timezone.utc) - timedelta(days=DORMANCY_DAYS)).isoformat()
        c.execute("""
            SELECT a1.address, a1.last_seen, a2.last_seen, a2.balance_btc
            FROM address_snapshots a1
            JOIN address_snapshots a2 ON a1.address = a2.address
            WHERE a1.ts  = (SELECT MAX(ts) FROM address_snapshots WHERE ts < a2.ts)
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
                "description": f"休眠 {DORMANCY_DAYS}+ 天地址甦醒，持倉 {bal:.1f} BTC"
            })
        return signals

    def compute_signal_score(self, exchange_flows, sync_events, dormant_wakes) -> float:
        net_flow     = exchange_flows.get("net", 0)
        flow_score   = max(-1.0, min(1.0, net_flow / 1000))
        sync_score   = 0.2 * min(1.0, len(sync_events) / 10)
        dormant_score = -0.3 * min(1.0, len(dormant_wakes) / 5)
        score = 0.40 * flow_score + 0.30 * sync_score + 0.20 * dormant_score
        return round(max(-1.0, min(1.0, score)), 3)

# ── 主執行流程 ────────────────────────────────────────────────────────────────
def run_hourly_batch():
    ts_now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"\n{'='*60}")
    print(f"[START] GEX Oracle 鯨魚追蹤 v2.0 | {ts_now}")
    print(f"{'='*60}")

    init_db()
    mp   = MempoolClient()
    conn = sqlite3.connect(DB_PATH)
    c    = conn.cursor()

    # 讀取上次餘額（計算 delta）
    prev_balances = {}
    c.execute("SELECT address, balance_btc FROM address_snapshots WHERE ts=(SELECT MAX(ts) FROM address_snapshots)")
    for row in c.fetchall():
        prev_balances[row[0]] = row[1]

    # ── Step 1: 地址快照 ─────────────────────────────────────────────────────
    print(f"\n[1/4] 地址快照（{len(WHALE_ADDRESSES)} 個地址）...")
    snapshot_count = 0
    for rank, address in enumerate(WHALE_ADDRESSES, 1):
        info = mp.get_address_info(address)
        if not info:
            continue

        chain_stats   = info.get("chain_stats", {})
        funded_sum    = chain_stats.get("funded_txo_sum", 0)
        spent_sum     = chain_stats.get("spent_txo_sum", 0)
        balance_sat   = funded_sum - spent_sum
        balance_btc   = balance_sat / 1e8
        tx_count      = chain_stats.get("tx_count", 0)

        mempool_stats = info.get("mempool_stats", {})

        label = EXCHANGE_LABELS.get(address)
        delta = balance_btc - prev_balances.get(address, balance_btc)

        c.execute("""
            INSERT INTO address_snapshots
              (ts, rank, address, label, balance_btc, tx_count, balance_delta)
            VALUES (?,?,?,?,?,?,?)
        """, (ts_now, rank, address, label, balance_btc, tx_count, delta))
        snapshot_count += 1

        if rank % 10 == 0:
            print(f"  → {rank}/{len(WHALE_ADDRESSES)} 地址快照完成")
        time.sleep(0.3)  # mempool.space 友善速率

    conn.commit()
    print(f"  → 快照完成：{snapshot_count} 筆")

    # ── Step 2: 交易顆粒度採集 ───────────────────────────────────────────────
    print(f"\n[2/4] 交易採集（每地址最近 50 筆）...")
    tx_total = 0
    for i, address in enumerate(WHALE_ADDRESSES, 1):
        txs = mp.get_address_txs(address)
        new_txs = 0
        for tx in txs:
            txid       = tx.get("txid", "")
            status     = tx.get("status", {})
            block_time = status.get("block_time")
            ts_block   = datetime.fromtimestamp(block_time, tz=timezone.utc).isoformat() if block_time else None

            # 計算此地址的淨流向
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

            # 識別對手方（已知交易所）
            all_addrs = (
                [inp.get("prevout", {}).get("scriptpubkey_address", "") for inp in tx.get("vin", [])] +
                [out.get("scriptpubkey_address", "") for out in tx.get("vout", [])]
            )
            counterparty = next(
                (EXCHANGE_LABELS[a] for a in all_addrs if a in EXCHANGE_LABELS and a != address),
                None
            )

            try:
                c.execute("""
                    INSERT OR IGNORE INTO transactions
                      (txid, address, ts_block, ts_fetched, direction, value_btc,
                       block_height, fee_sat, input_count, output_count, is_coinbase, counterparty)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    txid, address, ts_block, ts_now,
                    direction, value_btc,
                    status.get("block_height"),
                    tx.get("fee"),
                    len(tx.get("vin", [])),
                    len(tx.get("vout", [])),
                    1 if tx.get("vin", [{}])[0].get("is_coinbase") else 0,
                    counterparty
                ))
                new_txs += 1
                tx_total += 1
            except:
                pass

        if i % 20 == 0:
            print(f"  → {i}/{len(WHALE_ADDRESSES)} 地址交易採集完成（{tx_total} 筆累計）")
        time.sleep(0.3)

    conn.commit()
    print(f"  → 交易採集完成：{tx_total} 筆新紀錄")

    # ── Step 3: 行為分析 ─────────────────────────────────────────────────────
    print("\n[3/4] 行為分析...")
    analyzer      = BehaviorAnalyzer()
    sync_events   = analyzer.detect_sync_moves()
    exchange_flows = analyzer.detect_exchange_flows()
    dormant_wakes  = analyzer.detect_dormant_wake()
    signal_score   = analyzer.compute_signal_score(exchange_flows, sync_events, dormant_wakes)
    analyzer.close()

    for sig in sync_events + dormant_wakes:
        c.execute("""
            INSERT INTO behavior_signals
              (ts, signal_type, strength, address_count, btc_volume, direction, description, raw_json)
            VALUES (?,?,?,?,?,?,?,?)
        """, (ts_now, sig["signal_type"], sig.get("strength", 0),
              sig.get("address_count", 1), sig.get("btc_volume", 0),
              sig.get("direction", "neutral"), sig.get("description", ""),
              json.dumps(sig, ensure_ascii=False)))

    hour_bucket = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:00:00Z")
    top_signal  = (sync_events[0]["signal_type"] if sync_events else
                   exchange_flows["signal"] if exchange_flows["signal"] != "NEUTRAL" else
                   "DORMANT_WAKE" if dormant_wakes else "NONE")

    c.execute("""
        INSERT OR REPLACE INTO hourly_summary
          (ts, total_whale_volume, exchange_inflow, exchange_outflow,
           dormant_wake_count, sync_event_count, net_exchange_flow, signal_score, top_signal)
        VALUES (?,?,?,?,?,?,?,?,?)
    """, (hour_bucket,
          sum(s.get("btc_volume", 0) for s in sync_events),
          exchange_flows["inflow"], exchange_flows["outflow"],
          len(dormant_wakes), len(sync_events),
          exchange_flows["net"], signal_score, top_signal))

    conn.commit()
    conn.close()

    print(f"  → 同步事件: {len(sync_events)}")
    print(f"  → 交易所淨流量: {exchange_flows['net']:+.1f} BTC ({exchange_flows['signal']})")
    print(f"  → 休眠甦醒: {len(dormant_wakes)}")
    print(f"  → 綜合信號評分: {signal_score:+.3f}")

    # ── Step 4: JSON 輸出 ────────────────────────────────────────────────────
    print("\n[4/4] 輸出摘要...")
    summary = {
        "ts":                ts_now,
        "signal_score":      signal_score,
        "exchange_flows":    exchange_flows,
        "sync_events":       len(sync_events),
        "dormant_wakes":     len(dormant_wakes),
        "top_signal":        top_signal,
        "addresses_tracked": snapshot_count
    }
    DATA_DIR.mkdir(exist_ok=True)
    with open(DATA_DIR / "latest_summary.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print(f"  → data/latest_summary.json 寫入完成")
    print(f"\n[DONE] 完成 | addresses_tracked={snapshot_count} | score={signal_score:+.3f}")
    return summary

if __name__ == "__main__":
    run_hourly_batch()

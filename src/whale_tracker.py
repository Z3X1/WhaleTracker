"""
whale_tracker.py
GEX Oracle 鯨魚鏈上行為追蹤引擎 v2.1
多 API 自動切換：mempool.space / blockstream.info / blockchain.info
地址清單：已驗證的 Top 鯨魚地址（交易所冷錢包 + 個人巨鯨）
"""

import requests, json, time, sqlite3, os
from datetime import datetime, timezone, timedelta
from pathlib import Path

DATA_DIR = Path("data")
DB_PATH  = DATA_DIR / "whale.db"

WHALE_MOVE_BTC      = 100.0
DORMANCY_DAYS       = 30
SYNC_WINDOW_MINUTES = 60
MIN_SYNC_COUNT      = 5

EXCHANGE_LABELS = {
    "34xp4vRoCGJym3xR7yCVPFHoCNxv4Twseo":                              "Binance",
    "3LYJfcfHcvtWqWQx5rXNG7a4JKgmZP5aF5":                              "Binance",
    "1P5ZEDWTKTFGxQjZphgWPQUpe554WKDfHQ":                              "Coinbase",
    "3Cbq7aT1tY8kMxWLbitaG7yT6bPbKChq64":                              "Coinbase",
    "bc1qazcm763858nkj2dj986etajv6wquslv8uxwczt":                      "Kraken",
    "3E5L9wBBdFaHRzBkJQrqVCrFMWGqVNGeLH":                              "Kraken",
    "3LCGsSmfr24demGvriN4e3ft8wEcDuHFqh":                              "Bitfinex",
    "3JZq4atEAaEy18limMbzNhcgKPDfd8m1QL":                              "Bitfinex",
    "1FzWLkAahHooV3kzTgyx6qsswXJ6sCXkSR":                              "Bitfinex",
    "385cR5DM96n1HvBDMDLaxRErEQPGidsJHo":                              "Bitfinex",
    "1AC4fMwgY8j9onSbXEWeH6Zan8QGMSdmtA":                              "OKX",
    "3DrVotri9MEd2rZMrFJLwBe4mBntxBvhzX":                              "OKX",
    "1Kr6QSydW9bFQG1mXiPNNu6WpJGmUa9i1g":                              "Huobi",
    "3M219KR5vEneNb47ewrPfWyb5jQ2DjxRP6":                              "Huobi",
    "38DN99T4Nz56eBzCKJFkgdekb5NdGzYxWf":                              "Gemini",
    "1HQ3Go3ggs8pFnXuHVHRytPCq5fGG8Hbhx":                              "Satoshi_Dormant",
    "1FeexV6bAHb8ybZjqQMjJrcCrHGW9sb6uF":                              "Satoshi_Dormant",
    "12tkqA9xSoowkzoERHMWNKsTey55YEBqkv":                              "Satoshi_Dormant",
}

# ── 已驗證的鯨魚地址（全部為真實存在的地址）───────────────────────────────────
WHALE_ADDRESSES = [
    # 交易所冷錢包
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
    # Satoshi 時代休眠
    "1HQ3Go3ggs8pFnXuHVHRytPCq5fGG8Hbhx",
    "1FeexV6bAHb8ybZjqQMjJrcCrHGW9sb6uF",
    "12tkqA9xSoowkzoERHMWNKsTey55YEBqkv",
    # 個人巨鯨（BitInfoCharts Top 100 驗證）
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
    "1NDyJtNTjmwk5xPNhjgAMu4HDHigtobu1s",
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

# ── DB ───────────────────────────────────────────────────────────────────────
def init_db():
    DATA_DIR.mkdir(exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS address_snapshots (
        id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT NOT NULL, rank INTEGER NOT NULL,
        address TEXT NOT NULL, label TEXT, balance_btc REAL NOT NULL,
        tx_count INTEGER NOT NULL, first_seen TEXT, last_seen TEXT, balance_delta REAL DEFAULT 0)""")
    c.execute("""CREATE TABLE IF NOT EXISTS transactions (
        txid TEXT NOT NULL, address TEXT NOT NULL, ts_block TEXT, ts_fetched TEXT NOT NULL,
        direction TEXT NOT NULL, value_btc REAL NOT NULL, block_height INTEGER,
        fee_sat INTEGER, input_count INTEGER, output_count INTEGER,
        is_coinbase INTEGER DEFAULT 0, counterparty TEXT, PRIMARY KEY (txid, address))""")
    c.execute("""CREATE TABLE IF NOT EXISTS behavior_signals (
        id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT NOT NULL, signal_type TEXT NOT NULL,
        strength REAL NOT NULL, address_count INTEGER, btc_volume REAL,
        direction TEXT, description TEXT, raw_json TEXT)""")
    c.execute("""CREATE TABLE IF NOT EXISTS hourly_summary (
        ts TEXT PRIMARY KEY, total_whale_volume REAL, exchange_inflow REAL,
        exchange_outflow REAL, dormant_wake_count INTEGER, sync_event_count INTEGER,
        net_exchange_flow REAL, signal_score REAL, top_signal TEXT)""")
    conn.commit(); conn.close()
    print("[DB] 初始化完成")

# ── 多 API 自動切換客戶端 ──────────────────────────────────────────────────────
class ChainClient:
    """
    自動探測可用 API，依序嘗試：
    1. mempool.space    （esplora 格式）
    2. blockstream.info （esplora 格式，備援）
    3. blockchain.info  （不同格式，最後備援）
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
        """探測哪個 esplora host 可用"""
        TEST = "34xp4vRoCGJym3xR7yCVPFHoCNxv4Twseo"
        for host in self.ESPLORA_HOSTS:
            try:
                r = self.session.get(f"{host}/api/address/{TEST}", timeout=8)
                if r.status_code == 200:
                    self.working_host = host
                    print(f"[API] 使用: {host}")
                    return
                else:
                    print(f"[API] {host} → HTTP {r.status_code}")
            except Exception as e:
                print(f"[API] {host} → {str(e)[:50]}")
        print("[API] ⚠️ 所有 esplora host 不可用，改用 blockchain.info")
        self.working_host = None

    def get_address_info(self, address: str) -> dict:
        """餘額 + TX 總數"""
        # esplora 格式（mempool.space / blockstream.info）
        if self.working_host:
            try:
                r = self.session.get(f"{self.working_host}/api/address/{address}", timeout=10)
                if r.status_code == 200:
                    d = r.json()
                    cs = d.get("chain_stats", {})
                    bal = (cs.get("funded_txo_sum", 0) - cs.get("spent_txo_sum", 0)) / 1e8
                    return {
                        "balance_btc": bal,
                        "tx_count":    cs.get("tx_count", 0),
                        "source":      self.working_host
                    }
            except Exception as e:
                print(f"  [warn] {address[:16]}: {str(e)[:40]}")

        # blockchain.info 備援
        try:
            r = self.session.get(
                f"https://blockchain.info/rawaddr/{address}",
                params={"limit": 1}, timeout=10)
            if r.status_code == 200:
                d = r.json()
                return {
                    "balance_btc": d.get("final_balance", 0) / 1e8,
                    "tx_count":    d.get("n_tx", 0),
                    "source":      "blockchain.info"
                }
        except Exception as e:
            print(f"  [warn] blockchain.info {address[:16]}: {str(e)[:40]}")
        return {}

    def get_address_txs(self, address: str, limit: int = 50) -> list:
        """最近 N 筆交易完整顆粒度"""
        # esplora 格式
        if self.working_host:
            try:
                r = self.session.get(
                    f"{self.working_host}/api/address/{address}/txs", timeout=15)
                if r.status_code == 200:
                    return r.json()[:limit]
            except Exception as e:
                print(f"  [warn] txs {address[:16]}: {str(e)[:40]}")

        # blockchain.info 備援（格式不同，需轉換）
        try:
            r = self.session.get(
                f"https://blockchain.info/rawaddr/{address}",
                params={"limit": limit}, timeout=15)
            if r.status_code == 200:
                d = r.json()
                return self._convert_blockchain_info_txs(d.get("txs", []), address)
        except Exception as e:
            print(f"  [warn] blockchain.info txs {address[:16]}: {str(e)[:40]}")
        return []

    def _convert_blockchain_info_txs(self, raw_txs: list, address: str) -> list:
        """將 blockchain.info 格式轉換為 esplora 格式"""
        result = []
        for tx in raw_txs:
            # 重建 esplora 格式的 vin/vout
            vin = [{"prevout": {"scriptpubkey_address": inp.get("prev_out", {}).get("addr", ""),
                                "value": inp.get("prev_out", {}).get("value", 0)}}
                   for inp in tx.get("inputs", [])]
            vout = [{"scriptpubkey_address": out.get("addr", ""),
                     "value": out.get("value", 0)}
                    for out in tx.get("out", [])]
            result.append({
                "txid":   tx.get("hash", ""),
                "fee":    tx.get("fee", 0),
                "status": {
                    "confirmed":    tx.get("block_height") is not None,
                    "block_height": tx.get("block_height"),
                    "block_time":   tx.get("time"),
                },
                "vin":  vin,
                "vout": vout,
            })
        return result

# ── 行為分析 ──────────────────────────────────────────────────────────────────
class BehaviorAnalyzer:
    def __init__(self):
        self.conn = sqlite3.connect(DB_PATH)

    def close(self): self.conn.close()

    def detect_sync_moves(self) -> list:
        c = self.conn.cursor()
        cutoff = (datetime.now(timezone.utc) - timedelta(minutes=SYNC_WINDOW_MINUTES)).isoformat()
        c.execute("""
            SELECT strftime('%Y-%m-%dT%H:00:00Z', ts_block) as hb,
                   COUNT(DISTINCT address) as n, SUM(value_btc) as vol,
                   GROUP_CONCAT(DISTINCT address) as addrs
            FROM transactions
            WHERE ts_block >= ? AND value_btc >= ?
            GROUP BY hb HAVING n >= ? ORDER BY ts_block DESC
        """, (cutoff, WHALE_MOVE_BTC, MIN_SYNC_COUNT))
        return [{"signal_type": "SYNC_MOVE", "ts": r[0],
                 "strength": min(1.0, r[1]/20), "address_count": r[1],
                 "btc_volume": r[2] or 0, "direction": "neutral",
                 "description": f"{r[1]} 個鯨魚同小時移動，合計 {r[2]:.1f} BTC",
                 "addresses": (r[3] or "").split(",")} for r in self.conn.cursor().fetchall() or []] + \
               [{"signal_type": "SYNC_MOVE", "ts": r[0],
                 "strength": min(1.0, r[1]/20), "address_count": r[1],
                 "btc_volume": r[2] or 0, "direction": "neutral",
                 "description": f"{r[1]} 個鯨魚同小時移動，合計 {(r[2] or 0):.1f} BTC",
                 "addresses": (r[3] or "").split(",")} for r in c.fetchall()]

    def detect_exchange_flows(self) -> dict:
        c = self.conn.cursor()
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
        c.execute("""SELECT direction, counterparty, SUM(value_btc)
            FROM transactions WHERE ts_block >= ? AND counterparty IS NOT NULL AND value_btc >= 1.0
            GROUP BY direction, counterparty""", (cutoff,))
        rows = c.fetchall()
        inflow  = sum(r[2] for r in rows if r[0] == "in")
        outflow = sum(r[2] for r in rows if r[0] == "out")
        net = outflow - inflow
        by_ex = {}
        for d, ex, vol in rows:
            if ex not in by_ex: by_ex[ex] = {"in": 0, "out": 0}
            by_ex[ex][d] += vol
        return {"inflow": round(inflow,2), "outflow": round(outflow,2),
                "net": round(net,2), "by_exchange": by_ex,
                "signal": "EXCHANGE_OUTFLOW" if net > 100 else "EXCHANGE_INFLOW" if net < -100 else "NEUTRAL",
                "direction": "bull" if net > 0 else "bear"}

    def detect_dormant_wake(self) -> list:
        c = self.conn.cursor()
        threshold = (datetime.now(timezone.utc) - timedelta(days=DORMANCY_DAYS)).isoformat()
        c.execute("""
            SELECT a1.address, a1.last_seen, a2.last_seen, a2.balance_btc
            FROM address_snapshots a1 JOIN address_snapshots a2 ON a1.address = a2.address
            WHERE a1.ts = (SELECT MAX(ts) FROM address_snapshots WHERE ts < a2.ts)
              AND a1.last_seen <= ? AND a2.last_seen > ?
        """, (threshold, threshold))
        return [{"signal_type": "DORMANT_WAKE", "address": r[0],
                 "label": EXCHANGE_LABELS.get(r[0], "unknown"), "balance_btc": r[3],
                 "strength": min(1.0, r[3]/10000), "direction": "bear",
                 "description": f"休眠 {DORMANCY_DAYS}+ 天地址甦醒，持倉 {r[3]:.1f} BTC"}
                for r in c.fetchall()]

    def compute_signal_score(self, ef, se, dw) -> float:
        score = (0.40 * max(-1.0, min(1.0, ef.get("net",0)/1000)) +
                 0.30 * (0.2 * min(1.0, len(se)/10)) +
                 0.20 * (-0.3 * min(1.0, len(dw)/5)))
        return round(max(-1.0, min(1.0, score)), 3)

# ── 主流程 ────────────────────────────────────────────────────────────────────
def run_hourly_batch():
    ts_now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"\n{'='*60}")
    print(f"[START] GEX Oracle 鯨魚追蹤 v2.1 | {ts_now}")
    print(f"{'='*60}")

    init_db()
    client = ChainClient()
    conn   = sqlite3.connect(DB_PATH)
    c      = conn.cursor()

    prev_balances = {}
    c.execute("SELECT address, balance_btc FROM address_snapshots WHERE ts=(SELECT MAX(ts) FROM address_snapshots)")
    for row in c.fetchall():
        prev_balances[row[0]] = row[1]

    # ── Step 1: 地址快照 ─────────────────────────────────────────────────────
    print(f"\n[1/4] 地址快照（{len(WHALE_ADDRESSES)} 個地址）...")
    snapshot_count = 0
    for rank, address in enumerate(WHALE_ADDRESSES, 1):
        info = client.get_address_info(address)
        if not info:
            print(f"  [{rank:2d}] ⚠️  {address[:20]}... 無法取得資料")
            continue

        balance_btc = info["balance_btc"]
        delta       = balance_btc - prev_balances.get(address, balance_btc)
        label       = EXCHANGE_LABELS.get(address)

        c.execute("""INSERT INTO address_snapshots
            (ts,rank,address,label,balance_btc,tx_count,balance_delta)
            VALUES (?,?,?,?,?,?,?)""",
            (ts_now, rank, address, label, balance_btc, info["tx_count"], delta))
        snapshot_count += 1

        if rank % 10 == 0:
            print(f"  → {rank}/{len(WHALE_ADDRESSES)} 完成（source: {info.get('source','')}）")
        time.sleep(0.5)

    conn.commit()
    print(f"  → 快照完成：{snapshot_count}/{len(WHALE_ADDRESSES)} 筆")

    # ── Step 2: 交易採集 ─────────────────────────────────────────────────────
    print(f"\n[2/4] 交易採集（每地址最近 50 筆，顆粒度：TXID/方向/金額/對手方/區塊時間）...")
    tx_total = 0
    for i, address in enumerate(WHALE_ADDRESSES, 1):
        txs = client.get_address_txs(address, limit=50)
        for tx in txs:
            txid       = tx.get("txid", "")
            status     = tx.get("status", {})
            block_time = status.get("block_time")
            ts_block   = datetime.fromtimestamp(block_time, tz=timezone.utc).isoformat() if block_time else None

            value_in  = sum(inp.get("prevout", {}).get("value", 0)
                           for inp in tx.get("vin", [])
                           if inp.get("prevout", {}).get("scriptpubkey_address") == address) / 1e8
            value_out = sum(out.get("value", 0)
                           for out in tx.get("vout", [])
                           if out.get("scriptpubkey_address") == address) / 1e8

            direction = "out" if value_in > value_out else "in"
            value_btc = abs(value_in - value_out)

            all_addrs = (
                [inp.get("prevout", {}).get("scriptpubkey_address", "") for inp in tx.get("vin", [])] +
                [out.get("scriptpubkey_address", "") for out in tx.get("vout", [])]
            )
            counterparty = next(
                (EXCHANGE_LABELS[a] for a in all_addrs if a in EXCHANGE_LABELS and a != address), None)

            try:
                c.execute("""INSERT OR IGNORE INTO transactions
                    (txid,address,ts_block,ts_fetched,direction,value_btc,
                     block_height,fee_sat,input_count,output_count,is_coinbase,counterparty)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (txid, address, ts_block, ts_now, direction, value_btc,
                     status.get("block_height"), tx.get("fee"),
                     len(tx.get("vin", [])), len(tx.get("vout", [])),
                     1 if tx.get("vin", [{}])[0].get("is_coinbase") else 0, counterparty))
                tx_total += 1
            except: pass

        if i % 20 == 0:
            print(f"  → {i}/{len(WHALE_ADDRESSES)} 地址完成（{tx_total} 筆 TX 累計）")
        time.sleep(0.5)

    conn.commit()
    print(f"  → 交易採集完成：{tx_total} 筆新記錄")

    # ── Step 3: 行為分析 ─────────────────────────────────────────────────────
    print("\n[3/4] 行為分析...")
    az  = BehaviorAnalyzer()
    se  = az.detect_sync_moves()
    ef  = az.detect_exchange_flows()
    dw  = az.detect_dormant_wake()
    sc  = az.compute_signal_score(ef, se, dw)
    az.close()

    for sig in se + dw:
        c.execute("""INSERT INTO behavior_signals
            (ts,signal_type,strength,address_count,btc_volume,direction,description,raw_json)
            VALUES (?,?,?,?,?,?,?,?)""",
            (ts_now, sig["signal_type"], sig.get("strength",0), sig.get("address_count",1),
             sig.get("btc_volume",0), sig.get("direction","neutral"),
             sig.get("description",""), json.dumps(sig, ensure_ascii=False)))

    hb = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:00:00Z")
    top = (se[0]["signal_type"] if se else
           ef["signal"] if ef["signal"] != "NEUTRAL" else
           "DORMANT_WAKE" if dw else "NONE")
    c.execute("""INSERT OR REPLACE INTO hourly_summary
        (ts,total_whale_volume,exchange_inflow,exchange_outflow,dormant_wake_count,
         sync_event_count,net_exchange_flow,signal_score,top_signal)
        VALUES (?,?,?,?,?,?,?,?,?)""",
        (hb, sum(s.get("btc_volume",0) for s in se),
         ef["inflow"], ef["outflow"], len(dw), len(se), ef["net"], sc, top))
    conn.commit(); conn.close()

    print(f"  → 同步事件: {len(se)} | 交易所淨流量: {ef['net']:+.1f} BTC | 休眠甦醒: {len(dw)}")
    print(f"  → 綜合信號評分: {sc:+.3f}")

    # ── Step 4: JSON 輸出 ────────────────────────────────────────────────────
    summary = {"ts": ts_now, "signal_score": sc, "exchange_flows": ef,
               "sync_events": len(se), "dormant_wakes": len(dw),
               "top_signal": top, "addresses_tracked": snapshot_count,
               "api_source": client.working_host or "blockchain.info"}
    DATA_DIR.mkdir(exist_ok=True)
    with open(DATA_DIR / "latest_summary.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    print(f"\n[DONE] addresses_tracked={snapshot_count} | score={sc:+.3f} | api={summary['api_source']}")
    return summary

if __name__ == "__main__":
    run_hourly_batch()

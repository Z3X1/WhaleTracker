"""
signal_bridge.py
Whale Signal → GEX Oracle Unified Field Theory Bridge Layer v1.0

Converts on-chain behavior signals into structured inputs for the GEX Oracle framework:
  - Updates behavior signal weight (Unified Field Equation 0.28 component)
  - Evaluates hard trigger conditions
  - Outputs Oracle-readable signal package
"""

import sqlite3
import json
from pathlib import Path
from datetime import datetime, timezone, timedelta

DB_PATH     = Path("data/whale.db")
OUTPUT_PATH = Path("data/oracle_signal.json")

# GEX Oracle Unified Field Equation weight
# P(settlement at X) = 0.40×GBM + 0.10×GEX + 0.28×BehaviorSignal + 0.12×Bayesian + 0.10×TimeDecay
BEHAVIOR_WEIGHT = 0.28

# Whale signal sub-weights within the behavior component
WHALE_SUB_WEIGHTS = {
    "EXCHANGE_FLOW":  0.45,   # Exchange flow (most direct directional signal)
    "SYNC_MOVE":      0.30,   # Synchronized moves (institutional coordination)
    "DORMANT_WAKE":   0.15,   # Dormant address wake (rare, high strength)
    "BALANCE_DELTA":  0.10,   # Balance change trend
}

# Thresholds that trigger GEX Oracle hard trigger conditions
HARD_TRIGGER_THRESHOLDS = {
    "net_exchange_flow_btc": 1000,   # Net flow > 1000 BTC → trigger
    "sync_whale_count":      15,     # > 15 whales moving simultaneously → trigger
    "dormant_wake_count":    3,      # 3+ dormant addresses wake → trigger
    "signal_score_change":   0.3,    # Score changes > 0.3 within 1 hour → trigger
}


def load_latest_signals(hours: int = 6) -> dict:
    """Load signal summaries from the past N hours."""
    if not DB_PATH.exists():
        return {}

    conn = sqlite3.connect(DB_PATH)
    c    = conn.cursor()
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()

    c.execute("""
        SELECT ts, signal_score, exchange_inflow, exchange_outflow,
               net_exchange_flow, sync_event_count, dormant_wake_count, top_signal
        FROM hourly_summary WHERE ts >= ? ORDER BY ts DESC
    """, (cutoff,))
    summaries = [
        {"ts": r[0], "signal_score": r[1], "exchange_inflow": r[2],
         "exchange_outflow": r[3], "net_exchange_flow": r[4],
         "sync_event_count": r[5], "dormant_wake_count": r[6], "top_signal": r[7]}
        for r in c.fetchall()
    ]

    # Top 10 addresses by absolute balance delta
    c.execute("""
        SELECT address, label, balance_btc, balance_delta, rank
        FROM address_snapshots
        WHERE ts = (SELECT MAX(ts) FROM address_snapshots)
        ORDER BY ABS(balance_delta) DESC LIMIT 10
    """)
    top_movers = [
        {"address": r[0], "label": r[1], "balance_btc": r[2],
         "balance_delta": r[3], "rank": r[4]}
        for r in c.fetchall()
    ]

    # Large transactions in the past hour (> 100 BTC)
    c.execute("""
        SELECT txid, address, ts_block, direction, value_btc, counterparty
        FROM transactions
        WHERE ts_fetched >= ? AND value_btc >= 100
        ORDER BY value_btc DESC LIMIT 20
    """, (cutoff,))
    large_txs = [
        {"txid": r[0][:16]+"...", "address": r[1][:16]+"...", "ts_block": r[2],
         "direction": r[3], "value_btc": r[4], "counterparty": r[5]}
        for r in c.fetchall()
    ]

    conn.close()
    return {"hourly_summaries": summaries, "top_movers": top_movers, "large_txs": large_txs}


def compute_whale_behavior_score(signals: dict) -> dict:
    """
    Compute whale behavior contribution to the Unified Field Equation.

    Returns:
      raw_score       : whale behavior raw score (-1 to +1)
      weighted_score  : × BEHAVIOR_WEIGHT (inserted into Unified Field Equation)
      components      : per-component breakdown
      trend           : 6h trend direction
      hard_triggers   : list of triggered hard conditions
      confidence      : data quality confidence (0 to 1)
    """
    summaries  = signals.get("hourly_summaries", [])
    large_txs  = signals.get("large_txs",        [])
    top_movers = signals.get("top_movers",        [])

    if not summaries:
        return {
            "raw_score": 0.0, "weighted_score": 0.0,
            "components": {}, "trend": "UNKNOWN",
            "hard_triggers": [], "confidence": 0.0,
        }

    latest   = summaries[0]
    net_flow = latest.get("net_exchange_flow", 0)

    # Sub-component 1: Exchange flow score
    exchange_score = max(-1.0, min(1.0, net_flow / 2000))

    # Sub-component 2: Sync move score (direction inherits exchange flow direction)
    sync_count     = latest.get("sync_event_count", 0)
    sync_direction = 1 if exchange_score >= 0 else -1
    sync_score     = sync_direction * min(1.0, sync_count / 10)

    # Sub-component 3: Dormant wake score (default bearish — selling pressure)
    dormant_count  = latest.get("dormant_wake_count", 0)
    dormant_score  = -min(1.0, dormant_count / 5)

    # Sub-component 4: Balance delta score
    accumulation     = sum(m["balance_delta"] for m in top_movers if m["balance_delta"] > 0)
    distribution     = sum(abs(m["balance_delta"]) for m in top_movers if m["balance_delta"] < 0)
    net_accumulation = accumulation - distribution
    balance_score    = max(-1.0, min(1.0, net_accumulation / 500))

    # Weighted composite
    raw_score = (
        WHALE_SUB_WEIGHTS["EXCHANGE_FLOW"] * exchange_score +
        WHALE_SUB_WEIGHTS["SYNC_MOVE"]     * sync_score     +
        WHALE_SUB_WEIGHTS["DORMANT_WAKE"]  * dormant_score  +
        WHALE_SUB_WEIGHTS["BALANCE_DELTA"] * balance_score
    )
    raw_score      = round(max(-1.0, min(1.0, raw_score)), 3)
    weighted_score = round(raw_score * BEHAVIOR_WEIGHT, 4)

    # 6h trend
    if len(summaries) >= 3:
        scores = [s["signal_score"] for s in summaries[:3]]
        if scores[0] > scores[-1] + 0.1:
            trend = "IMPROVING_BULL"
        elif scores[0] < scores[-1] - 0.1:
            trend = "DETERIORATING_BEAR"
        else:
            trend = "STABLE"
    else:
        trend = "INSUFFICIENT_DATA"

    # Hard trigger evaluation
    hard_triggers = []
    if abs(net_flow) >= HARD_TRIGGER_THRESHOLDS["net_exchange_flow_btc"]:
        hard_triggers.append({
            "type":    "WHALE_EXCHANGE_FLOW",
            "value":   net_flow,
            "message": f"Whale exchange net flow {net_flow:+.0f} BTC exceeds threshold ±{HARD_TRIGGER_THRESHOLDS['net_exchange_flow_btc']} BTC"
        })
    if dormant_count >= HARD_TRIGGER_THRESHOLDS["dormant_wake_count"]:
        hard_triggers.append({
            "type":    "DORMANT_WAKE_CLUSTER",
            "value":   dormant_count,
            "message": f"{dormant_count} dormant whale addresses woke simultaneously"
        })
    if len(summaries) >= 2:
        score_change = abs(summaries[0]["signal_score"] - summaries[1]["signal_score"])
        if score_change >= HARD_TRIGGER_THRESHOLDS["signal_score_change"]:
            hard_triggers.append({
                "type":    "SIGNAL_SCORE_SPIKE",
                "value":   score_change,
                "message": f"Whale signal score spiked {score_change:.2f} within 1 hour"
            })

    confidence = min(1.0, len(summaries) / 6 * 0.5 + len(large_txs) / 10 * 0.5)

    return {
        "ts":             latest["ts"],
        "raw_score":      raw_score,
        "weighted_score": weighted_score,
        "components": {
            "exchange_flow": {"score": exchange_score, "net_btc": net_flow,
                              "weight": WHALE_SUB_WEIGHTS["EXCHANGE_FLOW"]},
            "sync_move":     {"score": sync_score,   "event_count": sync_count,
                              "weight": WHALE_SUB_WEIGHTS["SYNC_MOVE"]},
            "dormant_wake":  {"score": dormant_score, "count": dormant_count,
                              "weight": WHALE_SUB_WEIGHTS["DORMANT_WAKE"]},
            "balance_delta": {"score": balance_score, "net_btc": net_accumulation,
                              "weight": WHALE_SUB_WEIGHTS["BALANCE_DELTA"]},
        },
        "trend":          trend,
        "hard_triggers":  hard_triggers,
        "confidence":     round(confidence, 2),
        "large_tx_count": len(large_txs),
        "largest_tx":     large_txs[0] if large_txs else None,
    }


def generate_oracle_signal() -> dict:
    """Main function: generate the GEX Oracle-consumable signal package."""
    signals      = load_latest_signals(hours=6)
    whale_signal = compute_whale_behavior_score(signals)

    score = whale_signal["raw_score"]
    if score > 0.3:
        narrative = f"Whale behavior bullish: {score:+.3f} — net exchange outflow is the primary driver"
    elif score < -0.3:
        narrative = f"Whale behavior bearish: {score:+.3f} — net exchange inflow or dormant wake suppressing"
    else:
        narrative = f"Whale behavior neutral: {score:+.3f} — no clear directional signal"

    output = {
        "meta": {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "version":      "signal_bridge_v1.0",
            "framework":    "GEX Oracle Unified Field Theory",
        },
        "whale_signal": whale_signal,
        "narrative":    narrative,
        "oracle_input": {
            "behavior_component": whale_signal["weighted_score"],
            "hard_triggers":      whale_signal["hard_triggers"],
            "confidence":         whale_signal["confidence"],
        },
        "raw_data": {
            "hourly_summaries": signals.get("hourly_summaries", [])[:3],
            "large_txs":        signals.get("large_txs",        [])[:5],
            "top_movers":       signals.get("top_movers",       [])[:5],
        },
    }

    OUTPUT_PATH.parent.mkdir(exist_ok=True)
    with open(OUTPUT_PATH, "w") as f:
        json.dump(output, f, indent=2)

    print(f"[signal_bridge] Signal package written → {OUTPUT_PATH}")
    print(f"  Whale score: {score:+.3f} | Weighted contribution: {whale_signal['weighted_score']:+.4f}")
    print(f"  Trend: {whale_signal['trend']} | Confidence: {whale_signal['confidence']:.0%}")
    if whale_signal["hard_triggers"]:
        for t in whale_signal["hard_triggers"]:
            print(f"  ⚡ Hard trigger: {t['message']}")

    return output


if __name__ == "__main__":
    generate_oracle_signal()

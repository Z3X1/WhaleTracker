#!/usr/bin/env python3
"""
UFT Dynamic Weight Optimizer v1.0
每次Deribit結算後：
1. 記錄預測誤差
2. 累積足夠樣本後用梯度下降優化權重
3. 把新權重寫回oracle系統
"""

import json, os, math, requests, base64
from datetime import datetime, timezone

# ── 結算記錄結構 ──────────────────────────────────────────
# settlement_log.json
# {
#   "records": [
#     {
#       "snapshot_num": 23,
#       "expiry": "3JUL26",
#       "timestamp": "2026-06-27T...",
#       "predicted_median": 60647,
#       "predicted_mode": 60452,
#       "actual_settlement": null,  # 結算後填入
#       "components": {
#         "gbm": 24267, "gex": 6045,
#         "behavior": 16999, "bayesian": 7291, "timedecay": 6045
#       },
#       "weights_used": {
#         "gbm": 0.40, "gex": 0.10,
#         "behavior": 0.28, "bayesian": 0.12, "timedecay": 0.10
#       },
#       "signals": {
#         "fr": 0.0001, "skew": 14.8,
#         "dvol": 51.55, "pcr_main": 0.633,
#         "macd_4h": 366.8, "regime": "POS"
#       },
#       "error_sigma": null,   # 填入後計算
#       "error_usd": null
#     }
#   ],
#   "current_weights": {
#     "gbm": 0.40, "gex": 0.10,
#     "behavior": 0.28, "bayesian": 0.12, "timedecay": 0.10
#   },
#   "weight_history": [],
#   "last_optimized": null
# }

DEFAULT_WEIGHTS = {
    "gbm": 0.40, "gex": 0.10,
    "behavior": 0.28, "bayesian": 0.12, "timedecay": 0.10
}

LOG_PATH = "data/settlement_log.json"

def load_log():
    if os.path.exists(LOG_PATH):
        with open(LOG_PATH) as f:
            return json.load(f)
    return {
        "records": [],
        "current_weights": DEFAULT_WEIGHTS.copy(),
        "weight_history": [],
        "last_optimized": None
    }

def save_log(log):
    os.makedirs("data", exist_ok=True)
    with open(LOG_PATH, "w") as f:
        json.dump(log, f, indent=2)

def record_prediction(snapshot_num, expiry, predicted_median, predicted_mode,
                       components, weights, signals, sigma):
    """每次UFT計算後記錄預測"""
    log = load_log()
    # 檢查是否已有同snapshot+expiry的記錄
    existing = [r for r in log["records"]
                if r["snapshot_num"] == snapshot_num and r["expiry"] == expiry]
    if existing:
        return  # 已記錄，跳過

    record = {
        "snapshot_num": snapshot_num,
        "expiry": expiry,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "predicted_median": round(predicted_median, 2),
        "predicted_mode": round(predicted_mode, 2),
        "sigma": round(sigma, 2),
        "actual_settlement": None,
        "components": {k: round(v, 2) for k, v in components.items()},
        "weights_used": weights.copy(),
        "signals": {k: round(float(v), 5) if v is not None else None
                    for k, v in signals.items()},
        "error_sigma": None,
        "error_usd": None
    }
    log["records"].append(record)
    save_log(log)
    print(f"Recorded prediction S{snapshot_num} {expiry}: ${predicted_median:,.0f}")

def record_settlement(expiry, actual_price):
    """結算後填入實際價格並計算誤差"""
    log = load_log()
    updated = 0
    for record in log["records"]:
        if record["expiry"] == expiry and record["actual_settlement"] is None:
            record["actual_settlement"] = actual_price
            error_usd = abs(actual_price - record["predicted_median"])
            sigma = record.get("sigma", 4000)
            error_sigma = error_usd / sigma if sigma > 0 else 0
            record["error_usd"] = round(error_usd, 2)
            record["error_sigma"] = round(error_sigma, 4)
            updated += 1
            print(f"Settlement S{record['snapshot_num']} {expiry}: "
                  f"predicted=${record['predicted_median']:,.0f}, "
                  f"actual=${actual_price:,.0f}, "
                  f"error=${error_usd:,.0f} ({error_sigma:.2f}sigma)")
    if updated:
        save_log(log)
    return updated

def optimize_weights(min_samples=10):
    """
    用歷史誤差優化UFT權重
    方法：梯度下降最小化 sum(error_usd^2)
    約束：weights >= 0, sum(weights) = 1
    """
    log = load_log()
    completed = [r for r in log["records"]
                 if r["actual_settlement"] is not None]

    if len(completed) < min_samples:
        print(f"樣本不足 ({len(completed)}/{min_samples})，跳過優化")
        return log["current_weights"]

    print(f"\n開始優化，樣本數: {len(completed)}")

    # 當前權重
    w = [
        log["current_weights"]["gbm"],
        log["current_weights"]["gex"],
        log["current_weights"]["behavior"],
        log["current_weights"]["bayesian"],
        log["current_weights"]["timedecay"],
    ]
    keys = ["gbm", "gex", "behavior", "bayesian", "timedecay"]

    def calc_error(weights):
        """計算給定權重下的總誤差"""
        total_sq_error = 0
        for r in completed:
            c = r["components"]
            pred = sum(weights[i] * c.get(keys[i], 0) for i in range(5))
            err = (pred - r["actual_settlement"]) ** 2
            total_sq_error += err
        return total_sq_error / len(completed)

    # 梯度下降
    lr = 0.001
    best_error = calc_error(w)
    best_weights = w.copy()

    for iteration in range(500):
        grad = [0.0] * 5
        for r in completed:
            c = r["components"]
            pred = sum(w[i] * c.get(keys[i], 0) for i in range(5))
            diff = 2 * (pred - r["actual_settlement"]) / len(completed)
            for i in range(5):
                grad[i] += diff * c.get(keys[i], 0)

        # 更新
        w_new = [max(0.01, w[i] - lr * grad[i]) for i in range(5)]
        # 歸一化
        total = sum(w_new)
        w_new = [x / total for x in w_new]

        new_error = calc_error(w_new)
        if new_error < best_error:
            best_error = new_error
            best_weights = w_new.copy()
            w = w_new

    new_weights = {keys[i]: round(best_weights[i], 4) for i in range(5)}
    print(f"\n優化結果:")
    print(f"  舊權重: {log['current_weights']}")
    print(f"  新權重: {new_weights}")
    print(f"  誤差改善: {calc_error([log['current_weights'][k] for k in keys]):.0f} -> {best_error:.0f}")

    # 保存
    log["weight_history"].append({
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "samples": len(completed),
        "old_weights": log["current_weights"].copy(),
        "new_weights": new_weights,
        "avg_error_usd": round(math.sqrt(best_error), 2)
    })
    log["current_weights"] = new_weights
    log["last_optimized"] = datetime.now(timezone.utc).isoformat()
    save_log(log)

    return new_weights

def check_and_record_settlement():
    """
    自動檢查Deribit結算價格
    每次run時檢查：最近到期日是否已結算
    """
    from datetime import date, timedelta
    today = date.today()

    # 從log找未結算的到期日
    log = load_log()
    pending = set()
    for r in log["records"]:
        if r["actual_settlement"] is None:
            pending.add(r["expiry"])

    if not pending:
        return

    # 把到期日字串轉換為date
    months = {"JAN":1,"FEB":2,"MAR":3,"APR":4,"MAY":5,"JUN":6,
              "JUL":7,"AUG":8,"SEP":9,"OCT":10,"NOV":11,"DEC":12}

    for expiry_str in pending:
        try:
            # 解析格式如 "3JUL26"
            import re
            m = re.match(r"(\d+)([A-Z]+)(\d+)", expiry_str)
            if not m:
                continue
            day, mon, yr = int(m.group(1)), months[m.group(2)], 2000+int(m.group(3))
            expiry_date = date(yr, mon, day)

            # 若已過期（今天 >= 到期日）
            if today >= expiry_date:
                # 從Deribit取歷史結算價
                instrument = f"BTC-{expiry_str}"
                r_api = requests.get(
                    f"https://www.deribit.com/api/v2/public/get_delivery_prices"
                    f"?index_name=btc_usd&offset=0&count=10",
                    timeout=10
                )
                deliveries = r_api.json().get("result", {}).get("data", [])
                for d in deliveries:
                    d_date = date.fromtimestamp(d["date"] / 1000)
                    if d_date == expiry_date:
                        settlement_price = float(d["delivery_price"])
                        n = record_settlement(expiry_str, settlement_price)
                        print(f"Auto-recorded settlement {expiry_str}: ${settlement_price:,.2f} ({n} records)")
                        break
        except Exception as e:
            print(f"Settlement check error {expiry_str}: {e}")

if __name__ == "__main__":
    # 測試
    check_and_record_settlement()
    weights = optimize_weights(min_samples=5)
    print(f"\n當前最優權重: {weights}")

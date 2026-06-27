"""
GEX Oracle 數據抓取模組
在GitHub Actions環境執行，探測可用API並抓取所有數據
"""
import requests, json, os, time
from datetime import datetime, timezone

UA = {"User-Agent": "Mozilla/5.0 GEX-Oracle/2.0"}

def probe_and_fetch():
    results = {}
    data = {}

    # ── Spot Price ──────────────────────────────────────────
    for url, key in [
        ("https://fapi.binance.com/fapi/v1/ticker/price?symbol=BTCUSDT", "price"),
        ("https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT", "price"),
        ("https://api.binance.com/api/v3/ticker/24hr?symbol=BTCUSDT", "lastPrice"),
    ]:
        try:
            r = requests.get(url, timeout=10, headers=UA)
            d = r.json()
            val = float(d.get(key) or d.get("lastPrice") or d.get("price", 0))
            if val > 0:
                data["spot"] = val
                results["spot"] = {"url": url, "value": val}
                print(f"Spot: ${val:,.2f} ✅")
                break
        except Exception as e:
            results[f"spot_{url[:30]}"] = str(e)

    # ── Funding Rate ────────────────────────────────────────
    for url, key in [
        ("https://fapi.binance.com/fapi/v1/premiumIndex?symbol=BTCUSDT", "lastFundingRate"),
        ("https://fapi.binance.com/fapi/v1/fundingRate?symbol=BTCUSDT&limit=1", None),
    ]:
        try:
            r = requests.get(url, timeout=10, headers=UA)
            d = r.json()
            if isinstance(d, list):
                val = float(d[0].get("fundingRate", 0))
            else:
                val = float(d.get(key, 0))
            data["fr"] = val
            results["fr"] = {"url": url, "value": val}
            print(f"FR: {val*100:+.5f}% ✅")
            break
        except Exception as e:
            results[f"fr_{url[:30]}"] = str(e)

    # ── Open Interest ───────────────────────────────────────
    for url in [
        "https://fapi.binance.com/fapi/v1/openInterest?symbol=BTCUSDT",
        "https://fapi.binance.com/fapi/v3/openInterest?symbol=BTCUSDT",
    ]:
        try:
            r = requests.get(url, timeout=10, headers=UA)
            d = r.json()
            oi = float(d.get("openInterest") or d.get("sumOpenInterest", 0))
            if oi > 0:
                data["oi"] = oi / 10000
                results["oi"] = {"url": url, "value": data["oi"]}
                print(f"OI: {data['oi']:.2f}萬 ✅")
                break
        except Exception as e:
            results[f"oi_{url[:30]}"] = str(e)

    # ── Long/Short Ratio ────────────────────────────────────
    for url in [
        "https://fapi.binance.com/futures/data/globalLongShortAccountRatio?symbol=BTCUSDT&period=5m&limit=1",
        "https://fapi.binance.com/futures/data/topLongShortAccountRatio?symbol=BTCUSDT&period=5m&limit=1",
        "https://fapi.binance.com/futures/data/topLongShortPositionRatio?symbol=BTCUSDT&period=5m&limit=1",
    ]:
        try:
            r = requests.get(url, timeout=10, headers=UA)
            d = r.json()
            if isinstance(d, list) and d:
                val = float(d[0].get("longShortRatio", 0))
                if val > 0:
                    data["ls"] = val
                    results["ls"] = {"url": url, "value": val}
                    print(f"L/S: {val:.4f} ✅")
                    break
        except Exception as e:
            results[f"ls_{url[:30]}"] = str(e)

    # ── MACD via Klines ─────────────────────────────────────
    def calc_ema(prices, period):
        k = 2 / (period + 1)
        ema = prices[0]
        for p in prices[1:]:
            ema = p * k + ema * (1 - k)
        return ema

    def calc_macd(closes):
        def ema_series(prices, p):
            k = 2 / (p + 1)
            r = [prices[0]]
            for x in prices[1:]:
                r.append(x * k + r[-1] * (1 - k))
            return r
        e12 = ema_series(closes, 12)
        e26 = ema_series(closes, 26)
        dif = [a - b for a, b in zip(e12, e26)]
        k9 = 2 / 10
        dea = [dif[0]]
        for d in dif[1:]:
            dea.append(d * k9 + dea[-1] * (1 - k9))
        macd = [(d - e) * 2 for d, e in zip(dif, dea)]
        return dif[-1], dea[-1], macd[-1]

    kline_urls = [
        ("https://fapi.binance.com/fapi/v1/klines?symbol=BTCUSDT&interval={}&limit=100", "futures"),
        ("https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval={}&limit=100", "spot"),
    ]
    data["macd"] = {}
    data["ema"] = {}
    for tf in ["15m", "4h", "1d"]:
        for url_tpl, src in kline_urls:
            try:
                r = requests.get(url_tpl.format(tf), timeout=10, headers=UA)
                klines = r.json()
                if isinstance(klines, list) and len(klines) > 30:
                    closes = [float(k[4]) for k in klines]
                    dif, dea, macd = calc_macd(closes)
                    data["macd"][tf] = {"dif": dif, "dea": dea, "macd": macd}
                    data["ema"][tf] = {
                        str(p): calc_ema(closes, p)
                        for p in [5, 10, 30, 200]
                        if len(closes) >= p
                    }
                    results[f"klines_{tf}"] = {"src": src, "bars": len(closes)}
                    print(f"MACD {tf}: DIF={dif:.2f} MACD={macd:.2f} ({src}) ✅")
                    break
            except Exception as e:
                results[f"klines_{tf}_{src}"] = str(e)
        time.sleep(0.2)

    # ── DVOL ────────────────────────────────────────────────
    dvol_urls = [
        ("https://www.deribit.com/api/v2/public/get_index_price?index_name=btc_usd", None),
        ("https://www.deribit.com/api/v2/public/get_volatility_index_data?currency=BTC&resolution=3600&count=2", None),
        ("https://deribit.com/api/v2/public/get_index?currency=BTC", "BTC"),
    ]
    for url, key in dvol_urls:
        try:
            r = requests.get(url, timeout=10, headers=UA)
            d = r.json().get("result", {})
            if isinstance(d, dict):
                val = float(d.get("index_price") or d.get("current_index") or 0)
                if val == 0 and "data" in d:
                    val = float(d["data"][-1][4]) if d["data"] else 0
                if val == 0 and key and key in d:
                    val = float(d[key])
            elif isinstance(d, list) and d:
                val = float(d[-1][4]) if len(d[-1]) > 4 else 0
            else:
                val = 0
            if val > 0:
                data["dvol"] = val
                results["dvol"] = {"url": url, "value": val}
                print(f"DVOL: {val:.2f}% ✅")
                break
        except Exception as e:
            results[f"dvol_{url[:40]}"] = str(e)

    if "dvol" not in data:
        data["dvol"] = 46.5  # fallback
        print(f"DVOL: fallback 46.5%")

    # ── Deribit Options ─────────────────────────────────────
    data["options"] = {}
    for expiry in ["3JUL26", "31JUL26", "25SEP26"]:
        for url in [
            f"https://www.deribit.com/api/v2/public/get_book_summary_by_currency?currency=BTC&kind=option",
        ]:
            try:
                r = requests.get(url, timeout=15, headers=UA)
                items = r.json().get("result", [])
                opts = {}
                for item in items:
                    name = item.get("instrument_name", "")
                    parts = name.split("-")
                    if len(parts) != 4:
                        continue
                    _, exp, strike_str, opt_type = parts
                    if exp.upper() != expiry.upper():
                        continue
                    strike = int(strike_str)
                    oi = float(item.get("open_interest", 0))
                    iv = float(item.get("mark_iv", 0))
                    if strike not in opts:
                        opts[strike] = {"call_oi": 0, "put_oi": 0, "call_iv": 0, "put_iv": 0}
                    if opt_type == "C":
                        opts[strike]["call_oi"] = oi
                        opts[strike]["call_iv"] = iv
                    else:
                        opts[strike]["put_oi"] = oi
                        opts[strike]["put_iv"] = iv
                if opts:
                    data["options"][expiry] = opts
                    results[f"opts_{expiry}"] = len(opts)
                    print(f"Opts {expiry}: {len(opts)} strikes ✅")
                    break
            except Exception as e:
                results[f"opts_{expiry}"] = str(e)
        time.sleep(0.5)

    data["timestamp"] = datetime.now(timezone.utc).isoformat()
    data["api_results"] = results

    os.makedirs("data", exist_ok=True)
    with open("data/oracle_market_data.json", "w") as f:
        json.dump(data, f, indent=2)

    print(f"\n=== 數據摘要 ===")
    print(f"Spot: ${data.get('spot', 'N/A'):,.0f}" if data.get('spot') else "Spot: N/A")
    print(f"FR: {data.get('fr', 0)*100:+.5f}%" if data.get('fr') is not None else "FR: N/A")
    print(f"OI: {data.get('oi', 'N/A'):.2f}萬" if data.get('oi') else "OI: N/A")
    print(f"L/S: {data.get('ls', 'N/A'):.4f}" if data.get('ls') else "L/S: N/A")
    print(f"DVOL: {data.get('dvol', 0):.2f}%")
    print(f"MACD時框: {list(data.get('macd', {}).keys())}")
    print(f"期權到期日: {list(data.get('options', {}).keys())}")
    return data

if __name__ == "__main__":
    probe_and_fetch()

"""
Trading Desk Dashboard
Real-time pre-market scanner: COT, macro, sentiment, news, Fear & Greed.
"""

import asyncio
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
import oandapyV20
import oandapyV20.endpoints.pricing as pricing
import oandapyV20.endpoints.instruments as instruments
from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from lib.cot import (
    compute_cot_index,
    extract_market,
    extract_market_fin,
    fetch_cot_history,
)

# ── Config ────────────────────────────────────────────────────

load_dotenv()

OANDA_TOKEN = os.environ["OANDA_TOKEN"]
OANDA_ACCOUNT_ID = os.environ["OANDA_ACCOUNT_ID"]
OANDA_ENV = os.getenv("OANDA_ENV", "practice")
FRED_API_KEY = os.environ["FRED_API_KEY"]
HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "8888"))

client = oandapyV20.API(access_token=OANDA_TOKEN, environment=OANDA_ENV)

DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(exist_ok=True)

# ── Markets ───────────────────────────────────────────────────

MARKETS = {
    "EUR": {"oanda": "EUR_USD", "cftc": "099741", "name": "EUR/USD", "invert": False},
    "GBP": {"oanda": "GBP_USD", "cftc": "096742", "name": "GBP/USD", "invert": False},
    "AUD": {"oanda": "AUD_USD", "cftc": "232741", "name": "AUD/USD", "invert": False},
    "CAD": {"oanda": "USD_CAD", "cftc": "090741", "name": "USD/CAD", "invert": True},
    "CHF": {"oanda": "USD_CHF", "cftc": "092741", "name": "USD/CHF", "invert": True},
    "JPY": {"oanda": "USD_JPY", "cftc": "097741", "name": "USD/JPY", "invert": True},
    "Gold": {"oanda": "XAU_USD", "cftc": "088691", "name": "XAU/USD", "invert": False},
    "Silver": {"oanda": "XAG_USD", "cftc": "084691", "name": "XAG/USD", "invert": False},
    "Oil": {"oanda": "WTICO_USD", "cftc": "067651", "name": "WTI/USD", "invert": False},
    "NZD": {"oanda": "NZD_USD", "cftc": "112741", "name": "NZD/USD", "invert": False},
}

# COT edge validation (from quant research: IC + p-value)
VALIDATED_EDGE = {
    "EUR": {"ic": 0.173, "pval": 0.0001, "stars": "High"},
    "CAD": {"ic": -0.201, "pval": 0.0000, "stars": "High"},
    "AUD": {"ic": -0.119, "pval": 0.007, "stars": "High"},
    "CHF": {"ic": 0.142, "pval": 0.037, "stars": "Med"},
    "GBP": {"ic": -0.026, "pval": 0.567, "stars": ""},
    "JPY": {"ic": -0.030, "pval": 0.488, "stars": ""},
    "Gold": {"ic": -0.012, "pval": 0.787, "stars": ""},
    "Silver": {"ic": 0, "pval": 1, "stars": ""},
    "Oil": {"ic": 0, "pval": 1, "stars": ""},
    "NZD": {"ic": 0, "pval": 1, "stars": ""},
}

# ── Data Cache ────────────────────────────────────────────────

cache = {
    "prices": {},
    "daily_change": {},
    "weekly_change": {},
    "cot": {},
    "macro": {},
    "news": [],
    "last_update": {},
}


# ── Data Fetchers ─────────────────────────────────────────────


def fetch_prices():
    """Fetch live prices from OANDA."""
    instruments_list = [m["oanda"] for m in MARKETS.values()]
    params = {"instruments": ",".join(instruments_list)}
    try:
        r = pricing.PricingInfo(accountID=OANDA_ACCOUNT_ID, params=params)
        client.request(r)
        for p in r.response["prices"]:
            inst = p["instrument"]
            bid = float(p["bids"][0]["price"])
            ask = float(p["asks"][0]["price"])
            mid = (bid + ask) / 2
            for key, m in MARKETS.items():
                if m["oanda"] == inst:
                    if "JPY" in inst:
                        spread = round((ask - bid) * 100, 1)
                    elif "XAU" in inst or "XAG" in inst or "WTICO" in inst:
                        spread = round(ask - bid, 2)
                    else:
                        spread = round((ask - bid) * 10000, 1)
                    cache["prices"][key] = {"bid": bid, "ask": ask, "mid": mid, "spread": spread}
                    break
    except Exception as e:
        print(f"Price fetch error: {e}")


def fetch_daily_weekly_changes():
    """Fetch D1 candles to compute daily/weekly change."""
    for key, m in MARKETS.items():
        try:
            params = {"granularity": "D", "count": 6, "price": "M"}
            r = instruments.InstrumentsCandles(instrument=m["oanda"], params=params)
            client.request(r)
            candles = r.response["candles"]
            if len(candles) >= 2:
                prev = float(candles[-2]["mid"]["c"])
                curr = float(candles[-1]["mid"]["c"])
                daily_pct = (curr - prev) / prev * 100
                if m["invert"]:
                    daily_pct = -daily_pct
                cache["daily_change"][key] = round(daily_pct, 2)
            if len(candles) >= 6:
                week_ago = float(candles[0]["mid"]["c"])
                curr = float(candles[-1]["mid"]["c"])
                weekly_pct = (curr - week_ago) / week_ago * 100
                if m["invert"]:
                    weekly_pct = -weekly_pct
                cache["weekly_change"][key] = round(weekly_pct, 2)
        except Exception as e:
            print(f"Candle fetch error {key}: {e}")


def fetch_macro():
    """Fetch macro data from OANDA (SPX) + FRED (DXY, yields, VIX)."""
    try:
        params = {"instruments": "SPX500_USD,US30_USD"}
        r = pricing.PricingInfo(accountID=OANDA_ACCOUNT_ID, params=params)
        client.request(r)
        for p in r.response["prices"]:
            mid = (float(p["bids"][0]["price"]) + float(p["asks"][0]["price"])) / 2
            if p["instrument"] == "SPX500_USD":
                cache["macro"]["SPX"] = round(mid, 1)
            elif p["instrument"] == "US30_USD":
                cache["macro"]["DOW"] = round(mid, 1)
    except Exception as e:
        print(f"SPX fetch error: {e}")

    if cache["macro"].get("_fred_ts", 0) < time.time() - 3600:
        try:
            from fredapi import Fred

            fred = Fred(api_key=FRED_API_KEY)
            series = {
                "DXY": ("DTWEXBGS", "2026-01-01"),
                "US10Y": ("DGS10", "2026-01-01"),
                "US02Y": ("DGS2", "2026-01-01"),
                "VIX": ("VIXCLS", "2026-01-01"),
                "FED": ("FEDFUNDS", "2025-01-01"),
            }
            for label, (sid, start) in series.items():
                s = fred.get_series(sid, observation_start=start).dropna()
                if len(s) > 0:
                    cache["macro"][label] = round(float(s.iloc[-1]), 2)

            if "US10Y" in cache["macro"] and "US02Y" in cache["macro"]:
                cache["macro"]["CURVE"] = round(cache["macro"]["US10Y"] - cache["macro"]["US02Y"], 2)

            cache["macro"]["_fred_ts"] = time.time()
        except Exception as e:
            print(f"FRED fetch error: {e}")


def fetch_cot():
    """Load COT data from CFTC."""
    try:
        cot_cache = str(DATA_DIR / "cot")
        df_disagg, df_fin = fetch_cot_history(cot_cache)

        disagg_codes = {"Gold": "88", "Silver": "84", "Oil": "67"}
        fin_patterns = {
            "EUR": "EURO FX", "GBP": "BRITISH POUND", "JPY": "JAPANESE YEN",
            "AUD": "AUSTRALIAN DOLLAR", "CAD": "CANADIAN DOLLAR",
            "CHF": "SWISS FRANC", "NZD": "NEW ZEALAND DOLLAR",
        }

        for key in MARKETS:
            if key in disagg_codes:
                df_m = extract_market(df_disagg, disagg_codes[key])
            elif key in fin_patterns:
                df_m = extract_market_fin(df_fin, fin_patterns[key])
            else:
                continue

            if len(df_m) < 52:
                continue

            df_idx = compute_cot_index(df_m)
            if len(df_idx) == 0:
                continue

            latest = df_idx.iloc[-1]
            cache["cot"][key] = {
                "sm_index": round(float(latest.get("cot_index_sm", 50)), 1),
                "sm_net": int(latest.get("sm_net", 0)),
                "date": str(latest["date"].date()),
            }
    except Exception as e:
        print(f"COT fetch error: {e}")


def fetch_news():
    """Fetch economic calendar from faireconomy API."""
    if cache.get("_news_ts", 0) > time.time() - 300:
        return

    news_cache_file = DATA_DIR / "news_cache.json"

    try:
        resp = requests.get("https://nfs.faireconomy.media/ff_calendar_thisweek.json", timeout=10)
        if resp.status_code != 200:
            if news_cache_file.exists():
                events = json.loads(news_cache_file.read_text())
            else:
                return
        else:
            events = resp.json()
            news_cache_file.write_text(json.dumps(events))
    except Exception:
        if news_cache_file.exists():
            events = json.loads(news_cache_file.read_text())
        else:
            return

    now = datetime.now(timezone.utc)
    news_list = []
    for e in events:
        if not e.get("date"):
            continue
        impact = e.get("impact", "")
        if impact not in ("High", "Medium", "Low"):
            continue
        try:
            event_dt = datetime.fromisoformat(e["date"]).astimezone(timezone.utc)
        except Exception:
            try:
                from dateutil import parser as dtparser
                event_dt = dtparser.parse(e["date"]).astimezone(timezone.utc)
            except Exception:
                continue

        delta = (event_dt - now).total_seconds()
        if delta > 0:
            hours = int(delta // 3600)
            mins = int((delta % 3600) // 60)
            countdown = f"{hours}h {mins}m" if hours > 0 else f"{mins}m"
            status = "upcoming"
        else:
            countdown = "LIVE"
            status = "live"

        news_list.append({
            "time_utc": event_dt.strftime("%H:%M"),
            "date": event_dt.strftime("%a %d"),
            "title": e.get("title", ""),
            "country": e.get("country", ""),
            "impact": impact,
            "forecast": e.get("forecast", ""),
            "previous": e.get("previous", ""),
            "actual": e.get("actual", ""),
            "countdown": countdown,
            "status": status,
            "timestamp": event_dt.isoformat(),
        })

    news_list.sort(key=lambda x: x["timestamp"])
    cache["news"] = news_list
    cache["_news_ts"] = time.time()


def fetch_sentiment():
    """Fetch retail sentiment from OANDA Position Book."""
    if cache.get("_sentiment_ts", 0) > time.time() - 300:
        return
    for key, m in MARKETS.items():
        try:
            r = instruments.InstrumentsPositionBook(instrument=m["oanda"])
            client.request(r)
            buckets = r.response["positionBook"]["buckets"]
            pct_long = sum(float(b["longCountPercent"]) for b in buckets)
            pct_short = sum(float(b["shortCountPercent"]) for b in buckets)
            cache.setdefault("sentiment", {})[key] = {
                "long": round(pct_long, 1),
                "short": round(pct_short, 1),
            }
        except Exception:
            pass  # Some instruments don't have position book
    cache["_sentiment_ts"] = time.time()


# ── Computed Data ─────────────────────────────────────────────


def compute_macro_gauges():
    """Gauge levels for macro metrics (only universally accepted thresholds)."""
    m = cache["macro"]
    gauges = {}

    # VIX: CBOE standard levels
    vix = m.get("VIX", 20)
    if vix < 15:
        gauges["VIX"] = {"level": "Calm", "color": "green", "pct": min(100, vix / 40 * 100)}
    elif vix < 20:
        gauges["VIX"] = {"level": "Normal", "color": "blue", "pct": vix / 40 * 100}
    elif vix < 30:
        gauges["VIX"] = {"level": "Elevated", "color": "yellow", "pct": vix / 40 * 100}
    else:
        gauges["VIX"] = {"level": "Fear", "color": "red", "pct": min(100, vix / 40 * 100)}

    gauges["DXY"] = {"level": "", "color": "blue", "pct": 50}
    gauges["US10Y"] = {"level": "", "color": "blue", "pct": 50}

    # Yield Curve inversion: universally accepted warning signal
    curve = m.get("CURVE", 0)
    if curve < 0:
        gauges["CURVE"] = {"level": "Inverted", "color": "red", "pct": 20}
    else:
        gauges["CURVE"] = {"level": "", "color": "green", "pct": 80}

    return gauges


def compute_fear_greed():
    """Fetch CNN Fear & Greed Index."""
    if cache.get("_fg_ts", 0) > time.time() - 1800 and cache.get("_fg_data"):
        return cache["_fg_data"]

    try:
        resp = requests.get(
            "https://production.dataviz.cnn.io/index/fearandgreed/current",
            timeout=10,
            headers={"User-Agent": "TradingDesk/1.0"},
        )
        if resp.status_code == 200:
            data = resp.json()
            score = round(data.get("score", 50))
            rating = data.get("rating", "neutral")
            color = "red" if score < 40 else "green" if score > 60 else "gray"
            result = {"score": score, "label": rating.replace("_", " ").title(), "color": color}
            cache["_fg_data"] = result
            cache["_fg_ts"] = time.time()
            return result
    except Exception as e:
        print(f"Fear & Greed API error: {e}")

    return cache.get("_fg_data", {"score": 50, "label": "Neutral", "color": "gray"})


def compute_bias():
    """Compute directional bias from COT data."""
    result = {}
    for key in MARKETS:
        cot = cache["cot"].get(key, {})
        sm = cot.get("sm_index", 50)
        edge = VALIDATED_EDGE.get(key, {})

        if sm > 75:
            bias = "BUY" if not MARKETS[key]["invert"] else "SELL"
            bias_raw = "BULLISH"
        elif sm < 25:
            bias = "SELL" if not MARKETS[key]["invert"] else "BUY"
            bias_raw = "BEARISH"
        else:
            bias = ""
            bias_raw = "NEUTRAL"

        result[key] = {
            "bias": bias,
            "bias_raw": bias_raw,
            "edge": edge.get("stars", ""),
            "ic": edge.get("ic", 0),
            "pval": edge.get("pval", 1),
        }
    return result


def get_current_session():
    """Determine current trading session based on UTC hour."""
    hour = datetime.now(timezone.utc).hour
    if 22 <= hour or hour < 7:
        return {"name": "Asian", "color": "orange"}
    elif 7 <= hour < 12:
        return {"name": "London", "color": "purple"}
    elif 12 <= hour < 17:
        return {"name": "New York", "color": "teal"}
    elif 17 <= hour < 22:
        return {"name": "NY PM / Close", "color": "gray"}
    return {"name": "-", "color": "gray"}


def _generate_insight(markets, macro, fear_greed, session):
    """Generate 1-2 sentence market insight."""
    biased = [m for m in markets if m["bias"] != ""]
    fg_score = fear_greed.get("score", 50)

    if fg_score <= 25:
        mood = "Markets in panic. Expect sharp moves, wide spreads. Reduce size or wait for calm."
    elif fg_score <= 40:
        mood = "Fear in the market. Volatility likely. Trade with caution, tighter risk."
    elif fg_score >= 75:
        mood = "Extreme greed. Crowded trades may reverse. Watch for profit-taking."
    elif fg_score >= 60:
        mood = "Optimistic market. Momentum trades favored, but stay alert for shifts."
    else:
        mood = "Balanced sentiment. No strong macro pressure either way."

    vix = macro.get("VIX", 20)
    if vix > 30:
        mood += " VIX above 30, expect outsized swings."

    if biased:
        signals = []
        for m in biased[:3]:
            direction = "Buy" if m["bias"] == "BUY" else "Sell"
            signals.append(f"{direction} {m['name']} (SM {m['cot_sm']:.0f})")
        cot_str = "COT signals: " + ", ".join(signals) + "."
    else:
        cot_str = "No extreme COT signals this week."

    news = cache.get("news", [])
    now = datetime.now(timezone.utc)
    news_str = ""
    for n in news:
        if n.get("impact") != "High" or n.get("country") != "USD":
            continue
        try:
            evt_dt = datetime.fromisoformat(n["timestamp"])
            delta_h = (evt_dt - now).total_seconds() / 3600
            if 0 < delta_h < 4:
                news_str = f" Heads up: {n['title']} coming soon."
                break
        except Exception:
            pass

    return f"{mood} {cot_str}{news_str}"


def build_snapshot():
    """Build complete dashboard snapshot."""
    biases = compute_bias()
    session = get_current_session()
    gauges = compute_macro_gauges()
    fear_greed = compute_fear_greed()

    markets = []
    for key, m in MARKETS.items():
        price_data = cache["prices"].get(key, {})
        cot_data = cache["cot"].get(key, {})
        bias_data = biases.get(key, {})

        markets.append({
            "key": key,
            "name": m["name"],
            "price": price_data.get("mid", 0),
            "bid": price_data.get("bid", 0),
            "ask": price_data.get("ask", 0),
            "spread": price_data.get("spread", 0),
            "daily_chg": cache["daily_change"].get(key, 0),
            "weekly_chg": cache["weekly_change"].get(key, 0),
            "cot_sm": cot_data.get("sm_index", 50),
            "cot_net": cot_data.get("sm_net", 0),
            "cot_date": cot_data.get("date", ""),
            "bias": bias_data.get("bias", ""),
            "bias_raw": bias_data.get("bias_raw", "NEUTRAL"),
            "edge": bias_data.get("edge", ""),
            "ic": bias_data.get("ic", 0),
            "pval": bias_data.get("pval", 1),
            "invert": m["invert"],
            "retail_long": cache.get("sentiment", {}).get(key, {}).get("long", 0),
            "retail_short": cache.get("sentiment", {}).get(key, {}).get("short", 0),
        })

    markets.sort(key=lambda x: -abs(x["cot_sm"] - 50))
    macro = {k: v for k, v in cache["macro"].items() if not k.startswith("_")}
    insight = _generate_insight(markets, macro, fear_greed, session)

    return {
        "markets": markets,
        "macro": macro,
        "session": session,
        "news": cache.get("news", []),
        "gauges": gauges,
        "fear_greed": fear_greed,
        "insight": insight,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


# ── FastAPI App ───────────────────────────────────────────────

app = FastAPI(title="Trading Desk Dashboard")
static_dir = Path(__file__).parent / "static"


@app.on_event("startup")
async def startup():
    """Load initial data on server start."""
    print("Loading COT data...")
    fetch_cot()
    print("Loading macro data...")
    fetch_macro()
    print("Fetching news...")
    fetch_news()
    print("Fetching sentiment...")
    fetch_sentiment()
    print("Fetching prices...")
    fetch_prices()
    fetch_daily_weekly_changes()
    print("Dashboard ready.")


@app.get("/")
async def index():
    return FileResponse(static_dir / "index.html")


@app.get("/api/snapshot")
async def api_snapshot():
    return build_snapshot()


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    tick = 0
    try:
        while True:
            fetch_prices()
            if tick % 20 == 0:
                fetch_news()
            if tick % 100 == 0:
                fetch_daily_weekly_changes()
                fetch_sentiment()
            if tick % 600 == 0 and tick > 0:
                fetch_macro()

            snapshot = build_snapshot()
            await ws.send_json(snapshot)
            await asyncio.sleep(3)
            tick += 1
    except WebSocketDisconnect:
        pass
    except Exception as e:
        print(f"WS error: {e}")


app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host=HOST, port=PORT)

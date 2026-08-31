"""
PRAVAH — Predictive Routing And Voyage Analytics Hub
FastAPI + WebSocket backend.

Live external data sources used (no API key required):
  - Open-Meteo          -> real-time weather at every port, feeds voyage risk
  - Frankfurter (ECB)   -> real live + historical FX rates (USD/INR etc.)
Everything else (freight rates, commodities) is a clearly-labelled calibrated
simulation — swap for a real feed/model when you have one; response shapes
are kept stable so the frontend never needs to change.

Run:
    pip install -r requirements.txt
    uvicorn main:app --reload --port 8000
"""

import asyncio
import json
import math
import os
import random
from datetime import datetime, timedelta
from io import BytesIO
from pathlib import Path
from typing import Dict, List, Optional

import httpx
import websockets
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse, StreamingResponse, FileResponse
from pydantic import BaseModel

BASE_DIR = Path(__file__).resolve().parent
FRONTEND_FILE = BASE_DIR / "pravah_terminal.html"

app = FastAPI(title="PRAVAH API — Predictive Routing And Voyage Analytics Hub")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # tighten before deploying publicly
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Reference data. Coordinates are approximate port-city locations, good enough
# for weather lookups and map plotting — verify draft/LOA/berth figures against
# current port-authority circulars before treating them as fact.
# ---------------------------------------------------------------------------

PORTS = {
    "Paradip":     {"draft": 18.1, "loa_max": 300, "berths_total": 6, "berths_free": 2, "congestion": 38, "lat": 20.2648, "lon": 86.6947},
    "Vizag":       {"draft": 17.0, "loa_max": 290, "berths_total": 7, "berths_free": 1, "congestion": 62, "lat": 17.6868, "lon": 83.2185},
    "Gangavaram":  {"draft": 20.2, "loa_max": 330, "berths_total": 5, "berths_free": 3, "congestion": 22, "lat": 17.6205, "lon": 83.2504},
    "Dhamra":      {"draft": 18.5, "loa_max": 300, "berths_total": 4, "berths_free": 2, "congestion": 45, "lat": 20.7880, "lon": 86.9491},
    "Haldia":      {"draft": 8.6,  "loa_max": 186, "berths_total": 5, "berths_free": 1, "congestion": 71, "lat": 22.0667, "lon": 88.0698},
}

ORIGIN_INFO = {
    "Australia":  {"lat": -32.9283, "lon": 151.7817, "distance_factor": 1.00, "label": "Newcastle, NSW"},
    "Indonesia":  {"lat": -1.2379,  "lon": 116.8529, "distance_factor": 0.88, "label": "Samarinda, Kalimantan"},
    "Mozambique": {"lat": -19.8436, "lon": 34.8389,  "distance_factor": 1.32, "label": "Beira"},
}

VESSEL_CLASSES = {
    "Handysize": {"loa_max": 190, "dwt_mid": 34000},
    "Supramax":  {"loa_max": 200, "dwt_mid": 55000},
    "Panamax":   {"loa_max": 230, "dwt_mid": 72000},
    "Capesize":  {"loa_max": 300, "dwt_mid": 165000},
}

# Typical laden service speed by class, in knots — used for transit-time and
# repositioning estimates. Real vessels vary; these are reasonable averages.
VESSEL_SPEED_KNOTS = {"Handysize": 13.0, "Supramax": 13.5, "Panamax": 14.0, "Capesize": 14.5}
BALLAST_FUEL_COST_PER_NM_USD = 8.0  # rough bunker cost proxy per nautical mile while empty


def haversine_nm(lat1, lon1, lat2, lon2) -> float:
    """Great-circle distance in nautical miles."""
    R_km = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlambda / 2) ** 2
    km = 2 * R_km * math.asin(math.sqrt(a))
    return km * 0.539957  # km -> nautical miles

ROUTES = [
    "Australia-Paradip", "Indonesia-Vizag", "Mozambique-Gangavaram",
    "Australia-Dhamra", "Indonesia-Haldia",
]

live_rates: Dict[str, float] = {"Handysize": 14.2, "Supramax": 16.8, "Panamax": 15.4, "Capesize": 11.9}
rate_history: Dict[str, List[float]] = {k: [v] * 24 for k, v in live_rates.items()}

# ---------------------------------------------------------------------------
# LIVE market signals (Yahoo Finance chart endpoint — free, no key required,
# the same endpoint the popular `yfinance` library uses).
# The Baltic Exchange dry-bulk freight indices themselves are licensed/paid
# (Baltic Exchange, Clarksons, Drewry all charge for API access) — there is
# no free legal live feed for them. Instead we use two real, freely-available
# signals that genuinely drive freight rates and let them nudge our model:
#   - WTI crude oil price -> direct proxy for bunker fuel cost pressure
#   - Publicly-traded dry-bulk shipping company stocks (Star Bulk, Golden
#     Ocean, Genco) -> their share prices move with real freight market
#     sentiment, since that IS their business
# The per-vessel-class $/tonne figures remain a calibrated model, but are now
# live-adjusted by these real signals rather than a pure random walk.
# ---------------------------------------------------------------------------

YAHOO_SYMBOLS = {"oil": "CL=F", "SBLK": "SBLK", "GOGL": "GOGL", "GNK": "GNK"}
market_signals_cache = {
    "source": "not yet fetched",
    "oil_usd": None, "oil_change_pct": 0.0,
    "equities": {}, "equity_change_pct": 0.0,
    "bias": 0.0,
}


async def fetch_yahoo_quote(client: httpx.AsyncClient, symbol: str) -> Optional[dict]:
    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
        r = await client.get(url, params={"interval": "1d", "range": "5d"})
        r.raise_for_status()
        result = r.json()["chart"]["result"][0]
        meta = result["meta"]
        price = meta.get("regularMarketPrice")
        prev_close = meta.get("chartPreviousClose") or meta.get("previousClose")
        if price is None or not prev_close:
            return None
        return {"close": price, "change_pct": round((price - prev_close) / prev_close * 100, 3)}
    except Exception as e:
        print(f"[PRAVAH] Yahoo Finance fetch failed for '{symbol}': {type(e).__name__}: {e}")
        return None


async def refresh_market_signals():
    try:
        # Yahoo's chart endpoint wants a browser-like User-Agent or it can
        # reject requests — this is the same endpoint the popular `yfinance`
        # library uses under the hood.
        headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}
        async with httpx.AsyncClient(timeout=8, headers=headers, follow_redirects=True) as client:
            oil, sblk, gogl, gnk = await asyncio.gather(
                fetch_yahoo_quote(client, YAHOO_SYMBOLS["oil"]),
                fetch_yahoo_quote(client, YAHOO_SYMBOLS["SBLK"]),
                fetch_yahoo_quote(client, YAHOO_SYMBOLS["GOGL"]),
                fetch_yahoo_quote(client, YAHOO_SYMBOLS["GNK"]),
            )
        equities = {k: v for k, v in {"SBLK": sblk, "GOGL": gogl, "GNK": gnk}.items() if v is not None}
        if oil is None and not equities:
            raise RuntimeError("all Yahoo Finance quotes failed")

        oil_change = oil["change_pct"] if oil else 0.0
        equity_change = sum(v["change_pct"] for v in equities.values()) / len(equities) if equities else 0.0
        # small, deliberately conservative weighting — real signals *nudge* the
        # model, they don't dominate it
        bias = 0.5 * (oil_change / 100) + 0.5 * (equity_change / 100)

        market_signals_cache.update({
            "source": "Yahoo Finance (live, free public market data)",
            "oil_usd": oil["close"] if oil else None,
            "oil_change_pct": oil_change,
            "equities": equities,
            "equity_change_pct": round(equity_change, 3),
            "bias": bias,
            "fetched_at": datetime.utcnow().isoformat(),
        })
    except Exception as e:
        print(f"[PRAVAH] market signals refresh FAILED: {type(e).__name__}: {e}")
        market_signals_cache["source"] = "fallback (Yahoo Finance unreachable) — freight model running on baseline drift only"
        market_signals_cache["bias"] = 0.0


@app.get("/api/market-signals")
def get_market_signals():
    return market_signals_cache


def step_rates() -> Dict[str, float]:
    """Freight rate movement = small random walk + a real-market-data bias
    (see refresh_market_signals). Baltic Exchange's actual index is licensed;
    this is the closest honest approximation to 'live' without paying for it."""
    bias = market_signals_cache.get("bias", 0.0)
    for cls, val in live_rates.items():
        random_component = (random.random() - 0.5) * 0.05 * val
        bias_component = bias * val * 0.4  # bounded nudge, not a dominant force
        drift = random_component + bias_component
        live_rates[cls] = max(3.0, val + drift)
        rate_history[cls].append(live_rates[cls])
        if len(rate_history[cls]) > 100:
            rate_history[cls].pop(0)
    return dict(live_rates)


# ---------------------------------------------------------------------------
# WebSocket: live rate ticks
# ---------------------------------------------------------------------------

class ConnectionManager:
    def __init__(self):
        self.active: List[WebSocket] = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.active.append(ws)

    def disconnect(self, ws: WebSocket):
        if ws in self.active:
            self.active.remove(ws)

    async def broadcast(self, message: dict):
        dead = []
        for ws in self.active:
            try:
                await ws.send_json(message)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.disconnect(ws)


manager = ConnectionManager()
ship_manager = ConnectionManager()

# ---------------------------------------------------------------------------
# LIVE: real vessel positions via AISStream.io (https://aisstream.io).
# This is genuinely live AIS data — actual ship positions broadcast over VHF
# and relayed by AISStream's network — not a simulation. It's free but
# requires a free account + API key (there's no fully anonymous/keyless tier).
#
# Setup: sign up at https://aisstream.io, grab your API key, then run the
# backend with:  AISSTREAM_API_KEY=your-key-here uvicorn main:app --reload
# If the key isn't set, this feature is silently skipped and the map just
# shows ports/routes without live ships — everything else still works.
#
# NOTE: aisstream.io's exact message schema may evolve after this was written
# — if ships don't appear, check https://aisstream.io/documentation and adjust
# the field names below (MessageType/PositionReport/MetaData structure).
# ---------------------------------------------------------------------------

AISSTREAM_API_KEY = os.environ.get("AISSTREAM_API_KEY", "").strip()
# Rough bounding box covering Mozambique -> India -> Indonesia/Australia
# shipping lanes. [[lat, lon], [lat, lon]] per box, per AISStream docs.
AIS_BOUNDING_BOXES = [[[-40.0, 25.0], [30.0, 160.0]]]
ship_positions: Dict[str, dict] = {}  # mmsi -> latest known position (snapshot for new page loads)


@app.websocket("/ws/ships")
async def ws_ships(websocket: WebSocket):
    await ship_manager.connect(websocket)
    try:
        for pos in list(ship_positions.values()):
            await websocket.send_json({"type": "ship_position", **pos})
        while True:
            await asyncio.sleep(60)
    except WebSocketDisconnect:
        ship_manager.disconnect(websocket)


async def ais_stream_loop():
    if not AISSTREAM_API_KEY:
        print("[PRAVAH] AISSTREAM_API_KEY not set — live ship tracking disabled. "
              "Get a free key at https://aisstream.io and set the env var to enable it.")
        return
    subscribe_message = {
        "APIKey": AISSTREAM_API_KEY,
        "BoundingBoxes": AIS_BOUNDING_BOXES,
        "FilterMessageTypes": ["PositionReport"],
    }
    while True:
        try:
            async with websockets.connect("wss://stream.aisstream.io/v0/stream", ping_interval=20) as ws:
                await ws.send(json.dumps(subscribe_message))
                print("[PRAVAH] Connected to AISStream.io — streaming live vessel positions.")
                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                        if msg.get("MessageType") != "PositionReport":
                            continue
                        report = msg.get("Message", {}).get("PositionReport", {})
                        meta = msg.get("MetaData", {})
                        mmsi = str(meta.get("MMSI") or report.get("UserID") or "")
                        lat, lon = report.get("Latitude"), report.get("Longitude")
                        if not mmsi or lat is None or lon is None:
                            continue
                        position = {
                            "mmsi": mmsi,
                            "name": (meta.get("ShipName") or "").strip() or f"MMSI {mmsi}",
                            "lat": lat, "lon": lon,
                            "course": report.get("Cog", 0),
                            "speed_knots": report.get("Sog", 0),
                            "timestamp": meta.get("time_utc", datetime.utcnow().isoformat()),
                        }
                        ship_positions[mmsi] = position
                        if len(ship_positions) > 3000:  # cap memory
                            ship_positions.pop(next(iter(ship_positions)))
                        await ship_manager.broadcast({"type": "ship_position", **position})
                    except Exception as inner_e:
                        print(f"[PRAVAH] AIS message parse error: {inner_e}")
        except Exception as e:
            print(f"[PRAVAH] AIS stream connection lost/failed: {type(e).__name__}: {e} — retrying in 10s")
            await asyncio.sleep(10)


@app.websocket("/ws/rates")
async def ws_rates(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            await asyncio.sleep(60)
    except WebSocketDisconnect:
        manager.disconnect(websocket)


@app.on_event("startup")
async def start_tick_loop():
    async def rate_loop():
        while True:
            rates = step_rates()
            await manager.broadcast({
                "type": "rate_tick",
                "timestamp": datetime.utcnow().isoformat(),
                "rates": rates,
            })
            await asyncio.sleep(1.8)

    async def market_signals_loop():
        while True:
            await refresh_market_signals()
            await asyncio.sleep(60)  # real markets don't move meaningfully faster than this for our purposes

    asyncio.create_task(rate_loop())
    asyncio.create_task(market_signals_loop())
    asyncio.create_task(ais_stream_loop())


# ---------------------------------------------------------------------------
# Forecasting
# ---------------------------------------------------------------------------

def compute_forecast_points(vessel_class: str, horizon_days: int = 90, step_days: int = 5):
    base = live_rates[vessel_class]
    points = []
    val = base
    today = datetime.utcnow()
    for i in range(0, horizon_days, step_days):
        val += (random.random() - 0.48) * 0.6
        points.append({
            "date": (today + timedelta(days=i)).date().isoformat(),
            "mid": round(val, 2),
            "lo": round(val * 0.9, 2),
            "hi": round(val * 1.1, 2),
        })
    return points


@app.get("/api/forecast")
def forecast(route: str, vessel_class: str, horizon_days: int = 90, step_days: int = 5):
    """Synthetic-but-plausible forecast with confidence band. Swap the body
    for a trained Prophet/XGBoost model once you have real historical rate
    data — keep the response shape so the frontend doesn't need to change."""
    if vessel_class not in VESSEL_CLASSES:
        return {"error": f"unknown vessel_class '{vessel_class}'"}
    return {"route": route, "vessel_class": vessel_class, "points": compute_forecast_points(vessel_class, horizon_days, step_days)}


# ---------------------------------------------------------------------------
# Vessel / port recommendation (kept for backward compatibility)
# ---------------------------------------------------------------------------

class RecommendationRequest(BaseModel):
    origin: str
    destination: str
    cargo_mt: float
    contract_term: str = "Spot"


def pick_vessel(destination: str, cargo_mt: float) -> str:
    port = PORTS[destination]
    candidates = [v for v, spec in VESSEL_CLASSES.items() if spec["loa_max"] <= port["loa_max"] + 15]
    if not candidates:
        candidates = list(VESSEL_CLASSES.keys())
    return min(candidates, key=lambda v: abs(VESSEL_CLASSES[v]["dwt_mid"] - cargo_mt))


def clamp_cargo(cargo_mt: float) -> float:
    """No single dry-bulk vessel carries more than ~200,000 MT. Clamp anything
    outside a realistic range rather than let it produce nonsense downstream."""
    return max(5000.0, min(cargo_mt, 200000.0))


@app.post("/api/recommend")
def recommend(req: RecommendationRequest):
    port = PORTS.get(req.destination)
    if not port:
        return {"error": f"unknown destination port '{req.destination}'"}
    best = pick_vessel(req.destination, clamp_cargo(req.cargo_mt))
    rate = round(live_rates[best], 2)
    congestion_flag = port["congestion"] > 60
    return {
        "recommended_vessel": best,
        "dwt_class": VESSEL_CLASSES[best]["dwt_mid"],
        "indicative_rate_usd_per_tonne": rate,
        "destination_draft_m": port["draft"],
        "destination_loa_max_m": port["loa_max"],
        "congestion_pct": port["congestion"],
        "congestion_flag": congestion_flag,
        "note": (
            f"{req.destination} congestion is elevated ({port['congestion']}%) — factor in extra turnaround / idle time."
            if congestion_flag else f"{req.destination} congestion is within normal range."
        ),
    }


@app.get("/api/ports")
def get_ports():
    return PORTS


@app.get("/api/origins")
def get_origins():
    return ORIGIN_INFO


# ---------------------------------------------------------------------------
# LIVE: Weather + voyage risk (Open-Meteo — free, no key)
# ---------------------------------------------------------------------------

async def fetch_weather_raw(lat: float, lon: float) -> dict:
    url = "https://api.open-meteo.com/v1/forecast"
    params = {"latitude": lat, "longitude": lon, "current": "temperature_2m,wind_speed_10m,precipitation", "timezone": "auto"}
    async with httpx.AsyncClient(timeout=12, headers={"User-Agent": "PRAVAH/1.0"}) as client:
        r = await client.get(url, params=params)
        r.raise_for_status()
        return r.json()


def risk_from_weather(wind_kmh: float, precip_mm: float) -> dict:
    score = 0
    if wind_kmh > 40: score += 50
    elif wind_kmh > 25: score += 25
    elif wind_kmh > 15: score += 10
    if precip_mm > 10: score += 30
    elif precip_mm > 2: score += 10
    score = min(100, score)
    level = "High" if score >= 55 else "Moderate" if score >= 20 else "Low"
    return {"risk_score": score, "risk_level": level}


@app.get("/api/weather")
async def get_weather(location: str = Query(..., description="Port name or origin country name")):
    coords = None
    if location in PORTS:
        coords = PORTS[location]
    elif location in ORIGIN_INFO:
        coords = ORIGIN_INFO[location]
    if not coords:
        return {"error": f"unknown location '{location}'"}
    try:
        raw = await fetch_weather_raw(coords["lat"], coords["lon"])
        cur = raw.get("current", {})
        wind = cur.get("wind_speed_10m", 0.0)
        precip = cur.get("precipitation", 0.0)
        temp = cur.get("temperature_2m", None)
        risk = risk_from_weather(wind, precip)
        return {
            "location": location, "source": "open-meteo (live)",
            "temperature_c": temp, "wind_speed_kmh": wind, "precipitation_mm": precip,
            **risk,
        }
    except Exception as e:
        print(f"[PRAVAH] weather fetch FAILED for '{location}': {type(e).__name__}: {e}")
        return {
            "location": location, "source": "fallback (open-meteo unreachable)",
            "temperature_c": 28.0, "wind_speed_kmh": 12.0, "precipitation_mm": 0.5,
            "risk_score": 10, "risk_level": "Low", "error": str(e),
        }


# ---------------------------------------------------------------------------
# LIVE: FX rates (Frankfurter / ECB — free, no key)
# ---------------------------------------------------------------------------

FX_TARGETS = "INR,AUD,IDR,EUR"

async def fetch_fx_raw() -> dict:
    week_ago = (datetime.utcnow() - timedelta(days=7)).date().isoformat()
    # Frankfurter migrated from api.frankfurter.app to api.frankfurter.dev/v1/
    # (the old domain now 301-redirects there) — use the new one directly,
    # and follow_redirects=True as a defensive fallback in case it moves again.
    async with httpx.AsyncClient(timeout=12, headers={"User-Agent": "PRAVAH/1.0"}, follow_redirects=True) as client:
        latest = await client.get("https://api.frankfurter.dev/v1/latest", params={"from": "USD", "to": FX_TARGETS})
        latest.raise_for_status()
        hist = await client.get(f"https://api.frankfurter.dev/v1/{week_ago}", params={"from": "USD", "to": FX_TARGETS})
        hist.raise_for_status()
        return {"latest": latest.json(), "hist": hist.json(), "week_ago": week_ago}


@app.get("/api/fx")
async def get_fx():
    try:
        data = await fetch_fx_raw()
        latest_rates = data["latest"]["rates"]
        hist_rates = data["hist"]["rates"]
        inr_now, inr_then = latest_rates["INR"], hist_rates["INR"]
        trend_pct = round((inr_now - inr_then) / inr_then * 100, 3)
        return {
            "base": "USD", "source": "frankfurter.dev (live ECB rates)",
            "as_of": data["latest"]["date"], "rates": latest_rates,
            "week_ago_date": data["week_ago"], "week_ago_rates": hist_rates,
            "usd_inr_trend_pct_7d": trend_pct,
            "trend_direction": "INR weakening" if trend_pct > 0 else "INR strengthening" if trend_pct < 0 else "flat",
        }
    except Exception as e:
        print(f"[PRAVAH] FX fetch FAILED: {type(e).__name__}: {e}")
        return {
            "base": "USD", "source": "fallback (frankfurter unreachable)",
            "rates": {"INR": 87.5, "AUD": 1.52, "IDR": 16250.0, "EUR": 0.92},
            "usd_inr_trend_pct_7d": 0.0, "trend_direction": "flat", "error": str(e),
        }


# ---------------------------------------------------------------------------
# SIMULATED: Commodities & inflation (no reliable free no-key live tick feed
# exists for coal/iron-ore — label clearly, swap for World Bank / a paid
# provider in production)
# ---------------------------------------------------------------------------

commodity_state = {"Thermal Coal (USD/t)": 118.0, "Iron Ore 62% Fe (USD/t)": 102.0, "India CPI Inflation (YoY %)": 4.9}
commodity_history: Dict[str, List[float]] = {k: [v] * 20 for k, v in commodity_state.items()}


@app.get("/api/commodities")
def get_commodities():
    for k, v in commodity_state.items():
        drift = (random.random() - 0.5) * 0.02 * v
        commodity_state[k] = round(max(0.1, v + drift), 2)
        commodity_history[k].append(commodity_state[k])
        if len(commodity_history[k]) > 40:
            commodity_history[k].pop(0)
    return {"source": "calibrated simulation — replace with World Bank/paid feed for production", "values": commodity_state, "history": commodity_history}


# ---------------------------------------------------------------------------
# Cost breakdown
# ---------------------------------------------------------------------------

def build_cost_breakdown(vessel: str, cargo_mt: float, rate_usd_per_t: float, usd_inr: float) -> dict:
    freight_usd = rate_usd_per_t * cargo_mt
    port_charges_usd = cargo_mt * 3.2
    bunker_adj_usd = freight_usd * 0.05
    agency_fees_usd = 15000.0
    total_usd = freight_usd + port_charges_usd + bunker_adj_usd + agency_fees_usd
    return {
        "vessel": vessel, "cargo_mt": cargo_mt, "rate_usd_per_t": round(rate_usd_per_t, 2),
        "freight_usd": round(freight_usd, 2), "port_charges_usd": round(port_charges_usd, 2),
        "bunker_adjustment_usd": round(bunker_adj_usd, 2), "agency_fees_usd": agency_fees_usd,
        "total_usd": round(total_usd, 2), "usd_inr_rate": round(usd_inr, 4),
        "total_inr": round(total_usd * usd_inr, 2),
    }


@app.post("/api/cost-breakdown")
async def cost_breakdown(req: RecommendationRequest):
    port = PORTS.get(req.destination)
    if not port:
        return {"error": f"unknown destination port '{req.destination}'"}
    origin_info = ORIGIN_INFO.get(req.origin, {"distance_factor": 1.0})
    cargo_mt = clamp_cargo(req.cargo_mt)
    best = pick_vessel(req.destination, cargo_mt)
    rate = live_rates[best] * origin_info["distance_factor"]
    fx = await get_fx()
    usd_inr = fx["rates"]["INR"]
    return build_cost_breakdown(best, cargo_mt, rate, usd_inr)


# ---------------------------------------------------------------------------
# AI Decision Engine — the core feature. Deliberately transparent: every
# factor that moves the recommendation is named in the rationale, so it's
# an explainable decision-support system rather than a black box.
# ---------------------------------------------------------------------------

_cache: Dict[str, tuple] = {}  # key -> (expires_at, value)
CACHE_TTL_SECONDS = 45

async def cached(key: str, coro_fn):
    now = datetime.utcnow().timestamp()
    hit = _cache.get(key)
    if hit and hit[0] > now:
        return hit[1]
    value = await coro_fn()
    _cache[key] = (now + CACHE_TTL_SECONDS, value)
    return value


def score_decision(origin: str, destination: str, cargo_mt: float, weather: dict, fx: dict) -> dict:
    """Pure scoring logic — no I/O. Takes already-fetched weather/fx so callers
    (e.g. the strategy comparator) can fetch those once and reuse them across
    multiple origins instead of re-fetching per origin."""
    port = PORTS[destination]
    origin_info = ORIGIN_INFO[origin]
    best = pick_vessel(destination, cargo_mt)
    rate = live_rates[best] * origin_info["distance_factor"]
    pts = compute_forecast_points(best, horizon_days=60, step_days=10)
    trend = pts[-1]["mid"] - pts[0]["mid"]

    distance_nm = round(haversine_nm(origin_info["lat"], origin_info["lon"], port["lat"], port["lon"]), 0)
    transit_days = round(distance_nm / (VESSEL_SPEED_KNOTS[best] * 24), 1)

    score, reasons = 0, []
    if trend > 0:
        score -= 1
        reasons.append(f"Forecast shows rates trending up (+{trend:.2f} $/t over 60 days) — locking in now avoids further increases.")
    else:
        score += 1
        reasons.append(f"Forecast shows rates trending down ({trend:.2f} $/t over 60 days) — some room to wait for a better entry point.")

    if port["congestion"] > 60:
        score -= 1
        reasons.append(f"{destination} congestion is elevated at {port['congestion']}% — expect extra turnaround time.")
    else:
        reasons.append(f"{destination} congestion is normal at {port['congestion']}%.")

    if weather["risk_level"] == "High":
        score -= 2
        reasons.append(f"High voyage weather risk near {destination} (wind {weather['wind_speed_kmh']} km/h) — build in a schedule buffer.")
    elif weather["risk_level"] == "Moderate":
        score -= 1
        reasons.append(f"Moderate weather risk near {destination} (wind {weather['wind_speed_kmh']} km/h).")
    else:
        reasons.append(f"Weather conditions near {destination} are currently favourable.")

    fx_trend = fx.get("usd_inr_trend_pct_7d", 0.0)
    if fx_trend > 0.3:
        score -= 1
        reasons.append(f"INR has weakened {fx_trend}% vs USD over the last week — landed cost in INR is rising; consider fixing the rate now.")
    elif fx_trend < -0.3:
        score += 1
        reasons.append(f"INR has strengthened {abs(fx_trend)}% vs USD over the last week — landed cost in INR is easing.")
    else:
        reasons.append("USD/INR has been broadly stable over the last week.")

    action = "BOOK NOW" if score <= -2 else "MONITOR / WAIT" if score >= 1 else "MONITOR CLOSELY"
    confidence = max(50, min(95, 70 - score * 6))
    delivery_probability = max(55, min(98, 92 - weather["risk_score"] * 0.3 - port["congestion"] * 0.2))
    cost = build_cost_breakdown(best, cargo_mt, rate, fx["rates"]["INR"])

    return {
        "recommended_vessel": best,
        "action": action,
        "confidence_pct": round(confidence, 1),
        "delivery_probability_pct": round(delivery_probability, 1),
        "rationale": reasons,
        "forecast_trend_usd_per_t_60d": round(trend, 2),
        "distance_nm": distance_nm,
        "transit_days": transit_days,
        "weather": weather,
        "fx": fx,
        "cost": cost,
    }


@app.post("/api/decision")
async def decision_engine(req: RecommendationRequest):
    port = PORTS.get(req.destination)
    origin_info = ORIGIN_INFO.get(req.origin)
    if not port or not origin_info:
        return {"error": "unknown origin or destination"}

    cargo_mt = clamp_cargo(req.cargo_mt)
    weather, fx = await asyncio.gather(
        cached(f"weather:{req.destination}", lambda: get_weather(req.destination)),
        cached("fx", get_fx),
    )
    return score_decision(req.origin, req.destination, cargo_mt, weather, fx)


# ---------------------------------------------------------------------------
# Strategy selector — same scoring logic, run across every origin. Weather
# (destination-based) and FX are fetched ONCE and reused across origins,
# instead of re-fetched per origin — this is what makes the comparison fast.
# ---------------------------------------------------------------------------

class StrategyRequest(BaseModel):
    destination: str
    cargo_mt: float
    contract_term: str = "Spot"


@app.post("/api/strategy-compare")
async def strategy_compare(req: StrategyRequest):
    if req.destination not in PORTS:
        return {"error": f"unknown destination port '{req.destination}'"}
    cargo_mt = clamp_cargo(req.cargo_mt)

    weather, fx = await asyncio.gather(
        cached(f"weather:{req.destination}", lambda: get_weather(req.destination)),
        cached("fx", get_fx),
    )

    results = []
    for origin in ORIGIN_INFO:
        d = score_decision(origin, req.destination, cargo_mt, weather, fx)
        results.append({
            "origin": origin,
            "vessel": d["recommended_vessel"],
            "action": d["action"],
            "total_cost_usd": d["cost"]["total_usd"],
            "total_cost_inr": d["cost"]["total_inr"],
            "delivery_probability_pct": d["delivery_probability_pct"],
            "weather_risk": d["weather"]["risk_level"],
        })
    results.sort(key=lambda r: r["total_cost_usd"])
    return {"destination": req.destination, "cargo_mt": req.cargo_mt, "options": results}


# ---------------------------------------------------------------------------
# Idle & Repositioning Advisor — addresses "idle scenario management" from
# the original problem statement: once a vessel discharges and goes idle at
# an Indian port, where should it reposition to next to minimise ballast
# (empty-running) time while maximising its next-voyage earning potential?
#
# This is real math (haversine great-circle distance + live rate data), not
# a black box: net_score = expected next-voyage revenue - ballast fuel cost.
# ---------------------------------------------------------------------------

class IdleAdvisorRequest(BaseModel):
    current_location: str  # a destination port name (where the vessel just discharged)
    vessel_class: str


@app.post("/api/idle-advisor")
def idle_advisor(req: IdleAdvisorRequest):
    port = PORTS.get(req.current_location)
    if not port:
        return {"error": f"unknown current_location '{req.current_location}' — expected one of {list(PORTS.keys())}"}
    if req.vessel_class not in VESSEL_CLASSES:
        return {"error": f"unknown vessel_class '{req.vessel_class}'"}

    speed = VESSEL_SPEED_KNOTS[req.vessel_class]
    dwt = VESSEL_CLASSES[req.vessel_class]["dwt_mid"]
    base_rate = live_rates[req.vessel_class]

    options = []
    for origin, info in ORIGIN_INFO.items():
        distance_nm = round(haversine_nm(port["lat"], port["lon"], info["lat"], info["lon"]), 0)
        ballast_days = round(distance_nm / (speed * 24), 1)
        ballast_cost_usd = round(distance_nm * BALLAST_FUEL_COST_PER_NM_USD, 2)
        next_rate = base_rate * info["distance_factor"]
        expected_revenue_usd = round(next_rate * dwt, 2)
        net_score = round(expected_revenue_usd - ballast_cost_usd, 2)
        options.append({
            "origin": origin,
            "distance_nm": distance_nm,
            "ballast_days": ballast_days,
            "ballast_cost_usd": ballast_cost_usd,
            "next_voyage_rate_usd_per_t": round(next_rate, 2),
            "expected_revenue_usd": expected_revenue_usd,
            "net_score_usd": net_score,
        })
    options.sort(key=lambda o: o["net_score_usd"], reverse=True)
    best = options[0]

    return {
        "current_location": req.current_location,
        "vessel_class": req.vessel_class,
        "recommended_origin": best["origin"],
        "rationale": (
            f"Repositioning to {best['origin']} costs an estimated {best['ballast_days']} days "
            f"({best['distance_nm']:,.0f} nm) and ${best['ballast_cost_usd']:,.0f} in ballast fuel, "
            f"against an expected next-voyage revenue of ${best['expected_revenue_usd']:,.0f} at the current "
            f"{req.vessel_class} rate — the best net position of the {len(options)} options considered."
        ),
        "options": options,
    }


# ---------------------------------------------------------------------------
# Downloadable reports
# ---------------------------------------------------------------------------

async def build_report_data(origin: str, destination: str, cargo_mt: float, contract_term: str) -> dict:
    return await decision_engine(RecommendationRequest(origin=origin, destination=destination, cargo_mt=cargo_mt, contract_term=contract_term))


def format_report_text(origin, destination, cargo_mt, d: dict) -> str:
    lines = [
        "PRAVAH — Predictive Routing And Voyage Analytics Hub",
        "Voyage Decision Report",
        f"Generated: {datetime.utcnow().isoformat()}Z",
        "-" * 60,
        f"Route: {origin} -> {destination}",
        f"Cargo: {cargo_mt:,.0f} MT",
        f"Recommended vessel: {d['recommended_vessel']}",
        f"Action: {d['action']}  (confidence {d['confidence_pct']}%)",
        f"Estimated delivery probability: {d['delivery_probability_pct']}%",
        "",
        "Rationale:",
    ] + [f"  - {r}" for r in d["rationale"]] + [
        "",
        "Cost breakdown:",
        f"  Freight:          USD {d['cost']['freight_usd']:,.2f}",
        f"  Port charges:     USD {d['cost']['port_charges_usd']:,.2f}",
        f"  Bunker adj.:      USD {d['cost']['bunker_adjustment_usd']:,.2f}",
        f"  Agency fees:      USD {d['cost']['agency_fees_usd']:,.2f}",
        f"  TOTAL:            USD {d['cost']['total_usd']:,.2f}  (INR {d['cost']['total_inr']:,.0f})",
        "",
        f"Weather at destination: {d['weather']['temperature_c']}C, wind {d['weather']['wind_speed_kmh']} km/h, risk: {d['weather']['risk_level']} (source: {d['weather']['source']})",
        f"USD/INR: {d['fx']['rates']['INR']} (7-day trend: {d['fx'].get('usd_inr_trend_pct_7d', 'n/a')}%, source: {d['fx']['source']})",
    ]
    return "\n".join(lines)


@app.get("/api/report/txt")
async def report_txt(origin: str, destination: str, cargo_mt: float, contract_term: str = "Spot"):
    d = await build_report_data(origin, destination, cargo_mt, contract_term)
    if "error" in d:
        return PlainTextResponse(d["error"], status_code=400)
    text = format_report_text(origin, destination, cargo_mt, d)
    return PlainTextResponse(text, headers={"Content-Disposition": "attachment; filename=pravah_report.txt"})


@app.get("/api/report/pdf")
async def report_pdf(origin: str, destination: str, cargo_mt: float, contract_term: str = "Spot"):
    d = await build_report_data(origin, destination, cargo_mt, contract_term)
    if "error" in d:
        return PlainTextResponse(d["error"], status_code=400)

    from reportlab.lib.pagesizes import A4
    from reportlab.lib.styles import getSampleStyleSheet
    from reportlab.lib import colors
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle

    buf = BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4, title="PRAVAH Voyage Decision Report")
    styles = getSampleStyleSheet()
    story = [
        Paragraph("PRAVAH — Predictive Routing And Voyage Analytics Hub", styles["Title"]),
        Paragraph("Voyage Decision Report", styles["Heading2"]),
        Paragraph(f"Generated: {datetime.utcnow().isoformat()}Z", styles["Normal"]),
        Spacer(1, 12),
        Paragraph(f"<b>Route:</b> {origin} → {destination}", styles["Normal"]),
        Paragraph(f"<b>Cargo:</b> {cargo_mt:,.0f} MT", styles["Normal"]),
        Paragraph(f"<b>Recommended vessel:</b> {d['recommended_vessel']}", styles["Normal"]),
        Paragraph(f"<b>Action:</b> {d['action']} (confidence {d['confidence_pct']}%)", styles["Normal"]),
        Paragraph(f"<b>Delivery probability:</b> {d['delivery_probability_pct']}%", styles["Normal"]),
        Spacer(1, 12),
        Paragraph("Rationale", styles["Heading3"]),
    ]
    for r in d["rationale"]:
        story.append(Paragraph(f"• {r}", styles["Normal"]))

    story.append(Spacer(1, 12))
    story.append(Paragraph("Cost Breakdown", styles["Heading3"]))
    cost = d["cost"]
    table_data = [
        ["Item", "USD"],
        ["Freight", f"{cost['freight_usd']:,.2f}"],
        ["Port charges", f"{cost['port_charges_usd']:,.2f}"],
        ["Bunker adjustment", f"{cost['bunker_adjustment_usd']:,.2f}"],
        ["Agency fees", f"{cost['agency_fees_usd']:,.2f}"],
        ["TOTAL", f"{cost['total_usd']:,.2f}"],
        ["TOTAL (INR)", f"{cost['total_inr']:,.0f}"],
    ]
    t = Table(table_data, colWidths=[250, 150])
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#111412")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, -2), (-1, -1), "Helvetica-Bold"),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
    ]))
    story.append(t)

    story.append(Spacer(1, 12))
    story.append(Paragraph("Live Signals", styles["Heading3"]))
    story.append(Paragraph(
        f"Weather at destination: {d['weather']['temperature_c']}°C, wind {d['weather']['wind_speed_kmh']} km/h, "
        f"risk: {d['weather']['risk_level']} (source: {d['weather']['source']})", styles["Normal"]))
    story.append(Paragraph(
        f"USD/INR: {d['fx']['rates']['INR']} — 7-day trend {d['fx'].get('usd_inr_trend_pct_7d', 'n/a')}% "
        f"(source: {d['fx']['source']})", styles["Normal"]))

    doc.build(story)
    buf.seek(0)
    return StreamingResponse(buf, media_type="application/pdf", headers={"Content-Disposition": "attachment; filename=pravah_report.pdf"})


@app.get("/")
def root():
    """Serves the dashboard itself, so the whole app lives at one URL with
    no separate frontend host and no cross-origin requests needed."""
    if FRONTEND_FILE.exists():
        return FileResponse(FRONTEND_FILE)
    return {"status": "ok", "service": "pravah-api", "note": "pravah_terminal.html not found next to main.py"}


@app.get("/api/health")
def health():
    return {"status": "ok", "service": "pravah-api"}

# PRAVAH — Predictive Routing And Voyage Analytics Hub (backend)

FastAPI + WebSocket server behind the terminal dashboard.

## Run it

```bash
cd backend
pip install -r requirements.txt --break-system-packages   # or use a venv
uvicorn main:app --reload --port 8000
AISSTREAM_API_KEY=fddc6492269ad6a9c6484023d9bd3af87d9b7145 uvicorn main:app --reload --port 8000
```

Check it's alive: open http://localhost:8000/ — you should see `{"status":"ok",...}`.

## Endpoints

- `WS /ws/rates` — pushes a JSON tick every ~1.8s: `{type, timestamp, rates: {Handysize, Supramax, Panamax, Capesize}}`
- `GET /api/forecast?route=...&vessel_class=...` — forecast points with confidence band (calibrated simulation)
- `POST /api/recommend` — legacy simple vessel recommendation (kept for backward compatibility)
- `GET /api/ports` / `GET /api/origins` — reference data with coordinates, used by the map
- `GET /api/weather?location=Paradip` — **live** weather + computed voyage risk (Open-Meteo, no key)
- `GET /api/fx` — **live** USD→INR/AUD/IDR/EUR rates + real 7-day trend (Frankfurter/ECB, no key)
- `GET /api/commodities` — coal/iron-ore/inflation panel (calibrated simulation, clearly labelled)
- `POST /api/cost-breakdown` — itemised freight + port + bunker + agency cost, converted to INR at live FX
- `POST /api/decision` — **the AI Decision Engine**: combines forecast trend, port congestion, live weather risk, and live FX trend into one recommended vessel + action (BOOK NOW / MONITOR / WAIT) + a plain-language rationale list + confidence + delivery probability
- `POST /api/strategy-compare` — runs the decision engine across every origin for a fixed destination + cargo size, ranked by landed cost — this is the "same material, different routes" comparator
- `GET /api/report/txt` / `GET /api/report/pdf` — downloadable voyage decision report (reportlab)

## Live vessel tracking (AISStream.io)

The map can show **real, live vessel positions** — actual ships, actual AIS data, not simulated — using [AISStream.io](https://aisstream.io), which is free but requires a free account:

1. Sign up at https://aisstream.io and grab your API key.
2. Run the backend with the key set as an environment variable:
   ```bash
   AISSTREAM_API_KEY=your-key-here uvicorn main:app --reload --port 8000
   ```
   (On Windows PowerShell: `$env:AISSTREAM_API_KEY="your-key-here"; uvicorn main:app --reload --port 8000`)
3. Open the dashboard — orange triangle markers on the map are live ships within the tracked shipping lanes (Mozambique/Australia/Indonesia → India), updating in real time as AIS reports come in.

If you don't set the key, this feature is silently skipped — the map still works, just without ship markers. AISStream's exact JSON schema may change over time; if ships stop appearing, check https://aisstream.io/documentation and compare against the field names in `ais_stream_loop()` in `main.py`.

## What's actually real vs. modelled — be upfront about this with judges

| Data | Real or model | Source |
|---|---|---|
| Weather + voyage risk | **Real**, live | Open-Meteo |
| FX rates + 7-day trend | **Real**, live | Frankfurter/ECB |
| Vessel positions on map | **Real**, live | AISStream.io (needs your free API key) |
| Oil price + dry-bulk shipping stocks | **Real**, live | Stooq |
| Freight rates (per vessel class) | Calibrated model, **live-adjusted** by the real oil/equity signals above | — |
| Commodities (coal, iron ore, inflation) | Calibrated simulation | — |
| Port draft/LOA/berth data | Real reference figures, but static — verify against current port-authority circulars before presenting as fact | — |

The one thing that's genuinely impossible to get for free: the actual Baltic Exchange freight indices (Handysize/Supramax/Panamax/Capesize $/tonne rates) are licensed data — Baltic Exchange, Clarksons, and Drewry all charge for API access. There's no legal free live feed for that specific number, which is why it's a model rather than a raw feed.



`pravah_terminal.html` checks `http://localhost:8000/` every 15s:
- **Backend reachable** → weather, FX ticker, commodities, cost breakdown, map route, AI decision engine, strategy comparator, and PDF/TXT downloads all go live and refresh automatically.
- **Backend offline** → those panels show a clear "backend offline" note instead of faking data; the freight ticker, port table, and forecast chart keep working from their built-in local simulation so the page is never blank.

No code edit is needed to connect them — just have both running. If you host the backend somewhere other than `localhost:8000`, change the `API_BASE` constant near the top of the `<script>` block in `pravah_terminal.html`.


## Turning the simulation into real data (priority order for the hackathon)

1. **Port infrastructure data** — already static/real in `PORTS`. Verify current draft/LOA/berth figures against the latest circulars from each port authority / Indian Ports Association before presenting — these change periodically (dredging, new berths).
2. **Freight rate proxy** — replace `step_rates()` with a scheduled poll (APScheduler) of a public Baltic Dry Index or Baltic sub-index feed, scaled to route/vessel-class level using historical ratios you compute offline. This gets you a *real macro signal* even without route-level broker data.
3. **Commodity prices** — pull thermal coal / iron ore benchmark prices from a free public API (e.g. a commodities data provider with a free tier) as an input feature to the forecast model.
4. **Forecast model** — replace `forecast()` with a trained Prophet or XGBoost model loaded from a pickle file, using the features above. Keep the same response shape so the frontend doesn't change.
5. **AIS / congestion** — if you get access to a free-tier AIS API, use it to refine the `congestion` field in `PORTS`; otherwise keep it as a manually-updated estimate and say so in the demo.

Being upfront with judges about which panel is "live real data" vs "live simulated, real data source identified for production" is a stronger position than overclaiming — see the notes in the main conversation on why this typically scores better with SIH judges.

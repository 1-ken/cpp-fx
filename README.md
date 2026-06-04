# cTrader Plus (C++ / Drogon)

A C++ rewrite of the Python `curencies/` Finance Observer. It exposes the **same
HTTP + WebSocket API**, but sources live prices and historical OHLC from the
**cTrader Open API** (Protobuf over TLS) instead of scraping websites with
Playwright.

## What changed vs. the Python app


| Concern        | Python (`curencies/`)                        | This project                                                          |
| -------------- | -------------------------------------------- | --------------------------------------------------------------------- |
| Market data    | Playwright scraping Yahoo / TradingEconomics | cTrader Open API (TLS Protobuf)                                       |
| Live prices    | DOM snapshots every ~0.5s                    | `ProtoOASpotEvent` (bid/ask)                                          |
| History / OHLC | Aggregated from archived ticks in PostgreSQL | `ProtoOAGetTrendbarsReq` (real broker trend bars)                     |
| Symbols        | Hard-coded scrape targets                    | All broker symbols via `ProtoOASymbolsListReq`, grouped heuristically |
| Web framework  | FastAPI + uvicorn                            | Drogon                                                                |
| Auth           | NextAuth HS256 JWT                           | Same (jwt-cpp)                                                        |
| Storage        | Redis + PostgreSQL                           | Same                                                                  |
| Notifications  | SendGrid / Africa's Talking / Twilio         | Same (via Drogon HttpClient)                                          |


Prices from cTrader are scaled by `1/100000`. Daily change % is derived from the
spot event `sessionClose`. Trend bar prices are reconstructed as
`open = low + deltaOpen`, `high = low + deltaHigh`, `close = low + deltaClose`.

## Endpoints (same contracts as the Python app)

- `GET /health`, `GET /ping`, `GET /dashboard`, `GET /client-config`
- `GET /snapshot` — grouped `{currencies, commodities}`
- `GET /me`, `POST /onboarding/complete` — PostgreSQL user state
- `GET /stream-health`
- `WS /ws/observe?access_token=…&interval=1m&pair=EURUSD`
- `GET /historical` — raw ticks (PostgreSQL archive)
- `GET /historical/ohlc` — cTrader trend bars
- `GET /historical/ohlc-with-forming` — closed trend bars + forming candle
- `GET /historical/stream-metrics`
- `POST/GET /api/v1/alerts`, `GET/PUT/DELETE /api/v1/alerts/{id}`

## Prerequisites

- CMake ≥ 3.16, a C++17 compiler
- [Drogon](https://github.com/drogonframework/drogon) built with **PostgreSQL**
and **Redis** support (`-DBUILD_POSTGRESQL=ON -DBUILD_REDIS=ON`)
- Protobuf compiler + runtime (`protoc`, `libprotobuf`)
- OpenSSL
- [jwt-cpp](https://github.com/Thalhammer/jwt-cpp) (header-only). Install
system-wide, or vendor it at `third_party/jwt-cpp/include`.
- A running PostgreSQL and Redis instance
- A cTrader account + Open API application (clientId / clientSecret) and an
OAuth2 access token for the trading account (ctidTraderAccountId)

### Installing dependencies (Ubuntu example)

```bash
sudo apt-get update
sudo apt-get install -y cmake g++ protobuf-compiler libprotobuf-dev \
  libssl-dev libjsoncpp-dev libpq-dev libhiredis-dev uuid-dev zlib1g-dev
# Drogon (build from source with postgres + redis):
git clone --recurse-submodules https://github.com/drogonframework/drogon
cd drogon && mkdir build && cd build
cmake .. -DBUILD_POSTGRESQL=ON -DBUILD_REDIS=ON
make -j && sudo make install
# jwt-cpp (header-only):
git clone https://github.com/Thalhammer/jwt-cpp third_party/jwt-cpp
```

The four cTrader `.proto` files are already vendored in `proto/` (from
[spotware/openapi-proto-messages](https://github.com/spotware/openapi-proto-messages)).
CMake compiles them with `protoc` at build time.

## Build

```bash
cd ctraderplus-cpp
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
ctest --test-dir build      # runs the unit tests
```

## Configure

Copy `.env.example` to `.env` and fill in your credentials:

```bash
cp .env.example .env
```

Key variables:

- `CTRADER_CLIENT_ID`, `CTRADER_CLIENT_SECRET`, `CTRADER_ACCESS_TOKEN`,
`CTRADER_ACCOUNT_ID`, `CTRADER_HOST` (`live`/`demo`)
- `NEXTAUTH_SECRET` (or `AUTH_DISABLED=1` for local dev)
- `DATABASE_URL`, `REDIS_URL`
- Notification providers (optional)

`config.json` holds non-secret tuning (stream interval, retention, host names,
batch sizes).

## Run

```bash
./build/ctraderplus_server
# Dashboard: http://localhost:8000/dashboard
```

## Deploy (Nixpacks / Dokploy)

Set the service **root directory** to `ctraderplus-cpp/`. Nixpacks picks up
[`nixpacks.toml`](nixpacks.toml), which runs a **step-wise build** (Drogon fetch →
configure → compile → install → app lib → link) so each heavy step gets full CPU/RAM.
[`scripts/build-vps.sh`](scripts/build-vps.sh) sets `BUILD_JOBS` from available memory.
Production runtime tuning comes from [`config.production.json`](config.production.json)
(copied into the image at build time). The platform injects `PORT`; the app binds
`0.0.0.0:$PORT`.

### Standard B profile (4GB VPS: API + Postgres + Redis)

| Setting | Production value |
|---------|------------------|
| Symbols | All broker symbols (`subscribeAllSymbols: true`, `maxSubscribedSymbols: 0`) |
| HTTP threads | `2` |
| Notification workers | `2` |
| Stream interval | `2s` |
| Redis | Alert buffer, admin OTP, notification DLQ (`REDIS_PUBSUB_ENABLED=false`) |
| Postgres pool | `postgresConnNum: 2` |

Attach managed **PostgreSQL** and **Redis**, then set **runtime** environment
variables in Dokploy (not build-time Docker ARGs — avoids baking secrets into
image layers). Do not commit `.env`.

- **Required:** `CTRADER_CLIENT_ID`, `CTRADER_CLIENT_SECRET`,
  `CTRADER_ACCESS_TOKEN`, `CTRADER_ACCOUNT_ID`, `DATABASE_URL`, `REDIS_URL`,
  `NEXTAUTH_SECRET`, `AUTH_DISABLED=0`
- **Optional:** `CTRADER_HOST`, `CTRADER_REFRESH_TOKEN`, `WS_URL`,
  `API_BASE_URL`, `ADMIN_PHONE`, `CORS_ALLOW_ORIGIN`, notification provider vars
  (see [`.env.example`](.env.example))

**Alert notifications (SMS / email / call)** include pair, trigger type (`price` /
`candle_close`), custom message, and trigger time in **Kenya time (EAT)**.

**Dokploy / production URLs**

| Variable | Format | Example |
|----------|--------|---------|
| `DATABASE_URL` | `postgresql://user:pass@host:5432/db` (libpq; **not** `postgresql+asyncpg://`) | `postgresql://myuser:secret@fxalerts-commodities-xyqnzu:5432/commodities` |
| `REDIS_URL` | `redis://[user:password@]host:port[/db]` — use the **managed Redis hostname**, not an IP | `redis://default:secret@fxalerts-redis-bfgt7d:6379` |

The server resolves Redis hostnames via DNS at startup (same as cTrader). Keep
`fxalerts-redis-bfgt7d` (or your service name) in `REDIS_URL`; do not replace it
with a numeric IP. Logs should look like:
`Redis client created for fxalerts-redis-bfgt7d (10.x.x.x:6379/0) acl_user=default`.

On startup the server also runs idempotent Postgres migrations (adds `alerts.data`
JSONB and backfills from legacy Python flat columns).

**Build on a small VPS:** add 2GB swap before the first deploy. First build ~12–17
minutes step-wise; cached redeploys skip Drogon when already installed. Locally:

```bash
. ./scripts/build-vps.sh
cmake --build /tmp/drogon-build -j"$BUILD_JOBS"   # after Drogon configure
cmake --build build -j"$BUILD_JOBS" --target ctraderplus_lib
cmake --build build -j1 --target ctraderplus_server
```

**Post-deploy checks**

- `curl -sS http://localhost:8000/health` → 200
- Login latency under 1s
- `ss -tlnp | grep 8000` → one `ctraderplus_server`
- Logs show `threads=2` and full symbol subscription (~1600+ spots)
- Test SMS alert contains `PAIR`, `TYPE`, `MESSAGE`, and time ending in `EAT`
- `redis-cli LLEN fx:alerts:notifications:dlq` → 0 on success

**Resource limits (co-located 4GB VPS):** Postgres `shared_buffers=128MB`, Redis
`maxmemory 128mb`, API container `MemoryMax=1280M` optional.

## Architecture

```
cTrader (TLS Protobuf) --> CTraderClient --> MarketHub --> WebSocket /ws/observe
                                  |              |--------> Redis (cache/queue)
                                  |              |--------> Alert engine -> Email/SMS/Call
                                  |              \--------> PostgreSQL (tick archive, metrics)
                                  \--GetTrendbars--> /historical/ohlc(+forming)
```

- `src/ctrader/` — TLS Protobuf client, framing, auth, heartbeat, symbol registry.
- `src/market/MarketHub` — assembles snapshots, fans out, publishes, drives alerts.
- `src/services/` — PostgreSQL, Redis, Notifier.
- `src/alerts/` — price + candle-close alert engine.
- `src/controllers/` — HTTP routes + WebSocket controller + dashboard.

## Notes / limitations

- Candle-close alerts are evaluated by polling cTrader trend bars on a timer
(default every few seconds), replacing the Python PostgreSQL OHLC bucketing.
- The notification dispatcher attempts each send once and pushes failures to a
Redis DLQ; the multi-worker retry loop from the Python version is simplified.
- Access-token refresh (`ProtoOARefreshTokenReq`) is not yet automated; supply a
valid `CTRADER_ACCESS_TOKEN`.


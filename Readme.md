# Feed Bench

Four broker WebSocket feeds, one clock, one capture format.

Compares **Fyers**, **Dhan**, **Nubra**, and **Arrow** on the things that
actually differ: cadence, tick capture, depth, and stability. Raw capture goes
to disk at full rate; the UI is a throttled view of the same stream.

## Run

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env          # Nubra creds go here
python nubra_login.py         # ONCE, in a terminal — see below. Skip if not using Nubra.
python server.py              # -> http://127.0.0.1:8777
```

Enter credentials per broker in the left rail, paste instruments, hit **Start
feed**. Captures land in `captures/<date>/<provider>.jsonl`.

```bash
python analyze.py captures/2026-07-16 --ref fyers
```

## Credentials and instrument formats

| Broker | Auth | Instruments |
|---|---|---|
| Fyers | `access_token` as `APPID-100:eyJ0...` | `NSE:NIFTY25JULFUT` |
| Dhan | `client_id` + `access_token` | `NSE_FNO:43492` (SEGMENT:SECURITY_ID) |
| Nubra | `.env` + **terminal pre-login** (see below) | `69353` (ref_id) |
| Arrow | `ARROW_APP_ID` + `ARROW_ACCESS_TOKEN` in `.env` | `3045` (numeric token) |

**Every broker uses a different instrument identifier.** Until you build a
mapping table, the cross-feed comparison is measuring different things and
the tick-capture numbers are noise. This is the first real piece of work
after the harness runs.

## What the tests mean

**Inter-arrival** — the throttle detector. A p50 pinned near a round number
(250 / 500 / 1000ms) means the feed is polling on your behalf and calling it
streaming. A p50 that moves with market activity is a real stream. Validated
against a synthetic 1s-snapshot feed: it reported p50 = 1013ms while a real
stream showed 7ms.

**Tick capture** — distinct price changes vs the reference feed.

**1-min range shrinkage** — the cheapest and most damning test. A coalescing
feed misses spikes, so its bars are systematically *narrower*. On the
synthetic pair, the snapshot feed's ranges came out at 4.5% of the reference.
This failure is completely invisible on an LTP display and fatal to TCA.

**Depth levels** — what you actually receive, not what the docs claim.

## Arrow

```bash
python Arrow/arrow_websocket.py --dry-run             # resolve the universe, print it, exit
python Arrow/arrow_websocket.py --preflight           # also check credentials
python Arrow/arrow_websocket.py --strikes 15          # capture until 15:32 IST
python StartDailyCapture.py --with-arrow              # or run it alongside the others
```

Output lands in `Arrow/arrow_live_ticks_<date>.csv`, `Arrow/arrow_live_1s_<date>.csv`
and a DuckDB sidecar, in the same schema Dhan writes -- including the shared
`canonical_symbol`, so the dashboard ranks Arrow against the other feeds with no
special casing. Point the **Arrow CSV** field at the 1s file (or pick `Arrow` as
the reference) and every section works: decision comparison, receive-vs-exchange
timing, paired compare, row browser, AlphaBot matching.

Credentials go in `.env`:

```
ARROW_APP_ID=...
ARROW_ACCESS_TOKEN=...          # day-scoped; cached in Arrow/access_token.txt
# or, for unattended TOTP login instead of a pasted token:
ARROW_USER_ID=...
ARROW_PASSWORD=...
ARROW_API_SECRET=...
ARROW_TOTP_SECRET=...
```

The instrument master (`https://edge.arrow.trade/all`) is public CSV, so
`--dry-run` resolves the ATM-centred universe without credentials; only the live
socket and the ATM quote need them. Without a quote the ATM anchor falls back to
the midpoint of the nearest future's circuit bands, which is the previous close
-- fine before the open, worth overriding with `--atm` on a gapping day.

Two streams, same output rows, and they can run **at the same time** -- `hft`
writes `arrow_hft_*` files so the two never contend for a CSV or duckdb lock:

- `--stream ds` (default) -- `wss://ds.arrow.trade`, event-driven, 5+5 depth in
  `FULL` mode. Measured p50 ~4s between updates per contract (2026-09-09).
- `--stream hft` -- `wss://socket.arrow.trade`, subscribes at `--hft-latency` ms
  (50-60000). Read its inter-arrival p50 as the interval you asked for, not as
  the feed's cadence. NSEFO accepts `full` mode here despite the SDK docstring
  claiming LTP-only.

`hft` defaults to **CSV only**: batched duckdb inserts sustain ~1.3k ticks/sec
while the CSV alone does ~180k, and hft at 100ms x 65 symbols is ~650/sec --
too close to the duckdb ceiling. Build the sidecar afterwards with the
dashboard's *Create DuckDB Snapshots* button. Force it on with `--duckdb`.

Two Arrow quirks worth knowing:

- `E_INVALID_SYMID` / `E_SYMBOL_NOT_FOUND` on subscribe means an **expired or
  unknown contract**, not a permissions problem. Expect it after every weekly
  expiry, and outside market hours when the service unloads its symbol map.
- The full packet's `ts` field is **not** epoch time -- observed ~3.8e14ns, a
  monotonic counter. Listed time comes from `ltt` (epoch seconds); `ts` is kept
  in `raw_json` if you want relative sequencing.

Arrow's last-trade time has one-second resolution on the `ds` stream, the same
as Dhan and FYERS, so the receive-delay numbers are comparable across those
three. The `hft` stream's `ts` field is the one to use if you want finer than that.

## Nubra needs a terminal login first

`InitNubraSdk` is an interactive login. `env_creds=True` only supplies
`PHONE_NO` and `MPIN` from .env — the SDK still prompts stdin for the OTP or
TOTP code, and a stdin prompt inside the server's daemon thread cannot work.

```bash
python nubra_login.py            # OTP
python nubra_login.py --totp     # TOTP (set up with --setup-totp first)
python nubra_login.py --insti    # institutional
```

The SDK stores the session token and the collector reuses it — set Nubra's
login mode to `session` in the UI afterwards.

**If your firm has an institutional Nubra account, use `insti` instead.** It
reads CLIENT_CODE / EXCHANGE_CLIENT_CODE / USERNAME / PASSWORD / MPIN from
.env and never prompts. It's the only genuinely non-interactive path, which
matters the moment you want this running unattended.

## Known gaps, deliberately left visible

- **This measures SDK-callback-to-us, not wire-to-us.** Each collector goes
  through its vendor SDK and those differ in parse quality. Fine for ranking
  cadence, capture, and depth. For true lead-lag you want raw sockets on all
  four — worth doing once this tells you which two are even contenders.
- **Run it from where production runs.** Benchmarking from a laptop over
  office wifi means the ~20–30ms RTT to Mumbai and its jitter swamp every
  difference between providers, and you will conclude they're identical.
- **Fyers contradicts itself on the subscription cap.** The PyPI README says
  200 symbols; their support KB (Aug 2025) says 5000. At ~300 instruments you
  are exactly in the disputed range. The collector surfaces server-side
  rejections via `on_error_message` — watch it.
- **Dhan 20-level depth caps at 50 instruments/connection** against a
  5-connection ceiling, and their release notes describe it as "for all NSE
  instruments" — confirm BFO is carried before trusting any Sensex result.

## Test design

Two weeks minimum, covering **Tuesday (Nifty expiry)** and **Thursday (Sensex
expiry)** plus quiet Mondays and Fridays. Instrument set: Nifty and Sensex
weeklies ATM±10, both futures, both spots, plus deliberately far-OTM strikes —
throttling behaviour differs sharply with activity level.

Test BFO separately and explicitly. A feed that's great on NFO and mediocre on
BFO is useless given where the slippage is.

## Layout

```
core/bus.py         recv_ns stamping, raw capture, live stats
core/base.py        collector interface
collectors/*.py     one per broker
Arrow/arrow_websocket.py  Arrow capture (ds + hft streams)
server.py           FastAPI + 4Hz stats WebSocket
static/index.html   UI
analyze.py          offline comparison
```

The one rule the collectors follow: `emit()` is the first statement in every
socket callback. Parsing on the receive path injects jitter of the same order
as the differences you're trying to detect, and you end up benchmarking your
own JSON decoder.
## Quantick

Client: **qtickclient 3.3.0** (installed 2026-10-19 from `Quantick/qtickclient-3.3.0-py3-none-any.whl`;
3.1.0 wheel kept for rollback). 3.2.0+ sends "slim" ticks (`ltq`, `atp`, `tbq`, `tsq`; no `mode`,
`tradable` or `instrument_token`); the collector reads both old and new names and takes the
token from `instrument_info()`. 3.3.0 adds typed errors (`HistoricalAccessDeniedError`,
`InvalidRequestError`, `TokenWindowClosedError` for the 06:00-08:30 IST window, `RateLimitedError`).

Market data only (no order APIs). Its feed is Zerodha Kite data re-broadcast:
same instrument tokens and field names. The account is IP-whitelisted to the
`new-portal` server only, so the collector opens an SSH tunnel
through `new-portal` and every feed in the bench is stamped by this Mac's clock.

`.env`: `QUANTICK_USER`, `QUANTICK_PASSWORD`, `QUANTICK_BASE_URL`,
`QUANTICK_SSH_HOST=new-portal`, `QUANTICK_TUNNEL_PORT=18080`. The token expires at
01:00 IST daily; the collector logs in fresh on every start.

```bash
python Quantick/quantick_websocket.py --preflight          # tunnel + login + universe
python StartDailyCapture.py --keep-going --with-arrow --with-quantick --no-nubra
python Quantick/compare_feeds.py live --date YYYY-MM-DD     # after 15:30
python Quantick/compare_feeds.py eod  --date YYYY-MM-DD     # after ~19:00 (NSE F&O bhavcopy)
```

Universe: NIFTY ATM+/-15 nearest expiry + 3 futures + index (the contracts every
other collector captures), plus BANKNIFTY, NSE/BSE equities, SENSEX index/future/
options (BFO), MCX futures and CRUDEOIL options. It runs until 16:05 so cash
closing-session ticks are final. Output uses Arrow's schema and canonical keys
(`Quantick/quantick_live_1s_<date>.csv`, raw ticks, universe JSON).

`compare_feeds.py` writes `Quantick/reports/<date>/summary*.md` plus CSVs:

- live: cadence, backlog, LTP agreement (by exchange second and by receive
  second), lead/lag on price changes, 1-minute range shrinkage vs every other
  broker captured that day.
- eod: Quantick's day candles vs the official NSE/BSE bhavcopy, its minute-candle
  coverage, its minute candles vs every live capture, and its final live tick vs
  the bhavcopy (all exchanges; MCX has no free bhavcopy).

Known on 2026-10-15: equity minute history ended at 15:14 (day close/volume
wrong until backfilled; the 14 Oct candle was correct the next day), and F&O
history runs to 15:39 because post-close packets are folded in. Re-run `eod` the
next morning to see whether history was repaired.

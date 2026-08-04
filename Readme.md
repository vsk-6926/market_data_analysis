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
| Arrow | `app_id` + `access_token` | `3045` (numeric token) |

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
server.py           FastAPI + 4Hz stats WebSocket
static/index.html   UI
analyze.py          offline comparison
```

The one rule the collectors follow: `emit()` is the first statement in every
socket callback. Parsing on the receive path injects jitter of the same order
as the differences you're trying to detect, and you end up benchmarking your
own JSON decoder.
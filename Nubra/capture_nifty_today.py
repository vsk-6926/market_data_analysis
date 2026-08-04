#!/usr/bin/env python3
"""
capture_nifty_today.py  -  STANDALONE live capture of TODAY's NIFTY data.

Self-contained: does NOT import anything from the nubra_pipeline package.
Drop it anywhere, put a .env next to it, run it. It will keep polling until
market close and leave you one parquet per day, schema-compatible with the
pipeline's data_1m output (same 22 columns) so you can merge later.

WHAT IT CAPTURES
  - NIFTY futures  : the next N monthly expiries (default 3)
  - NIFTY options  : CE + PE, ATM +/- N strikes (default 15), for the next
                     N expiries (default 1 = the nearest weekly/monthly)
  - Interval 1s by default, so the output is usable for second-level comparison.
    1m is still available if you explicitly request it.

HOW IT WORKS
  1. md.option_chain("NIFTY") -> live ATM strike, strike step, all expiries.
     (No Upstox dump, no stale instrument master needed.)
  2. Builds Nubra symbols locally (weekly vs monthly formats).
  3. Every POLL_SECONDS it re-fetches the whole day so far with intraDay=True
     and rewrites the parquet atomically. Idempotent - a crash or restart
     loses nothing, the next cycle refills the full day.
  4. Stops shortly after 15:30 IST, or on Ctrl-C (writes final file either way).

SETUP
    pip install nubra-sdk pandas pyarrow python-dotenv requests
    # .env next to this script (or point --cred-dir at a folder with one):
    #   PHONE_NO=9xxxxxxxxx
    #   MPIN=xxxx
    # OTP is entered interactively on start.

RUN
    python capture_nifty_today.py                    # defaults to 1s capture
    python capture_nifty_today.py --strikes 25 --expiries 2 --poll 60
    python capture_nifty_today.py --interval 1m      # 1m: includes greeks/IV
    python capture_nifty_today.py --dry-run          # show universe, fetch nothing

RATE BUDGET (read before raising --strikes / --expiries)
    instruments = expiries * (2*strikes + 1) * 2  +  fut_months
    api_calls_per_cycle = ceil(instruments / 5)
    Cap is ~60 calls/min; this script self-throttles to 45. Defaults give
    1*31*2 + 3 = 65 instruments = 13 calls/cycle -> a cycle takes ~15s.
    Keep api_calls_per_cycle * (60/45) comfortably under --poll.
"""

import os
import sys
import math
import time
import argparse
import builtins
import datetime as dt
from datetime import timezone, timedelta
from collections import deque, defaultdict
from contextlib import contextmanager

import pandas as pd
import pyotp

from nubra_python_sdk.marketdata.market_data import MarketData
from nubra_python_sdk.start_sdk import InitNubraSdk, NubraEnv

# ===========================================================================
# Constants mirrored from the pipeline so output stays schema-compatible
# ===========================================================================
ENV = NubraEnv.PROD
EXCHANGE = "NSE"
UNDERLYING = "NIFTY"

IST = dt.timezone(timedelta(hours=5, minutes=30))
MARKET_OPEN_UTC = (3, 45)     # 09:15 IST
MARKET_CLOSE_UTC = (10, 0)    # 15:30 IST

FIELDS = ["open", "high", "low", "close",
          "l1bid", "l1ask",
          "cumulative_volume", "cumulative_volume_premium",
          "cumulative_oi", "cumulative_volume_delta",
          "theta", "delta", "gamma", "vega",
          "iv_bid", "iv_ask", "iv_mid"]
PRICE_FIELDS = {"open", "high", "low", "close", "l1bid", "l1ask"}

LABEL_COLS = ["datetime_IST", "received_at_IST", "symbol", "expiry", "strike", "instrument_type"]
CANONICAL_COLS = LABEL_COLS + FIELDS

MAX_INSTRUMENTS_PER_REQUEST = 5
MAX_CALLS_PER_MIN = 45
API_CALL_DELAY = 0.5
MAX_RETRIES = 4
RETRY_BACKOFF = [30, 60, 90, 120]

_MONTHS = {1: "JAN", 2: "FEB", 3: "MAR", 4: "APR", 5: "MAY", 6: "JUN",
           7: "JUL", 8: "AUG", 9: "SEP", 10: "OCT", 11: "NOV", 12: "DEC"}
_WEEKLY_MONTH = {1: "1", 2: "2", 3: "3", 4: "4", 5: "5", 6: "6",
                 7: "7", 8: "8", 9: "9", 10: "O", 11: "N", 12: "D"}


# ===========================================================================
# Small utilities
# ===========================================================================
class RateLimiter:
    """Sliding-window limiter: at most `max_calls` in any `period_s` window."""

    def __init__(self, max_calls, period_s=60.0):
        self.max_calls = max_calls
        self.period_s = period_s
        self.calls = deque()

    def wait(self):
        now = time.monotonic()
        while self.calls and now - self.calls[0] >= self.period_s:
            self.calls.popleft()
        if len(self.calls) >= self.max_calls:
            sleep_for = self.period_s - (now - self.calls[0]) + 0.05
            if sleep_for > 0:
                print(f"      [rate-limit] sleeping {sleep_for:.1f}s", flush=True)
                time.sleep(sleep_for)
            return self.wait()
        self.calls.append(time.monotonic())


def iso_utc(d, hm):
    return dt.datetime.combine(d, dt.time(hm[0], hm[1]), tzinfo=timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%S.000Z")


def iso_utc_dt(ts):
    return ts.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")


def now_ist():
    return dt.datetime.now(IST)


def rs(paise):
    """Nubra returns money AND strikes in paise."""
    return None if paise is None else paise / 100.0


def parse_expiry(v):
    """all_expiries entries: 'YYYYMMDD', 'YYYY-MM-DD', or epoch s/ms."""
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    if "-" in s:
        try:
            return dt.datetime.strptime(s[:10], "%Y-%m-%d").date()
        except ValueError:
            return None
    if s.isdigit():
        if len(s) == 8:
            try:
                return dt.datetime.strptime(s, "%Y%m%d").date()
            except ValueError:
                return None
        n = int(s)
        if n > 10_000_000_000:      # milliseconds
            n //= 1000
        try:
            return dt.datetime.fromtimestamp(n, tz=IST).date()
        except (ValueError, OSError, OverflowError):
            return None
    return None


def is_monthly(expiry, all_expiries):
    """Monthly = the LAST expiry inside its own calendar month."""
    same_month = [e for e in all_expiries
                  if e.year == expiry.year and e.month == expiry.month]
    return bool(same_month) and expiry == max(same_month)


def build_nubra_symbol(underlying, expiry, strike, instrument_type, weekly):
    """Exactly the pipeline's symbol grammar - keep these two in sync."""
    yy = f"{expiry.year % 100:02d}"
    it = (instrument_type or "").upper()
    if it == "FUT":
        return f"{underlying}{yy}{_MONTHS[expiry.month]}FUT"
    try:
        sval = float(strike)
        strike_str = str(int(sval)) if sval == int(sval) else str(sval)
    except (TypeError, ValueError):
        strike_str = str(strike)
    if weekly:
        return (f"{underlying}{yy}{_WEEKLY_MONTH[expiry.month]}"
                f"{expiry.day:02d}{strike_str}{it}")
    return f"{underlying}{yy}{_MONTHS[expiry.month]}{strike_str}{it}"


def chunks(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


def _current_totp_from_env():
    secret = (os.getenv("TOTP_SECRET") or "").strip()
    if not secret:
        return None
    return pyotp.TOTP(secret).now()


@contextmanager
def _auto_nubra_input():
    original_input = builtins.input

    def patched(prompt=""):
        msg = str(prompt)
        if "Enter TOTP" in msg:
            totp = _current_totp_from_env()
            if totp:
                print("  auto-supplying TOTP from TOTP_SECRET", flush=True)
                return totp
        return original_input(prompt)

    builtins.input = patched
    try:
        yield
    finally:
        builtins.input = original_input


def tsp_series(points, divide=1):
    if not points:
        return None
    idx = pd.to_datetime([p.timestamp for p in points],
                         unit="ns", utc=True).tz_convert("Asia/Kolkata")
    return pd.Series([(p.value / divide) if p.value is not None else None
                      for p in points], index=idx)


def market_open_ist(day):
    return dt.datetime.combine(day, dt.time(9, 15), tzinfo=IST)


def market_close_ist(day):
    return dt.datetime.combine(day, dt.time(15, 30), tzinfo=IST)


def current_fetch_window(day, now=None):
    now = now or now_ist()
    open_ts = market_open_ist(day)
    close_ts = market_close_ist(day)
    if now < open_ts:
        return None
    end_ts = min(now, close_ts)
    return iso_utc_dt(open_ts), iso_utc_dt(end_ts)


def seconds_until_market_open(day, now=None):
    now = now or now_ist()
    wait = (market_open_ist(day) - now).total_seconds()
    return max(0, int(math.ceil(wait)))


def wait_for_market_open(day):
    now = now_ist()
    wait_s = seconds_until_market_open(day, now)
    if wait_s <= 0:
        return
    hh = wait_s // 3600
    mm = (wait_s % 3600) // 60
    ss = wait_s % 60
    print(f"  market has not opened yet. Waiting until 09:15 IST ({hh:02d}:{mm:02d}:{ss:02d})...", flush=True)
    time.sleep(wait_s)
    print(f"  market window open at {now_ist():%H:%M:%S} IST - starting fetches.", flush=True)


# ===========================================================================
# Universe discovery - straight from the live option chain
# ===========================================================================
def discover_universe(md, n_expiries, n_strikes, n_fut_months):
    """Returns (contracts, atm_rs, spot_rs).

    contracts: list of dicts {symbol, expiry, strike, instrument_type}
    """
    oc = md.option_chain(UNDERLYING)
    chain = oc.chain

    spot = rs(chain.current_price)
    atm = rs(chain.at_the_money_strike)

    expiries = sorted({e for e in (parse_expiry(x) for x in chain.all_expiries) if e})
    today = now_ist().date()
    expiries = [e for e in expiries if e >= today]
    if not expiries:
        raise RuntimeError("option_chain returned no current/future expiries")

    # Derive the strike step from the chain itself rather than hard-coding 50.
    strikes_rs = sorted({rs(o.strike_price) for o in chain.ce if o.strike_price})
    steps = [round(b - a) for a, b in zip(strikes_rs, strikes_rs[1:]) if b > a]
    step = min(steps) if steps else 50
    if atm is None:
        atm = round((spot or strikes_rs[len(strikes_rs) // 2]) / step) * step
    atm = round(atm / step) * step

    wanted = [atm + k * step for k in range(-n_strikes, n_strikes + 1)]
    valid = set(strikes_rs) if strikes_rs else None
    if valid:
        wanted = [s for s in wanted if s in valid]

    contracts = []

    # --- options: nearest n_expiries -------------------------------------
    for exp in expiries[:n_expiries]:
        weekly = not is_monthly(exp, expiries)
        for strike in wanted:
            for it in ("CE", "PE"):
                contracts.append({
                    "symbol": build_nubra_symbol(UNDERLYING, exp, strike, it, weekly),
                    "expiry": exp,
                    "strike": f"{float(strike)}",
                    "instrument_type": it,
                })

    # --- futures: next n_fut_months monthly expiries ----------------------
    monthlies = [e for e in expiries if is_monthly(e, expiries)][:n_fut_months]
    for exp in monthlies:
        contracts.append({
            "symbol": build_nubra_symbol(UNDERLYING, exp, "", "FUT", False),
            "expiry": exp,
            "strike": "",
            "instrument_type": "FUT",
        })

    return contracts, atm, spot, step


# ===========================================================================
# Fetch
# ===========================================================================
def fetch_batch(md, limiter, batch, req_type, interval, day, intraday):
    """One historical_data call for <=5 symbols. Returns {symbol: StockChart}."""
    out = {}
    resp = None
    received_at_ist = None
    window = current_fetch_window(day)
    if window is None:
        raise RuntimeError("Market has not opened yet for Nubra historical fetches. Start after 09:15 IST or let the script wait.")
    start_date, end_date = window
    for attempt in range(MAX_RETRIES + 1):
        limiter.wait()
        time.sleep(API_CALL_DELAY)
        try:
            resp = md.historical_data({
                "exchange": EXCHANGE,
                "type": req_type,
                "values": batch,
                "fields": FIELDS,
                "startDate": start_date,
                "endDate": end_date,
                "interval": interval,
                "intraDay": intraday,
                "realTime": False,
            })
            received_at_ist = now_ist().isoformat()
            break
        except Exception as e:
            msg = str(e).lower()
            transient = any(t in msg for t in
                            ("403", "forbidden", "timeout", "max retries",
                             "connection", "reset by peer", "502", "503", "504"))
            if transient and attempt < MAX_RETRIES:
                wait = RETRY_BACKOFF[min(attempt, len(RETRY_BACKOFF) - 1)]
                print(f"      [throttle] {batch[0]} retry {attempt + 1}, "
                      f"waiting {wait}s...", flush=True)
                time.sleep(wait)
                continue
            print(f"      [batch FAILED] {batch[0]}...: {str(e)[:70]}", flush=True)
            return out, True, received_at_ist
    if resp is None or not resp.result or not resp.result[0].values:
        return out, False, received_at_ist
    for inst in resp.result[0].values:
        out.update(inst)
    return out, False, received_at_ist


def build_contract_frame(symbol, sc, meta, received_at_ist):
    """Fixed 22-column frame. Empty fields become all-NaN columns, never dropped."""
    series_by_field, ref_index = {}, None
    for fld in FIELDS:
        s = tsp_series(getattr(sc, fld, None),
                       divide=100 if fld in PRICE_FIELDS else 1)
        if s is not None and len(s) > 0:
            series_by_field[fld] = s
            if ref_index is None or len(s) > len(ref_index):
                ref_index = s.index
    if ref_index is None:
        return None
    data = {}
    for fld in FIELDS:
        if fld in series_by_field:
            data[fld] = series_by_field[fld].reindex(ref_index)
        else:
            data[fld] = pd.Series(float("nan"), index=ref_index, dtype="float64")
    df = pd.DataFrame(data).sort_index()
    df.insert(0, "instrument_type", meta["instrument_type"])
    df.insert(0, "strike", meta["strike"])
    df.insert(0, "expiry", meta["expiry"])
    df.insert(0, "symbol", symbol)
    df.insert(0, "received_at_IST", received_at_ist)
    df.insert(0, "datetime_IST", df.index)
    return df


def fetch_all(md, limiter, contracts, interval, day, intraday):
    meta = {c["symbol"]: c for c in contracts}
    opt = [s for s, m in meta.items() if m["instrument_type"] in ("CE", "PE")]
    fut = [s for s, m in meta.items() if m["instrument_type"] == "FUT"]

    frames, failures = [], 0
    for req_type, syms in (("FUT", fut), ("OPT", opt)):
        for batch in chunks(syms, MAX_INSTRUMENTS_PER_REQUEST):
            got, failed, received_at_ist = fetch_batch(md, limiter, batch, req_type,
                                                       interval, day, intraday)
            failures += int(failed)
            for symbol, sc in got.items():
                if symbol not in meta:
                    continue
                df = build_contract_frame(symbol, sc, meta[symbol], received_at_ist)
                if df is not None:
                    frames.append(df)
    if not frames:
        return None, failures
    combined = pd.concat(frames, ignore_index=True)
    combined = combined.reindex(columns=CANONICAL_COLS)
    combined = combined.sort_values(["symbol", "datetime_IST"]).reset_index(drop=True)
    return combined, failures


def write_atomic(df, path):
    tmp = path + ".tmp"
    df.to_parquet(tmp, index=False)
    os.replace(tmp, path)


# ===========================================================================
def probe_mode(md, limiter, contracts, interval, day):
    """Find out whether intraDay=True actually returns today's bars.

    Per the SDK docs intraDay=True ignores startDate/endDate and uses the
    current date - which is what we want. But behaviour varies, so try it,
    and fall back to explicit start/end if it comes back empty.
    """
    if current_fetch_window(day) is None:
        raise RuntimeError("Market has not opened yet, so Nubra probe fetches would be invalid. Run preflight after 09:15 IST.")
    probe = [c["symbol"] for c in contracts if c["instrument_type"] == "FUT"][:1]
    if not probe:
        probe = [contracts[0]["symbol"]]
    req_type = "FUT" if probe[0].endswith("FUT") else "OPT"
    for intraday in (True, False):
        got, _, _ = fetch_batch(md, limiter, probe, req_type, interval, day, intraday)
        sc = got.get(probe[0])
        n = len(getattr(sc, "close", []) or []) if sc else 0
        print(f"  probe intraDay={intraday}: {probe[0]} -> {n} bars")
        if n:
            return intraday
    raise RuntimeError(
        "Nubra probe returned 0 bars in both intraDay modes. "
        "This usually means the market is not open yet or Nubra rejected the request payload."
    )


def main():
    ap = argparse.ArgumentParser(description="Live-capture today's NIFTY fut+opt data.")
    ap.add_argument("--strikes", type=int, default=15,
                    help="strikes either side of ATM (default 15 -> 31 strikes)")
    ap.add_argument("--expiries", type=int, default=1,
                    help="how many nearest option expiries (default 1)")
    ap.add_argument("--fut-months", type=int, default=3,
                    help="how many monthly futures (default 3)")
    ap.add_argument("--interval", default="1s", choices=["1m", "1s"],
                    help="default 1s for second-level comparison; 1m keeps greeks/IV")
    ap.add_argument("--login-mode", default="otp", choices=["otp", "totp"],
                    help="otp = prompt once in terminal; totp = auto-use TOTP_SECRET")
    ap.add_argument("--poll", type=int, default=60,
                    help="seconds between refresh cycles (default 60)")
    ap.add_argument("--out", default=".",
                    help="output directory (default: this script folder)")
    ap.add_argument("--cred-dir", default=None,
                    help="folder holding .env + auth cache (default: this script's folder)")
    ap.add_argument("--until", default="15:32",
                    help="stop time in IST HH:MM (default 15:32)")
    ap.add_argument("--csv", action="store_true",
                    help="also write a .csv alongside the parquet")
    ap.add_argument("--preflight", action="store_true",
                    help="run auth + discovery + a small fetch probe, then exit")
    ap.add_argument("--dry-run", action="store_true",
                    help="print the universe and exit without fetching")
    args = ap.parse_args()

    here = os.path.abspath(os.path.dirname(__file__))
    cred_dir = os.path.abspath(args.cred_dir or here)
    out_root = os.path.abspath(os.path.join(here, args.out)) if not os.path.isabs(args.out) else args.out
    out_dir = out_root
    os.makedirs(out_dir, exist_ok=True)

    stop_h, stop_m = (int(x) for x in args.until.split(":"))

    print("=" * 68)
    print(f"  NIFTY live capture  |  {now_ist():%Y-%m-%d %H:%M:%S} IST")
    print(f"  interval={args.interval}  poll={args.poll}s  out={out_dir}")
    print("=" * 68)
    if args.interval == "1s":
        print("  NOTE: this script is still historical/poll-based, not websocket-based.")
        print("        It now defaults to 1s bars so the saved parquet matches your")
        print("        second-level comparison workflow.")

    # --- auth: the SDK reads .env and writes its token cache in the CWD ---
    prev_cwd = os.getcwd()
    os.chdir(cred_dir)
    try:
        if args.login_mode == "totp":
            with _auto_nubra_input():
                nubra = InitNubraSdk(ENV, env_creds=True, totp_login=True)
                md = MarketData(nubra)
        else:
            nubra = InitNubraSdk(ENV, env_creds=True, totp_login=False)
            md = MarketData(nubra)
    finally:
        os.chdir(prev_cwd)

    # --- universe ---------------------------------------------------------
    contracts, atm, spot, step = discover_universe(
        md, args.expiries, args.strikes, args.fut_months)
    known = {c["symbol"] for c in contracts}
    n_calls = math.ceil(len(contracts) / MAX_INSTRUMENTS_PER_REQUEST)
    print(f"\n  spot~{spot}  ATM={atm}  step={step}")
    print(f"  {len(contracts)} instruments -> {n_calls} API calls/cycle "
          f"(~{n_calls * 60 / MAX_CALLS_PER_MIN:.0f}s minimum per cycle)")
    exps = sorted({c["expiry"] for c in contracts})
    print(f"  expiries: {[e.isoformat() for e in exps]}")
    if n_calls * 60 / MAX_CALLS_PER_MIN > args.poll:
        print("  NOTE: a cycle takes longer than --poll; cycles will just run "
              "back-to-back.")

    if args.dry_run:
        for c in contracts[:10]:
            print("   ", c["symbol"])
        print(f"    ... ({len(contracts)} total)")
        return

    limiter = RateLimiter(MAX_CALLS_PER_MIN)
    day = now_ist().date()
    if args.preflight and current_fetch_window(day) is None:
        raise RuntimeError("Market has not opened yet. Nubra preflight fetches are only valid after 09:15 IST.")
    if not args.preflight:
        wait_for_market_open(day)
    print()
    intraday = probe_mode(md, limiter, contracts, args.interval, day)

    if args.preflight:
        sample = [c for c in contracts if c["instrument_type"] == "FUT"][:1] or contracts[:1]
        df, failures = fetch_all(md, limiter, sample, args.interval, day, intraday)
        if df is None or df.empty:
            raise RuntimeError(f"preflight fetch returned no rows ({failures} failed batches)")
        last_bar = pd.to_datetime(df["datetime_IST"]).max()
        print(f"  preflight ok: {len(df):,} rows across {df['symbol'].nunique()} contract(s), last bar {last_bar}")
        return

    out_path = os.path.join(out_dir, f"{day.isoformat()}.parquet")
    cycle = 0
    last_universe_refresh = time.monotonic()

    try:
        while True:
            t = now_ist()
            if (t.hour, t.minute) >= (stop_h, stop_m):
                print(f"\n  reached {args.until} IST - stopping.")
                break

            cycle += 1
            t0 = time.monotonic()
            df, failures = fetch_all(md, limiter, contracts,
                                     args.interval, day, intraday)
            if df is None or df.empty:
                print(f"  [{t:%H:%M:%S}] cycle {cycle}: no rows yet "
                      f"({failures} failed batches)")
            else:
                write_atomic(df, out_path)
                if args.csv:
                    df.to_csv(out_path.replace(".parquet", ".csv"), index=False)
                last_bar = pd.to_datetime(df["datetime_IST"]).max()
                bar_fmt = "%H:%M:%S" if args.interval == "1s" else "%H:%M"
                print(f"  [{t:%H:%M:%S}] cycle {cycle}: {df['symbol'].nunique()} "
                      f"contracts, {len(df):,} rows, last bar {last_bar:{bar_fmt}} "
                      f"({time.monotonic() - t0:.0f}s"
                      f"{f', {failures} failed batches' if failures else ''})")

            # Re-resolve ATM every ~15 min and ADD strikes if the market moved.
            if time.monotonic() - last_universe_refresh > 900:
                last_universe_refresh = time.monotonic()
                try:
                    fresh, atm2, spot2, _ = discover_universe(
                        md, args.expiries, args.strikes, args.fut_months)
                    added = [c for c in fresh if c["symbol"] not in known]
                    if added:
                        contracts.extend(added)
                        known.update(c["symbol"] for c in added)
                        print(f"      [universe] ATM moved to {atm2}; "
                              f"added {len(added)} contracts "
                              f"(now {len(contracts)})")
                except Exception as e:
                    print(f"      [universe] refresh failed: {str(e)[:60]}")

            slept = time.monotonic() - t0
            if slept < args.poll:
                time.sleep(args.poll - slept)

    except KeyboardInterrupt:
        print("\n  interrupted - doing one final fetch...")
        df, _ = fetch_all(md, limiter, contracts, args.interval, day, intraday)
        if df is not None and not df.empty:
            write_atomic(df, out_path)

    if os.path.exists(out_path):
        final = pd.read_parquet(out_path)
        print(f"\n  saved {out_path}")
        print(f"  {len(final):,} rows | {final['symbol'].nunique()} contracts | "
              f"{final['datetime_IST'].min()} .. {final['datetime_IST'].max()}")
        print(f"  columns: {len(final.columns)}")
    else:
        print("\n  nothing saved.")


if __name__ == "__main__":
    sys.exit(main())

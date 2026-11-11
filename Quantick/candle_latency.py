#!/usr/bin/env python3
"""1-minute candle close time vs when the candle is actually available.

Two ways a system gets a 1-minute candle, both measured here for NIFTY futures,
the ATM CE/PE of the nearest expiry, and the NIFTY index:

  API candles   poll a broker's historical API around every minute boundary and
                record when candle M first appears and when it stops changing.
                Sources: Quantick, and Fyers (what AlphaBot's Algo Builder reads:
                algo_builder/market_data_adapter.py -> fyers.history, at :00).
  Tick candles  build the candle from the live tick capture: candle M can close
                when the first tick stamped >= M+1 arrives. Sources: the Nubra
                orderbook capture (AlphaBot's live feed) and the Quantick capture.

    python Quantick/candle_latency.py poll --sources quantick fyers --until 15:31
    python Quantick/candle_latency.py report --date 2026-10-19

Poll schedule per symbol per minute: +0.3 .. +2.8 s every 0.5 s, then 3.5, 4.5,
6, 8, 11, 15 and 30 s after the boundary (13 requests). With 4 symbols that is
52/min per source, well under Fyers' 200/min limit. Quantick refused all
historical calls from 13:21 on 2026-10-16 ("requires a historical or both
subscription"), so a quantick source that keeps failing is dropped after 30 errors.
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import sys
import threading
import time
import urllib.request
from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent
ROOT_DIR = BASE_DIR.parent
sys.path.insert(0, str(BASE_DIR))
IST = dt.timezone(dt.timedelta(hours=5, minutes=30))

COLUMNS = ["source", "role", "symbol", "sent_at", "received_at", "minute", "open", "high", "low", "close", "volume"]
OFFSETS = [0.3, 0.8, 1.3, 1.8, 2.3, 2.8, 3.5, 4.5, 6.0, 8.0, 11.0, 15.0, 30.0]
FYERS_MASTER_URL = "https://public.fyers.in/sym_details/NSE_FO.csv"


def now() -> dt.datetime:
    return dt.datetime.now(IST)


def out_path(day: str) -> Path:
    return BASE_DIR / f"quantick_candle_latency_{day}.csv"


# ------------------------------------------------------------------ sources


class FyersSource:
    name = "fyers"

    def __init__(self) -> None:
        sys.path.insert(0, str(ROOT_DIR / "FYERS"))
        import fyers_websocket as fw
        from fyers_apiv3 import fyersModel

        self.fw = fw
        self.client = fyersModel.FyersModel(client_id=fw.load_client_id(fw.LOGIN_FILE),
                                            token=fw.load_access_token(fw.ACCESS_TOKEN_FILE),
                                            is_async=False, log_path="")
        self.master = ROOT_DIR / "FYERS" / "NSE_FO.csv"
        urllib.request.urlretrieve(FYERS_MASTER_URL, self.master)
        self.reference_price()  # fails fast on an expired token

    def reference_price(self) -> float:
        res = self.client.quotes({"symbols": "NSE:NIFTY50-INDEX"})
        if res.get("s") != "ok":
            raise RuntimeError(f"Fyers quote failed: {str(res)[:200]}")
        return float(res["d"][0]["v"]["lp"])

    def symbols(self, atm: float) -> dict[str, str]:
        today = now().date()
        fut = self.fw.resolve_nearest_nifty_future(self.master)
        opts = [o for o in self.fw.load_nifty_option_candidates(self.master) if o["expiry"] >= today]
        expiry = min(o["expiry"] for o in opts)
        pick = {o["symbol"][-2:]: o["symbol"] for o in opts
                if o["expiry"] == expiry and abs(float(o["strike"]) - atm) < 1e-6}
        return {"INDEX": "NSE:NIFTY50-INDEX", "FUT": fut, "ATM_CE": pick["CE"], "ATM_PE": pick["PE"]}

    def candles(self, symbol: str, frm: dt.datetime, to: dt.datetime) -> list[dict]:
        res = self.client.history({"symbol": symbol, "resolution": "1", "date_format": "0",
                                   "range_from": str(int(frm.timestamp())), "range_to": str(int(to.timestamp())),
                                   "cont_flag": "1"})
        if res.get("s") != "ok":
            raise RuntimeError(str(res)[:200])
        return [{"minute": dt.datetime.fromtimestamp(c[0], IST).strftime("%Y-%m-%d %H:%M:%S"),
                 "open": c[1], "high": c[2], "low": c[3], "close": c[4], "volume": c[5]}
                for c in res.get("candles") or []]

    def close(self) -> None:
        pass


class QuantickSource:
    name = "quantick"

    def __init__(self) -> None:
        from compare_feeds import quantick_client

        self.client, self.tunnel = quantick_client()

    def reference_price(self) -> float:
        today = now().date().isoformat()
        c = self.client.historical(symbol="NSE:NIFTY 50-IN", from_date=today, to_date=today, interval="day")["candles"]
        return float(c[-1]["close"])

    def symbols(self, atm: float) -> dict[str, str]:
        legs = self.client.option_symbols("NIFTY", min_strike=atm, max_strike=atm)["legs"]
        return {"INDEX": "NSE:NIFTY 50-IN", "FUT": "NFO:NIFTY-I", "ATM_CE": legs[0]["CE"], "ATM_PE": legs[0]["PE"]}

    def candles(self, symbol: str, frm: dt.datetime, to: dt.datetime) -> list[dict]:
        res = self.client.historical(symbol=symbol, from_date=frm.strftime("%Y-%m-%d %H:%M:%S"),
                                     to_date=to.strftime("%Y-%m-%d %H:%M:%S"), interval="minute")
        return [{"minute": c.get("date"), "open": c.get("open"), "high": c.get("high"), "low": c.get("low"),
                 "close": c.get("close"), "volume": c.get("volume")} for c in res.get("candles") or []]

    def close(self) -> None:
        self.client.close()
        if self.tunnel:
            self.tunnel.stop()


# --------------------------------------------------------------------- poll


def poll(args: argparse.Namespace) -> int:
    sources = []
    for name in args.sources:
        try:
            sources.append(FyersSource() if name == "fyers" else QuantickSource())
            print(f"  {name}: connected", flush=True)
        except Exception as exc:
            print(f"  !! {name} unavailable: {type(exc).__name__}: {str(exc)[:200]}", flush=True)
    if not sources:
        print("no candle source available")
        return 1

    atm = args.atm
    for src in sources:
        if atm:
            break
        try:
            atm = round(src.reference_price() / 50) * 50
            print(f"  ATM {atm} from {src.name}", flush=True)
        except Exception as exc:
            print(f"  {src.name} reference price failed: {str(exc)[:160]}", flush=True)
    if not atm:
        raise SystemExit("could not determine NIFTY ATM; pass --atm")

    plan: list[tuple[object, str, str]] = []
    for src in list(sources):
        try:
            for role, sym in src.symbols(atm).items():
                if role not in args.roles:
                    continue
                plan.append((src, role, sym))
                print(f"  {src.name:<9} {role:<7} {sym}", flush=True)
        except Exception as exc:
            print(f"  !! {src.name} symbol lookup failed: {str(exc)[:160]}", flush=True)

    hh, mm = map(int, args.until.split(":"))
    stop_at = now().replace(hour=hh, minute=mm, second=0, microsecond=0)
    path = out_path(now().date().isoformat())
    new = not path.exists()
    handle = path.open("a", newline="")
    writer = csv.writer(handle)
    if new:
        writer.writerow(COLUMNS)
    lock = threading.Lock()
    errors = {src.name: 0 for src in sources}
    dropped: set[str] = set()

    def fetch(src, role: str, sym: str) -> None:
        if src.name in dropped:
            return
        t = now()
        frm = (t - dt.timedelta(minutes=2)).replace(second=0, microsecond=0)
        sent = now()
        try:
            rows = src.candles(sym, frm, t)
        except Exception as exc:
            with lock:
                errors[src.name] += 1
                if errors[src.name] in (1, 10) or errors[src.name] % 100 == 0:
                    print(f"  {src.name} {role}: {type(exc).__name__}: {str(exc)[:160]} (errors={errors[src.name]})", flush=True)
                if src.name == "quantick" and errors[src.name] >= 30:
                    dropped.add(src.name)
                    print("  !! dropping quantick: historical access keeps failing", flush=True)
            return
        recv = now()
        with lock:
            writer.writerows([[src.name, role, sym, sent.isoformat(), recv.isoformat(), r["minute"], r["open"],
                               r["high"], r["low"], r["close"], r["volume"]] for r in rows])
            handle.flush()

    print(f"polling {len(plan)} series until {args.until} -> {path.name}", flush=True)
    try:
        while now() < stop_at:
            boundary = (now() + dt.timedelta(minutes=1)).replace(second=0, microsecond=0)
            for off in OFFSETS:
                delay = (boundary + dt.timedelta(seconds=off) - now()).total_seconds()
                if delay > 0:
                    time.sleep(delay)
                threads = [threading.Thread(target=fetch, args=p) for p in plan]
                for th in threads:
                    th.start()
                for th in threads:
                    th.join(timeout=10)
            if boundary.minute % 15 == 0:
                print(f"  [{now():%H:%M:%S}] ok errors={errors}", flush=True)
    finally:
        handle.close()
        for src in sources:
            src.close()
    return 0


# ------------------------------------------------------------------- report


def api_candle_table(df: pd.DataFrame) -> pd.DataFrame:
    detail = []
    vals = ["open", "high", "low", "close", "volume"]
    for (source, role, minute), g in df.groupby(["source", "role", "minute"]):
        close_at = minute + pd.Timedelta(minutes=1)
        after = g[g["sent_at"] >= close_at]
        if after.empty:
            continue
        first, final = after.iloc[0], g.iloc[-1]
        # Only time it when we were already polling at the boundary.
        at_boundary = (first["sent_at"] - close_at).total_seconds() < 1.0
        same = (g[vals].values == final[vals].values).all(axis=1)
        diffs = [i for i, ok in enumerate(same) if not ok]
        settled = g.iloc[diffs[-1] + 1] if diffs else first
        if settled["received_at"] < first["received_at"]:
            settled = first  # unchanged since served mid-minute: final as of the first post-close fetch
        detail.append({
            "source": source, "role": role, "symbol": g["symbol"].iloc[0], "minute": minute,
            "available_s": (first["received_at"] - close_at).total_seconds() if at_boundary else None,
            "final_s": (settled["received_at"] - close_at).total_seconds() if at_boundary else None,
            "served_while_forming": bool((g["sent_at"] < close_at).any()),
            "close_changed_after_first": abs(float(first["close"]) - float(final["close"])) > 1e-9,
            "final_close": float(final["close"]),
        })
    return pd.DataFrame(detail)


def tick_candle_table(day: str, minutes: list) -> pd.DataFrame:
    """When could a tick-built candle M close: first tick stamped >= M+1, per feed and contract role."""
    out = []
    feeds = []
    q = BASE_DIR / f"quantick_live_ticks_{day}.csv"
    n = ROOT_DIR / "Nubra" / f"nubra_live_ticks_{day}.csv"
    if q.exists():
        df = pd.read_csv(q, usecols=["underlying", "exchange_segment", "instrument_type", "strike", "expiry",
                                     "listed_at_ist", "received_at_ist", "ltp"])
        df = df[(df["underlying"] == "NIFTY") & df["exchange_segment"].astype(str).str.startswith("NFO")]
        feeds.append(("quantick ticks", df.rename(columns={"listed_at_ist": "exch", "received_at_ist": "recv"})))
    if n.exists():
        df = pd.read_csv(n, usecols=["instrument_type", "strike", "expiry", "datetime_IST", "received_at_IST", "ltp"])
        feeds.append(("nubra ticks", df.rename(columns={"datetime_IST": "exch", "received_at_IST": "recv"})))
    for name, df in feeds:
        df = df.dropna(subset=["ltp"]).copy()
        df["exch"] = pd.to_datetime(df["exch"], format="ISO8601")
        df["recv"] = pd.to_datetime(df["recv"], format="ISO8601")
        df["strike"] = pd.to_numeric(df["strike"], errors="coerce")
        df["exp"] = df["expiry"].astype(str).str.replace("-", "").str[:8]
        opts = df[df["instrument_type"].isin(["CE", "PE"])]
        if opts.empty:
            continue
        expiry = min(e for e in opts["exp"].unique() if e >= day.replace("-", ""))
        futs = df[df["instrument_type"] == "FUT"]
        roles = {}
        if not futs.empty:
            roles["FUT"] = futs[futs["exp"] == futs["exp"].min()]
        ref = float(futs["ltp"].median()) if not futs.empty else float(opts["strike"].median())
        atm = round(ref / 50) * 50
        for side in ("CE", "PE"):
            roles[f"ATM_{side}"] = opts[(opts["exp"] == expiry) & (opts["instrument_type"] == side) & (opts["strike"] == atm)]
        for role, g in roles.items():
            g = g.sort_values("recv")
            for m in minutes:
                close_at = m + pd.Timedelta(minutes=1)
                inside = g[(g["exch"] >= m) & (g["exch"] < close_at)]
                nxt = g[g["exch"] >= close_at]
                if inside.empty or nxt.empty:
                    continue
                out.append({"source": name, "role": role, "minute": m,
                            "closable_s": (nxt["recv"].iloc[0] - close_at).total_seconds(),
                            "tick_close": float(inside["ltp"].iloc[-1])})
    return pd.DataFrame(out)


def report(args: argparse.Namespace) -> int:
    day = args.date
    df = pd.read_csv(out_path(day))
    if "source" not in df.columns:  # 2026-10-16 file: quantick only, keyed by symbol
        df.insert(0, "source", "quantick")
        df.insert(1, "role", df["symbol"])
    for c in ("sent_at", "received_at"):
        df[c] = pd.to_datetime(df[c], format="ISO8601")
    df["minute"] = pd.to_datetime(df["minute"], format="ISO8601").dt.tz_localize(IST)
    df = df[(df["minute"].dt.time >= dt.time(9, 15)) & (df["minute"].dt.time <= dt.time(15, 29))]
    df = df.sort_values("received_at")

    api = api_candle_table(df)
    ticks = tick_candle_table(day, sorted(api["minute"].unique()) if not api.empty else [])
    out_dir = BASE_DIR / "reports" / day
    out_dir.mkdir(parents=True, exist_ok=True)
    api.to_csv(out_dir / "candle_latency_api_detail.csv", index=False)
    ticks.to_csv(out_dir / "candle_latency_tick_detail.csv", index=False)

    rows = []
    for (source, role), g in api.groupby(["source", "role"]):
        a, f = g["available_s"].dropna(), g["final_s"].dropna()
        rows.append({"how": "API candle", "source": source, "contract": role, "minutes": len(g),
                     "first seen p50 s": round(a.median(), 2) if len(a) else None,
                     "final p50 s": round(f.median(), 2) if len(f) else None,
                     "final p95 s": round(f.quantile(0.95), 2) if len(f) else None,
                     "final max s": round(f.max(), 2) if len(f) else None,
                     "served while forming": f"{100 * g['served_while_forming'].mean():.0f}%",
                     "close changed after first fetch": f"{100 * g['close_changed_after_first'].mean():.0f}%"})
    if not ticks.empty:
        for (source, role), g in ticks.groupby(["source", "role"]):
            c = g["closable_s"]
            rows.append({"how": "tick-built", "source": source, "contract": role, "minutes": len(g),
                         "first seen p50 s": round(c.median(), 2), "final p50 s": round(c.median(), 2),
                         "final p95 s": round(c.quantile(0.95), 2), "final max s": round(c.max(), 2),
                         "served while forming": "-", "close changed after first fetch": "-"})
    summary = pd.DataFrame(rows)
    summary.to_csv(out_dir / "candle_latency_summary.csv", index=False)
    print("Seconds after the 1-minute candle closes (exchange time) until it is available / final:\n")
    print(summary.to_string(index=False))

    if not api.empty:
        piv = api.pivot_table(index=["role", "minute"], columns="source", values="final_close")
        if not ticks.empty:
            piv = piv.join(ticks.pivot_table(index=["role", "minute"], columns="source", values="tick_close"), how="outer")
        cols = list(piv.columns)
        agree = []
        for i, a in enumerate(cols):
            for b in cols[i + 1:]:
                both = piv[[a, b]].dropna()
                if len(both):
                    agree.append({"pair": f"{a} vs {b}", "minutes": len(both),
                                  "close equal": f"{100 * ((both[a] - both[b]).abs() < 1e-9).mean():.0f}%",
                                  "within 0.05": f"{100 * ((both[a] - both[b]).abs() <= 0.0501).mean():.0f}%"})
        if agree:
            print("\nFinal 1-minute close agreement between sources:\n")
            print(pd.DataFrame(agree).to_string(index=False))
            pd.DataFrame(agree).to_csv(out_dir / "candle_close_agreement.csv", index=False)
    print(f"\nWritten: {out_dir}/candle_latency_*.csv")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("poll")
    p.add_argument("--sources", nargs="+", default=["quantick"], choices=["quantick", "fyers"])
    p.add_argument("--until", default="15:31")
    p.add_argument("--atm", type=float, default=None, help="NIFTY ATM strike (default: from the index price)")
    p.add_argument("--roles", nargs="+", default=["INDEX", "FUT", "ATM_CE", "ATM_PE"],
                   help="which series to poll (fewer = fewer requests; 3 roles = 39/min)")
    r = sub.add_parser("report")
    r.add_argument("--date", default=now().date().isoformat())
    args = ap.parse_args()
    return poll(args) if args.cmd == "poll" else report(args)


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import math
import os
import signal
import sys
import threading
import time
from pathlib import Path
from typing import Any

import pyotp

from nubra_python_sdk.marketdata.market_data import MarketData
from nubra_python_sdk.refdata.instruments import InstrumentData
from nubra_python_sdk.start_sdk import InitNubraSdk, NubraEnv
from nubra_python_sdk.ticker import websocketdata

from capture_nifty_today import (
    _auto_nubra_input,
    discover_universe,
    now_ist,
    wait_for_market_open,
)


ENV = NubraEnv.PROD
EXCHANGE = "NSE"
UNDERLYING = "NIFTY"
IST = dt.timezone(dt.timedelta(hours=5, minutes=30))

RAW_TICKS_BASENAME = "nubra_live_ticks"
AGG_1S_BASENAME = "nubra_live_1s"

CANONICAL_COLS = [
    "datetime_IST",
    "received_at_IST",
    "first_received_at_IST",
    "last_received_at_IST",
    "symbol",
    "expiry",
    "strike",
    "instrument_type",
    "open",
    "high",
    "low",
    "close",
    "l1bid",
    "l1ask",
    "cumulative_volume",
    "cumulative_volume_premium",
    "cumulative_oi",
    "cumulative_volume_delta",
    "theta",
    "delta",
    "gamma",
    "vega",
    "iv_bid",
    "iv_ask",
    "iv_mid",
]


def output_paths(out_dir: Path, day: dt.date) -> dict[str, Path]:
    tag = day.isoformat()
    return {
        "raw_csv": out_dir / f"{RAW_TICKS_BASENAME}_{tag}.csv",
        "agg_csv": out_dir / f"{AGG_1S_BASENAME}_{tag}.csv",
    }


def parse_price(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value) / 100.0
    except (TypeError, ValueError):
        return None


def parse_epoch_to_ist(value: Any) -> dt.datetime:
    if value in (None, ""):
        return now_ist()
    try:
        numeric = int(value)
    except (TypeError, ValueError):
        return now_ist()
    if numeric > 10**18:
        return dt.datetime.fromtimestamp(numeric / 1_000_000_000, tz=dt.timezone.utc).astimezone(IST)
    if numeric > 10**15:
        return dt.datetime.fromtimestamp(numeric / 1_000_000, tz=dt.timezone.utc).astimezone(IST)
    if numeric > 10**12:
        return dt.datetime.fromtimestamp(numeric / 1_000, tz=dt.timezone.utc).astimezone(IST)
    return dt.datetime.fromtimestamp(numeric, tz=dt.timezone.utc).astimezone(IST)


def bucket_start(ts: dt.datetime, interval: str) -> dt.datetime:
    if interval == "1m":
        return ts.replace(second=0, microsecond=0)
    return ts.replace(microsecond=0)


def meta_row(meta: dict[str, Any], **updates: Any) -> dict[str, Any]:
    row = {
        "datetime_IST": None,
        "received_at_IST": None,
        "first_received_at_IST": None,
        "last_received_at_IST": None,
        "symbol": meta["symbol"],
        "expiry": meta["expiry"].isoformat() if hasattr(meta["expiry"], "isoformat") else meta["expiry"],
        "strike": meta["strike"],
        "instrument_type": meta["instrument_type"],
        "open": None,
        "high": None,
        "low": None,
        "close": None,
        "l1bid": None,
        "l1ask": None,
        "cumulative_volume": None,
        "cumulative_volume_premium": None,
        "cumulative_oi": None,
        "cumulative_volume_delta": None,
        "theta": None,
        "delta": None,
        "gamma": None,
        "vega": None,
        "iv_bid": None,
        "iv_ask": None,
        "iv_mid": None,
    }
    row.update(updates)
    return row


class TickStorage:
    def __init__(self, raw_csv_path: Path, agg_csv_path: Path, interval: str):
        self.raw_csv_path = raw_csv_path
        self.agg_csv_path = agg_csv_path
        self.interval = interval
        self.lock = threading.Lock()
        self.pending: dict[str, dict[str, Any]] = {}
        self._init_raw_csv()
        self._init_agg_csv()

    def _init_raw_csv(self) -> None:
        if self.raw_csv_path.exists():
            return
        with self.raw_csv_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow([
                "datetime_IST",
                "received_at_IST",
                "symbol",
                "ref_id",
                "expiry",
                "strike",
                "instrument_type",
                "ltp",
                "l1bid",
                "l1ask",
                "cumulative_volume",
                "open_interest",
                "raw_json",
            ])

    def _init_agg_csv(self) -> None:
        assert self.agg_csv_path is not None
        if self.agg_csv_path.exists():
            return
        with self.agg_csv_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(CANONICAL_COLS)

    def persist(self, tick: dict[str, Any], meta: dict[str, Any]) -> None:
        with self.lock:
            self._write_raw(tick, meta)
            self._update_bucket(tick, meta)

    def close(self) -> None:
        with self.lock:
            for symbol in sorted(self.pending):
                self._flush_bucket(self.pending[symbol])
            self.pending.clear()

    def _write_raw(self, tick: dict[str, Any], meta: dict[str, Any]) -> None:
        with self.raw_csv_path.open("a", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow([
                tick["listed_at"].isoformat(),
                tick["received_at_IST"],
                meta["symbol"],
                tick.get("ref_id"),
                meta["expiry"].isoformat() if hasattr(meta["expiry"], "isoformat") else meta["expiry"],
                meta["strike"],
                meta["instrument_type"],
                tick["ltp"],
                tick["l1bid"],
                tick["l1ask"],
                tick["cumulative_volume"],
                tick.get("cumulative_oi"),
                tick["raw_json"],
            ])

    def _update_bucket(self, tick: dict[str, Any], meta: dict[str, Any]) -> None:
        start = bucket_start(tick["listed_at"], self.interval)
        symbol = meta["symbol"]
        bucket = self.pending.get(symbol)
        if bucket is not None and bucket["datetime_IST"] != start.isoformat():
            self._flush_bucket(bucket)
            bucket = None

        if bucket is None:
            bucket = meta_row(
                meta,
                datetime_IST=start.isoformat(),
                received_at_IST=tick["received_at_IST"],
                first_received_at_IST=tick["received_at_IST"],
                last_received_at_IST=tick["received_at_IST"],
                open=tick["ltp"],
                high=tick["ltp"],
                low=tick["ltp"],
                close=tick["ltp"],
                l1bid=tick.get("l1bid"),
                l1ask=tick.get("l1ask"),
                cumulative_volume=tick.get("cumulative_volume"),
                cumulative_oi=tick.get("cumulative_oi"),
                iv_mid=tick.get("iv_mid"),
                delta=tick.get("delta"),
                gamma=tick.get("gamma"),
                theta=tick.get("theta"),
                vega=tick.get("vega"),
            )
            self.pending[symbol] = bucket
            return

        ltp = tick["ltp"]
        if ltp is not None:
            if bucket["open"] is None:
                bucket["open"] = ltp
            bucket["high"] = ltp if bucket["high"] is None else max(bucket["high"], ltp)
            bucket["low"] = ltp if bucket["low"] is None else min(bucket["low"], ltp)
            bucket["close"] = ltp
        for field in ("l1bid", "l1ask", "cumulative_volume", "cumulative_oi",
                      "iv_mid", "delta", "gamma", "theta", "vega"):
            if tick.get(field) is not None:
                bucket[field] = tick[field]
        bucket["received_at_IST"] = tick["received_at_IST"]
        bucket["last_received_at_IST"] = tick["received_at_IST"]

    def _flush_bucket(self, row: dict[str, Any]) -> None:
        with self.agg_csv_path.open("a", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow([row.get(col) for col in CANONICAL_COLS])


def login_client(login_mode: str):
    if login_mode == "totp":
        with _auto_nubra_input():
            return InitNubraSdk(ENV, env_creds=True, totp_login=True)
    return InitNubraSdk(ENV, env_creds=True, totp_login=False)


def resolve_contract_refs(nubra, contracts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    inst = InstrumentData(nubra)
    resolved = []
    missing = []
    for contract in contracts:
        instrument = inst.get_instrument_by_symbol(contract["symbol"], exchange="NSE")
        ref_id = getattr(instrument, "ref_id", None)
        if ref_id is None and isinstance(instrument, dict):
            ref_id = instrument.get("ref_id")
        if ref_id is None:
            missing.append(contract["symbol"])
            continue
        item = dict(contract)
        item["ref_id"] = int(ref_id)
        resolved.append(item)
    if not resolved:
        raise RuntimeError("Could not resolve any Nubra ref_id values for the selected universe.")
    if missing:
        print(f"  warning: could not resolve {len(missing)} contract(s); continuing with {len(resolved)} resolved instruments", flush=True)
        for symbol in missing[:10]:
            print(f"    - {symbol}", flush=True)
    return resolved


def _num(value):
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def build_option_symbol(asset, expiry, strike, opt_type):
    exp = expiry
    try:
        d = dt.date.fromisoformat(str(expiry)[:10])
        exp = d.strftime("%y%b").upper()
    except (ValueError, TypeError):
        exp = str(expiry)
    strike_txt = str(int(strike)) if strike is not None and float(strike).is_integer() else str(strike)
    return f"{asset}{exp}{strike_txt}{opt_type}"


def on_option_chain_message(wrapper, storage, price_divisor=100.0):
    """Handle one OptionChainWrapper update from the Nubra `option` stream.

    Receive time is clocked FIRST, before any parsing, so it reflects arrival
    on this machine. The per-option `timestamp` is the exchange (listed) time.
    """
    received = now_ist()
    received_iso = received.isoformat()

    asset = getattr(wrapper, "asset", None) or UNDERLYING
    expiry = getattr(wrapper, "expiry", None)

    def price(v):
        n = _num(v)
        return None if n is None else n / price_divisor

    sides = (("CE", getattr(wrapper, "ce", None) or []),
             ("PE", getattr(wrapper, "pe", None) or []))
    for opt_type, rows in sides:
        for od in rows:
            strike_raw = _num(getattr(od, "strike_price", None))
            # Nubra returns strikes and prices in exchange-native paise (x100),
            # e.g. 2935000 -> 29350. Scale by the same divisor as LTP.
            strike = None if strike_raw is None else strike_raw / price_divisor
            # The parquet schema types `strike` and `expiry` as strings, so
            # coerce here — passing a float into a pyarrow string column raises
            # "Expected bytes, got a 'float' object".
            if strike is None:
                strike_str = None
            elif float(strike).is_integer():
                strike_str = str(int(strike))
            else:
                strike_str = str(strike)
            expiry_str = None if expiry is None else str(expiry)
            listed = parse_epoch_to_ist(getattr(od, "timestamp", None))
            meta = {
                "symbol": build_option_symbol(asset, expiry, strike, opt_type),
                "expiry": expiry_str,
                "strike": strike_str,
                "instrument_type": opt_type,
            }
            tick = {
                "listed_at": listed,
                "received_at_IST": received_iso,
                "ref_id": getattr(od, "ref_id", None),
                "ltp": price(getattr(od, "last_traded_price", None)),
                "l1bid": None,   # option-chain stream carries no L1 bid/ask
                "l1ask": None,
                "cumulative_volume": _num(getattr(od, "volume", None)),
                "cumulative_oi": _num(getattr(od, "open_interest", None)),
                "iv_mid": _num(getattr(od, "iv", None)),
                "delta": _num(getattr(od, "delta", None)),
                "gamma": _num(getattr(od, "gamma", None)),
                "theta": _num(getattr(od, "theta", None)),
                "vega": _num(getattr(od, "vega", None)),
                "raw_json": json.dumps({
                    "asset": asset, "expiry": str(expiry),
                    "atm": getattr(wrapper, "at_the_money_strike", None),
                    "underlying_price": getattr(wrapper, "current_price", None),
                    "ref_id": getattr(od, "ref_id", None),
                    "timestamp": getattr(od, "timestamp", None),
                    "strike_price": getattr(od, "strike_price", None),
                    "option_type": opt_type,
                    "last_traded_price": getattr(od, "last_traded_price", None),
                    "iv": getattr(od, "iv", None),
                    "delta": getattr(od, "delta", None),
                    "gamma": getattr(od, "gamma", None),
                    "theta": getattr(od, "theta", None),
                    "vega": getattr(od, "vega", None),
                    "volume": getattr(od, "volume", None),
                    "open_interest": getattr(od, "open_interest", None),
                    "previous_open_interest": getattr(od, "previous_open_interest", None),
                }, default=str),
            }
            storage.persist(tick, meta)


def parse_until(value: str) -> tuple[int, int]:
    hh, mm = str(value).split(":", 1)
    return int(hh), int(mm)


def main() -> int:
    ap = argparse.ArgumentParser(description="True Nubra WebSocket capture for NIFTY future + ATM option chain.")
    ap.add_argument("--strikes", type=int, default=15, help="strikes either side of ATM")
    ap.add_argument("--expiries", type=int, default=1, help="nearest option expiries")
    ap.add_argument("--fut-months", type=int, default=3, help="how many monthly futures")
    ap.add_argument("--interval", default="1s", choices=["1s", "1m"], help="aggregation bucket size for saved output")
    ap.add_argument("--login-mode", default="otp", choices=["otp", "totp"], help="otp prompts once; totp auto-uses TOTP_SECRET")
    ap.add_argument("--poll", type=int, default=60, help="unused in websocket mode; kept only for launcher compatibility")
    ap.add_argument("--out", default=str(Path(__file__).resolve().parent), help="output directory (defaults to this Nubra/ folder)")
    ap.add_argument("--cred-dir", default=None, help="folder holding .env + auth cache")
    ap.add_argument("--until", default="15:32", help="stop time in IST HH:MM")
    ap.add_argument("--dry-run", action="store_true", help="show resolved universe, then exit")
    ap.add_argument("--preflight", action="store_true", help="validate login + universe + ref_ids, then exit")
    ap.add_argument("--price-divisor", type=float, default=100.0, help="divide option LTP by this to get rupees (default 100; set 1 if already in rupees)")
    args = ap.parse_args()

    out_dir = Path(args.out).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    if args.cred_dir:
        os.chdir(args.cred_dir)

    print("=" * 72)
    print(f"  NIFTY live websocket capture  |  {now_ist().strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print(f"  interval={args.interval}  out={out_dir}")
    print("=" * 72)

    nubra = login_client(args.login_mode)
    md = MarketData(nubra)
    contracts, atm, spot, step = discover_universe(md, args.expiries, args.strikes, args.fut_months)
    print(f"  spot~{spot}  ATM={atm}  step={step}", flush=True)
    exps = sorted({c['expiry'] for c in contracts})
    # Option-chain stream subscribes by ASSET:EXPIRY key, not per-contract ref_id.
    option_keys = [f"{UNDERLYING}:{e.strftime('%Y%m%d')}" for e in exps]
    print(f"  expiries: {[e.isoformat() for e in exps]}", flush=True)
    print(f"  option-chain subscription keys: {option_keys}", flush=True)

    if args.dry_run or args.preflight:
        print(f"  would subscribe (data_type=option, exchange={EXCHANGE}): {option_keys}", flush=True)
        return 0

    day = now_ist().date()
    wait_for_market_open(day)
    outputs = output_paths(out_dir, day)
    storage = TickStorage(
        outputs["raw_csv"],
        outputs["agg_csv"],
        args.interval,
    )

    close_reason = {"text": ""}
    socket = None

    def _on_connect(msg: str):
        print(f"  [socket] {msg}", flush=True)

    def _on_error(err: str):
        print(f"  [socket error] {err}", flush=True)

    def _on_close(reason: str):
        close_reason["text"] = str(reason)
        print(f"  [socket closed] {reason}", flush=True)

    socket = websocketdata.NubraDataSocket(
        client=nubra,
        on_option_data=lambda msg: on_option_chain_message(msg, storage, args.price_divisor),
        on_connect=_on_connect,
        on_close=_on_close,
        on_error=_on_error,
        reconnect=True,
        persist_subscriptions=True,
    )

    def shutdown(signum=None, frame=None):
        print("\n  stopping Nubra websocket capture...", flush=True)
        if socket is not None:
            socket.close()
        storage.close()
        raise SystemExit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    socket.connect()
    socket.subscribe(option_keys, data_type="option", exchange=EXCHANGE)

    stop_h, stop_m = parse_until(args.until)
    try:
        while True:
            t = now_ist()
            if (t.hour, t.minute) >= (stop_h, stop_m):
                print(f"\n  reached {args.until} IST - stopping.", flush=True)
                break
            time.sleep(1)
    finally:
        if socket is not None:
            socket.close()
        storage.close()
        if outputs["agg_csv"].exists():
            print(f"\n  saved {outputs['agg_csv']}", flush=True)
        if outputs["raw_csv"].exists():
            print(f"  raw ticks: {outputs['raw_csv']}", flush=True)
        if close_reason["text"]:
            print(f"  final socket status: {close_reason['text']}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import math
import os
import signal
import struct
import sys
import threading
import time
import urllib.request
from pathlib import Path
from typing import Any

import pandas as pd
from dotenv import dotenv_values

from dhanhq import DhanContext, MarketFeed, dhanhq


BASE_DIR = Path(__file__).resolve().parent
IST = dt.timezone(dt.timedelta(hours=5, minutes=30))
UNDERLYING = "NIFTY"
RAW_TICKS_BASENAME = "dhan_live_ticks"
TICKS_1S_BASENAME = "dhan_live_1s"
SECURITY_MASTER_URL = "https://images.dhan.co/api-data/api-scrip-master-detailed.csv"
SECURITY_MASTER_FILE = BASE_DIR / "api-scrip-master-detailed.csv"


RAW_COLUMNS = [
    "date",
    "datetime",
    "listed_at_ist",
    "received_at_ist",
    "security_id",
    "exchange_segment",
    "symbol",
    "native_symbol",
    "canonical_symbol",
    "underlying",
    "expiry",
    "strike",
    "instrument_type",
    "ltp",
    "ltq",
    "avg_price",
    "volume",
    "total_sell_quantity",
    "total_buy_quantity",
    "open_interest",
    "oi_day_high",
    "oi_day_low",
    "open_price",
    "high_price",
    "low_price",
    "close_price",
    "bid_price",
    "ask_price",
    "bid_qty",
    "ask_qty",
    "bid_orders",
    "ask_orders",
    "message_type",
    "raw_json",
]


ROLLUP_COLUMNS = [
    "date",
    "datetime",
    "listed_at_ist",
    "first_received_at_ist",
    "last_received_at_ist",
    "security_id",
    "exchange_segment",
    "symbol",
    "native_symbol",
    "canonical_symbol",
    "underlying",
    "expiry",
    "strike",
    "instrument_type",
    "ltp",
    "open_price",
    "high_price",
    "low_price",
    "close_price",
    "volume",
    "bid_price",
    "ask_price",
    "bid_qty",
    "ask_qty",
    "open_interest",
    "message_type",
]


def now_ist() -> dt.datetime:
    return dt.datetime.now(IST)


def wait_for_market_open(day: dt.date) -> None:
    open_at = dt.datetime.combine(day, dt.time(9, 15), tzinfo=IST)
    while True:
        current = now_ist()
        if current >= open_at:
            return
        wait_seconds = int((open_at - current).total_seconds())
        print(f"  market not open yet, waiting {wait_seconds}s until 09:15 IST...", flush=True)
        time.sleep(min(wait_seconds, 30))


def output_paths(out_dir: Path, day: dt.date) -> dict[str, Path]:
    tag = day.isoformat()
    return {
        "raw_csv": out_dir / f"{RAW_TICKS_BASENAME}_{tag}.csv",
        "agg_csv": out_dir / f"{TICKS_1S_BASENAME}_{tag}.csv",
    }


def _safe_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        num = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(num) or math.isinf(num):
        return None
    return num


def _safe_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _first_non_blank(*values: Any) -> str | None:
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return None


def _strike_text(value: Any) -> str | None:
    num = _safe_float(value)
    if num is None:
        return None
    if float(num).is_integer():
        return str(int(num))
    return str(num)


def _canon_option(underlying: str, expiry: dt.date, strike: Any, option_type: str) -> str | None:
    strike_num = _safe_float(strike)
    if strike_num is None:
        return None
    strike_txt = str(int(round(strike_num)))
    if expiry.day <= 7:
        # Dhan weekly names use the exact expiry date; keep that for broker alignment.
        return f"{underlying}{expiry.year:04d}{expiry.month:02d}{expiry.day:02d}{strike_txt}{option_type}"
    return f"{underlying}{expiry.year:04d}{expiry.month:02d}{expiry.day:02d}{strike_txt}{option_type}"


def _canon_future(underlying: str, expiry: dt.date) -> str:
    return f"{underlying}{expiry.year % 100:02d}{expiry.strftime('%b').upper()}FUT"


def canonical_symbol(underlying: str, expiry: dt.date | None, strike: Any, option_type: str | None, native_symbol: str) -> str:
    und = str(underlying or UNDERLYING).strip().upper() or UNDERLYING
    if expiry and option_type in {"CE", "PE"}:
        key = _canon_option(und, expiry, strike, option_type)
        if key:
            return key
    if expiry and option_type == "FUT":
        return _canon_future(und, expiry)
    return str(native_symbol or und).strip().upper()


def _prefer_keys(payload: Any, keys: tuple[str, ...]) -> float | None:
    if isinstance(payload, dict):
        for key in keys:
            if key in payload:
                value = _safe_float(payload.get(key))
                if value is not None:
                    return value
        for value in payload.values():
            found = _prefer_keys(value, keys)
            if found is not None:
                return found
    elif isinstance(payload, list):
        for item in payload:
            found = _prefer_keys(item, keys)
            if found is not None:
                return found
    return None


def extract_reference_price(payload: Any) -> float | None:
    return _prefer_keys(
        payload,
        (
            "last_price",
            "ltp",
            "LTP",
            "lastTradedPrice",
            "avg_price",
            "price",
        ),
    )


def midpoint_reference_price(row: Any) -> float | None:
    upper = _safe_float(row.get("SM_UPPER_LIMIT")) if hasattr(row, "get") else None
    lower = _safe_float(row.get("SM_LOWER_LIMIT")) if hasattr(row, "get") else None
    if upper is None or lower is None:
        return None
    if upper <= 0 or lower <= 0:
        return None
    return (upper + lower) / 2.0


def resolve_reference_price(dhan_client: dhanhq, future_row: Any, atm_override: float | None = None) -> tuple[float, str]:
    if atm_override is not None:
        return float(atm_override), "manual_atm"

    security_id = str(future_row["SECURITY_ID"]).strip()
    for fetch_name, fetcher in (
        ("ticker_data", dhan_client.ticker_data),
        ("quote_data", dhan_client.quote_data),
        ("ohlc_data", dhan_client.ohlc_data),
    ):
        try:
            payload = fetcher({"NSE_FNO": [security_id]})
        except Exception:
            payload = None
        price = extract_reference_price(payload)
        if price is not None:
            return price, fetch_name

    fallback = midpoint_reference_price(future_row)
    if fallback is not None:
        return fallback, "security_master_midpoint"

    raise RuntimeError("Could not resolve a Dhan reference price for the nearest NIFTY future.")


def load_credentials(cred_dir: Path | None) -> DhanContext:
    cfg: dict[str, Any] = {}
    search_dirs = [p for p in (cred_dir, BASE_DIR, BASE_DIR.parent) if p is not None]
    for folder in search_dirs:
        env_path = folder / ".env"
        if env_path.exists():
            cfg.update({k: v for k, v in dotenv_values(env_path).items() if v not in (None, "")})

    def _read_text(path: Path) -> str | None:
        if not path.exists():
            return None
        text = path.read_text(encoding="utf-8").strip()
        return text or None

    client_id = _first_non_blank(
        os.getenv("DHAN_CLIENT_ID"),
        os.getenv("DHAN_CLIENT_CODE"),
        cfg.get("DHAN_CLIENT_ID"),
        cfg.get("DHAN_CLIENT_CODE"),
        _read_text(BASE_DIR / "client_id.txt"),
    )
    access_token = _first_non_blank(
        os.getenv("DHAN_ACCESS_TOKEN"),
        os.getenv("DHAN_TOKEN"),
        cfg.get("DHAN_ACCESS_TOKEN"),
        cfg.get("DHAN_TOKEN"),
        _read_text(BASE_DIR / "access_token.txt"),
    )
    if not client_id or not access_token:
        raise RuntimeError(
            "Dhan credentials missing. Set DHAN_CLIENT_ID and DHAN_ACCESS_TOKEN in .env "
            "or place client_id.txt/access_token.txt inside Dhan/."
        )
    return DhanContext(client_id, access_token)


def ensure_security_master(path: Path, refresh: bool = False) -> pd.DataFrame:
    path.parent.mkdir(parents=True, exist_ok=True)
    need_download = refresh or not path.exists()
    if path.exists():
        modified_day = dt.datetime.fromtimestamp(path.stat().st_mtime, tz=IST).date()
        if modified_day < now_ist().date():
            need_download = True
    if need_download:
        print(f"  downloading Dhan security master -> {path}", flush=True)
        urllib.request.urlretrieve(SECURITY_MASTER_URL, path)
    df = pd.read_csv(path, low_memory=False)
    if df.empty or "SECURITY_ID" not in df.columns:
        raise RuntimeError(f"Invalid Dhan security master at {path}")
    return df


def prepare_security_master(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in ("EXCH_ID", "SEGMENT", "INSTRUMENT", "UNDERLYING_SYMBOL", "DISPLAY_NAME", "OPTION_TYPE"):
        if col in out.columns:
            out[col] = out[col].astype(str).str.strip()
    out["SECURITY_ID"] = out["SECURITY_ID"].astype(str).str.strip()
    out["SM_EXPIRY_DATE"] = pd.to_datetime(out.get("SM_EXPIRY_DATE"), errors="coerce").dt.date
    out["STRIKE_PRICE"] = pd.to_numeric(out.get("STRIKE_PRICE"), errors="coerce")
    out["LOT_SIZE"] = pd.to_numeric(out.get("LOT_SIZE"), errors="coerce")
    return out


def resolve_nifty_universe(master: pd.DataFrame, dhan_client: dhanhq, strikes: int, expiries: int, fut_months: int, atm_override: float | None = None) -> tuple[list[tuple[int, str, int]], dict[str, dict[str, Any]], dict[str, Any]]:
    base = master[
        (master["EXCH_ID"].astype(str).str.upper() == "NSE")
        & (master["SEGMENT"].astype(str).str.upper() == "D")
        & (master["UNDERLYING_SYMBOL"].astype(str).str.upper() == UNDERLYING)
    ].copy()
    if base.empty:
        raise RuntimeError("No NIFTY derivative rows found in Dhan security master.")

    today = now_ist().date()
    base = base[base["SM_EXPIRY_DATE"].notna()]
    futures = base[base["INSTRUMENT"].astype(str).str.upper().str.startswith("FUT")].copy()
    options = base[base["INSTRUMENT"].astype(str).str.upper().str.startswith("OPT")].copy()
    futures = futures[futures["SM_EXPIRY_DATE"] >= today].sort_values(["SM_EXPIRY_DATE", "SECURITY_ID"])
    options = options[options["SM_EXPIRY_DATE"] >= today].sort_values(["SM_EXPIRY_DATE", "STRIKE_PRICE", "SECURITY_ID"])
    if futures.empty or options.empty:
        raise RuntimeError("Dhan security master did not contain usable NIFTY futures/options rows.")

    chosen_futures = futures.head(max(1, int(fut_months or 1))).copy()
    reference_future = chosen_futures.iloc[0]
    reference_price, reference_source = resolve_reference_price(
        dhan_client,
        reference_future,
        atm_override=atm_override,
    )

    option_expiries = sorted(e for e in options["SM_EXPIRY_DATE"].dropna().unique().tolist() if e >= today)
    chosen_expiries = option_expiries[: max(1, int(expiries or 1))]
    nearest_expiry = chosen_expiries[0]
    option_slice = options[options["SM_EXPIRY_DATE"] == nearest_expiry].copy()
    strike_values = sorted(v for v in option_slice["STRIKE_PRICE"].dropna().unique().tolist() if v > 0)
    if not strike_values:
        raise RuntimeError("Could not resolve Dhan option strikes for the selected expiries.")
    step_candidates = [b - a for a, b in zip(strike_values, strike_values[1:]) if b > a]
    step = min(step_candidates) if step_candidates else 50.0
    atm = float(atm_override) if atm_override is not None else round(reference_price / step) * step
    target_strikes = {round(atm + step * offset, 4) for offset in range(-int(strikes or 0), int(strikes or 0) + 1)}
    selected_options = options[
        options["SM_EXPIRY_DATE"].isin(chosen_expiries)
        & options["OPTION_TYPE"].astype(str).str.upper().isin({"CE", "PE"})
        & options["STRIKE_PRICE"].round(4).isin(target_strikes)
    ].copy()
    if selected_options.empty:
        raise RuntimeError("No Dhan option contracts matched the requested ATM-centered strike window.")

    rows = pd.concat([chosen_futures, selected_options], ignore_index=True)
    rows = rows.drop_duplicates(subset=["SECURITY_ID"]).copy()
    rows["OPTION_TYPE"] = rows["OPTION_TYPE"].fillna("").astype(str).str.upper()

    instruments: list[tuple[int, str, int]] = []
    meta_by_security: dict[str, dict[str, Any]] = {}
    for _, row in rows.iterrows():
        expiry = row.get("SM_EXPIRY_DATE")
        option_type = row.get("OPTION_TYPE") if row.get("OPTION_TYPE") in {"CE", "PE"} else "FUT"
        native_symbol = str(row.get("DISPLAY_NAME") or row.get("SYMBOL_NAME") or row.get("SECURITY_ID")).strip()
        meta = {
            "security_id": str(row["SECURITY_ID"]).strip(),
            "exchange_segment": "NSE_FNO",
            "native_symbol": native_symbol,
            "symbol": native_symbol,
            "canonical_symbol": canonical_symbol(UNDERLYING, expiry, row.get("STRIKE_PRICE"), option_type, native_symbol),
            "underlying": UNDERLYING,
            "expiry": expiry.isoformat() if hasattr(expiry, "isoformat") else None,
            "strike": _strike_text(row.get("STRIKE_PRICE")),
            "instrument_type": option_type,
            "lot_size": _safe_int(row.get("LOT_SIZE")),
        }
        meta_by_security[meta["security_id"]] = meta
        instruments.append((MarketFeed.NSE_FNO, meta["security_id"], MarketFeed.Full))

    universe_info = {
        "reference_price": round(float(reference_price), 4),
        "reference_source": reference_source,
        "atm": round(float(atm), 4),
        "step": round(float(step), 4),
        "future_symbols": chosen_futures["DISPLAY_NAME"].dropna().astype(str).tolist(),
        "option_expiries": [e.isoformat() for e in chosen_expiries],
        "instrument_count": len(instruments),
    }
    return instruments, meta_by_security, universe_info


class ComparableMarketFeed(MarketFeed):
    def _parse_depth(self, binary_blob: bytes) -> list[dict[str, Any]]:
        packet_format = "<IIHHff"
        packet_size = struct.calcsize(packet_format)
        out = []
        for i in range(5):
            start = i * packet_size
            end = start + packet_size
            bid_qty, ask_qty, bid_orders, ask_orders, bid_price, ask_price = struct.unpack(packet_format, binary_blob[start:end])
            out.append(
                {
                    "bid_quantity": bid_qty,
                    "ask_quantity": ask_qty,
                    "bid_orders": bid_orders,
                    "ask_orders": ask_orders,
                    "bid_price": round(float(bid_price), 2),
                    "ask_price": round(float(ask_price), 2),
                }
            )
        return out

    def process_ticker(self, data):
        _, _, exchange_segment, security_id, ltp, ltt_epoch = struct.unpack("<BHBIfI", data[0:16])
        return {
            "type": "Ticker Data",
            "exchange_segment": exchange_segment,
            "security_id": str(security_id),
            "LTP": round(float(ltp), 2),
            "LTT_epoch": int(ltt_epoch),
        }

    def process_quote(self, data):
        unpacked = struct.unpack("<BHBIfHIfIIIffff", data[0:50])
        return {
            "type": "Quote Data",
            "exchange_segment": unpacked[2],
            "security_id": str(unpacked[3]),
            "LTP": round(float(unpacked[4]), 2),
            "LTQ": int(unpacked[5]),
            "LTT_epoch": int(unpacked[6]),
            "avg_price": round(float(unpacked[7]), 2),
            "volume": int(unpacked[8]),
            "total_sell_quantity": int(unpacked[9]),
            "total_buy_quantity": int(unpacked[10]),
            "open": round(float(unpacked[11]), 2),
            "close": round(float(unpacked[12]), 2),
            "high": round(float(unpacked[13]), 2),
            "low": round(float(unpacked[14]), 2),
        }

    def process_prev_close(self, data):
        unpacked = struct.unpack("<BHBIfI", data[0:16])
        return {
            "type": "Previous Close",
            "exchange_segment": unpacked[2],
            "security_id": str(unpacked[3]),
            "prev_close": round(float(unpacked[4]), 2),
            "prev_OI": int(unpacked[5]),
        }

    def process_oi(self, data):
        unpacked = struct.unpack("<BHBII", data[0:12])
        return {
            "type": "OI Data",
            "exchange_segment": unpacked[2],
            "security_id": str(unpacked[3]),
            "OI": int(unpacked[4]),
        }

    def process_full(self, data):
        unpacked = struct.unpack("<BHBIfHIfIIIIIIffff100s", data[0:162])
        return {
            "type": "Full Data",
            "exchange_segment": unpacked[2],
            "security_id": str(unpacked[3]),
            "LTP": round(float(unpacked[4]), 2),
            "LTQ": int(unpacked[5]),
            "LTT_epoch": int(unpacked[6]),
            "avg_price": round(float(unpacked[7]), 2),
            "volume": int(unpacked[8]),
            "total_sell_quantity": int(unpacked[9]),
            "total_buy_quantity": int(unpacked[10]),
            "OI": int(unpacked[11]),
            "oi_day_high": int(unpacked[12]),
            "oi_day_low": int(unpacked[13]),
            "open": round(float(unpacked[14]), 2),
            "close": round(float(unpacked[15]), 2),
            "high": round(float(unpacked[16]), 2),
            "low": round(float(unpacked[17]), 2),
            "depth": self._parse_depth(unpacked[18]),
        }


def bucket_start(ts: dt.datetime, interval: str) -> dt.datetime:
    if interval == "1m":
        return ts.replace(second=0, microsecond=0)
    return ts.replace(microsecond=0)


class TickStorage:
    def __init__(self, raw_csv_path: Path, rollup_csv_path: Path, interval: str):
        self.raw_csv_path = raw_csv_path
        self.rollup_csv_path = rollup_csv_path
        self.interval = interval
        self.lock = threading.Lock()
        self.pending: dict[str, dict[str, Any]] = {}
        self._init_csv(self.raw_csv_path, RAW_COLUMNS)
        self._init_csv(self.rollup_csv_path, ROLLUP_COLUMNS)

    def _init_csv(self, path: Path, header: list[str]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            return
        with path.open("w", newline="", encoding="utf-8") as handle:
            csv.writer(handle).writerow(header)

    def persist(self, tick: dict[str, Any]) -> None:
        with self.lock:
            self._append_raw(tick)
            self._update_rollup(tick)

    def close(self) -> None:
        with self.lock:
            for key in sorted(self.pending):
                self._append_rollup(self.pending[key])
            self.pending.clear()

    def _append_raw(self, tick: dict[str, Any]) -> None:
        with self.raw_csv_path.open("a", newline="", encoding="utf-8") as handle:
            csv.writer(handle).writerow([tick.get(col) for col in RAW_COLUMNS])

    def _update_rollup(self, tick: dict[str, Any]) -> None:
        listed_at = dt.datetime.fromisoformat(str(tick["listed_at_ist"]))
        bucket_dt = bucket_start(listed_at, self.interval)
        key = tick["canonical_symbol"]
        bucket = self.pending.get(key)
        if bucket is not None and bucket["datetime"] != bucket_dt.isoformat():
            self._append_rollup(bucket)
            bucket = None

        ltp = _safe_float(tick.get("ltp"))
        if bucket is None:
            bucket = {
                "date": bucket_dt.date().isoformat(),
                "datetime": bucket_dt.isoformat(),
                "listed_at_ist": tick.get("listed_at_ist"),
                "first_received_at_ist": tick.get("received_at_ist"),
                "last_received_at_ist": tick.get("received_at_ist"),
                "security_id": tick.get("security_id"),
                "exchange_segment": tick.get("exchange_segment"),
                "symbol": tick.get("symbol"),
                "native_symbol": tick.get("native_symbol"),
                "canonical_symbol": tick.get("canonical_symbol"),
                "underlying": tick.get("underlying"),
                "expiry": tick.get("expiry"),
                "strike": tick.get("strike"),
                "instrument_type": tick.get("instrument_type"),
                "ltp": ltp,
                "open_price": ltp,
                "high_price": ltp,
                "low_price": ltp,
                "close_price": ltp,
                "volume": _safe_int(tick.get("volume")),
                "bid_price": _safe_float(tick.get("bid_price")),
                "ask_price": _safe_float(tick.get("ask_price")),
                "bid_qty": _safe_int(tick.get("bid_qty")),
                "ask_qty": _safe_int(tick.get("ask_qty")),
                "open_interest": _safe_int(tick.get("open_interest")),
                "message_type": tick.get("message_type"),
            }
            self.pending[key] = bucket
            return

        if ltp is not None:
            prices = [v for v in (bucket["high_price"], bucket["low_price"], ltp) if v is not None]
            bucket["ltp"] = ltp
            bucket["close_price"] = ltp
            bucket["high_price"] = max(prices) if prices else None
            bucket["low_price"] = min(prices) if prices else None
            if bucket["open_price"] is None:
                bucket["open_price"] = ltp
        for field in ("volume", "bid_price", "ask_price", "bid_qty", "ask_qty", "open_interest", "message_type"):
            value = tick.get(field)
            if value not in (None, ""):
                bucket[field] = value
        if tick.get("listed_at_ist"):
            bucket["listed_at_ist"] = tick["listed_at_ist"]
        if tick.get("received_at_ist"):
            bucket["last_received_at_ist"] = tick["received_at_ist"]

    def _append_rollup(self, row: dict[str, Any]) -> None:
        with self.rollup_csv_path.open("a", newline="", encoding="utf-8") as handle:
            csv.writer(handle).writerow([row.get(col) for col in ROLLUP_COLUMNS])


def listed_timestamp_ist(message: dict[str, Any], fallback: dt.datetime) -> dt.datetime:
    epoch = _safe_int(message.get("LTT_epoch"))
    if epoch is None or epoch <= 0:
        return fallback
    return dt.datetime.fromtimestamp(epoch, tz=dt.timezone.utc).astimezone(IST)


def normalize_tick(message: dict[str, Any], meta_by_security: dict[str, dict[str, Any]], received_at: dt.datetime) -> dict[str, Any] | None:
    if not isinstance(message, dict):
        return None
    security_id = str(message.get("security_id") or "").strip()
    if not security_id:
        return None
    meta = meta_by_security.get(security_id)
    if not meta:
        return None
    listed_at = listed_timestamp_ist(message, received_at)
    depth = message.get("depth") or []
    best = depth[0] if depth else {}
    return {
        "date": listed_at.date().isoformat(),
        "datetime": listed_at.isoformat(),
        "listed_at_ist": listed_at.isoformat(),
        "received_at_ist": received_at.isoformat(),
        "security_id": security_id,
        "exchange_segment": meta["exchange_segment"],
        "symbol": meta["symbol"],
        "native_symbol": meta["native_symbol"],
        "canonical_symbol": meta["canonical_symbol"],
        "underlying": meta["underlying"],
        "expiry": meta["expiry"],
        "strike": meta["strike"],
        "instrument_type": meta["instrument_type"],
        "ltp": _safe_float(message.get("LTP")),
        "ltq": _safe_int(message.get("LTQ")),
        "avg_price": _safe_float(message.get("avg_price")),
        "volume": _safe_int(message.get("volume")),
        "total_sell_quantity": _safe_int(message.get("total_sell_quantity")),
        "total_buy_quantity": _safe_int(message.get("total_buy_quantity")),
        "open_interest": _safe_int(message.get("OI")),
        "oi_day_high": _safe_int(message.get("oi_day_high")),
        "oi_day_low": _safe_int(message.get("oi_day_low")),
        "open_price": _safe_float(message.get("open")),
        "high_price": _safe_float(message.get("high")),
        "low_price": _safe_float(message.get("low")),
        "close_price": _safe_float(message.get("close")),
        "bid_price": _safe_float(best.get("bid_price")),
        "ask_price": _safe_float(best.get("ask_price")),
        "bid_qty": _safe_int(best.get("bid_quantity")),
        "ask_qty": _safe_int(best.get("ask_quantity")),
        "bid_orders": _safe_int(best.get("bid_orders")),
        "ask_orders": _safe_int(best.get("ask_orders")),
        "message_type": message.get("type"),
        "raw_json": json.dumps(message, default=str),
    }


def parse_until(value: str) -> tuple[int, int]:
    hh, mm = str(value).split(":", 1)
    return int(hh), int(mm)


def print_universe(universe_info: dict[str, Any], meta_by_security: dict[str, dict[str, Any]]) -> None:
    print(f"  reference price  : {universe_info['reference_price']}", flush=True)
    print(f"  reference source : {universe_info.get('reference_source', 'unknown')}", flush=True)
    print(f"  ATM / step       : {universe_info['atm']} / {universe_info['step']}", flush=True)
    print(f"  option expiries  : {', '.join(universe_info['option_expiries'])}", flush=True)
    print(f"  instruments      : {universe_info['instrument_count']}", flush=True)
    sample = list(meta_by_security.values())[:12]
    for item in sample:
        print(
            f"    - {item['security_id']} | {item['native_symbol']} | {item['canonical_symbol']}",
            flush=True,
        )
    if len(meta_by_security) > len(sample):
        print(f"    ... {len(meta_by_security) - len(sample)} more", flush=True)


def main() -> int:
    ap = argparse.ArgumentParser(description="Capture Dhan live NIFTY market data to raw tick and 1-second CSV files.")
    ap.add_argument("--strikes", type=int, default=15, help="ATM-centered strikes either side")
    ap.add_argument("--expiries", type=int, default=1, help="nearest option expiries to subscribe")
    ap.add_argument("--fut-months", type=int, default=3, help="number of NIFTY futures to subscribe")
    ap.add_argument("--interval", default="1s", choices=["1s", "1m"], help="aggregation bucket size for saved output")
    ap.add_argument("--out", default=str(BASE_DIR), help="output directory (defaults to Dhan/)")
    ap.add_argument("--cred-dir", default=None, help="folder holding .env for Dhan credentials")
    ap.add_argument("--until", default="15:32", help="stop time in IST HH:MM")
    ap.add_argument("--atm", type=float, default=None, help="optional manual ATM override")
    ap.add_argument("--dry-run", action="store_true", help="show resolved Dhan universe, then exit")
    ap.add_argument("--preflight", action="store_true", help="validate login + security master + universe, then exit")
    ap.add_argument("--refresh-master", action="store_true", help="force download the latest Dhan security master CSV")
    args = ap.parse_args()

    out_dir = Path(args.out).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    cred_dir = Path(args.cred_dir).resolve() if args.cred_dir else None

    print("=" * 72)
    print(f"  DHAN live websocket capture  |  {now_ist().strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print(f"  interval={args.interval}  out={out_dir}")
    print("=" * 72)

    context = load_credentials(cred_dir)
    dhan_client = dhanhq(context)
    master = prepare_security_master(ensure_security_master(SECURITY_MASTER_FILE, refresh=args.refresh_master))
    instruments, meta_by_security, universe_info = resolve_nifty_universe(
        master,
        dhan_client,
        strikes=args.strikes,
        expiries=args.expiries,
        fut_months=args.fut_months,
        atm_override=args.atm,
    )
    print_universe(universe_info, meta_by_security)

    if args.dry_run or args.preflight:
        return 0

    day = now_ist().date()
    wait_for_market_open(day)
    outputs = output_paths(out_dir, day)
    storage = TickStorage(outputs["raw_csv"], outputs["agg_csv"], args.interval)
    stop_h, stop_m = parse_until(args.until)
    should_stop = {"value": False}
    feed_holder = {"feed": None}

    def shutdown(signum=None, frame=None):
        should_stop["value"] = True
        print("\n  stopping Dhan websocket capture...", flush=True)
        feed = feed_holder.get("feed")
        if feed is not None:
            try:
                feed.close_connection()
            except Exception as exc:
                print(f"  warning: Dhan socket close failed: {exc}", flush=True)
        storage.close()
        raise SystemExit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    while not should_stop["value"]:
        current = now_ist()
        if (current.hour, current.minute) >= (stop_h, stop_m):
            print(f"\n  reached {args.until} IST - stopping.", flush=True)
            break
        try:
            feed = ComparableMarketFeed(context, instruments, version="v2")
            feed_holder["feed"] = feed
            print("  connecting Dhan market feed...", flush=True)
            feed.run_forever()
            print("  Dhan market feed connected.", flush=True)
            while not should_stop["value"]:
                current = now_ist()
                if (current.hour, current.minute) >= (stop_h, stop_m):
                    should_stop["value"] = True
                    break
                message = feed.get_data()
                received_at = now_ist()
                tick = normalize_tick(message, meta_by_security, received_at)
                if tick is not None:
                    storage.persist(tick)
        except KeyboardInterrupt:
            should_stop["value"] = True
        except Exception as exc:
            print(f"  Dhan websocket error: {exc}", flush=True)
            time.sleep(3)
        finally:
            feed = feed_holder.get("feed")
            if feed is not None:
                try:
                    feed.close_connection()
                except Exception:
                    pass
            feed_holder["feed"] = None

    storage.close()
    if outputs["agg_csv"].exists():
        print(f"\n  saved {outputs['agg_csv']}", flush=True)
    if outputs["raw_csv"].exists():
        print(f"  raw ticks: {outputs['raw_csv']}", flush=True)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\nInterrupted by user.")
    except Exception as exc:
        print(f"Startup failed: {exc}")
        sys.exit(1)

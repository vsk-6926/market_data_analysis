#!/usr/bin/env python3
"""Capture Arrow (iRage Broking) live NIFTY market data to raw tick and 1-second CSV files.

Arrow is the retail arm of iRage, so the low-latency infrastructure predates the
product. Whether that survives the trip through their SDK is exactly what this
capture is for -- the output schema is the same one Dhan/FYERS/XTS write, so
Server.py can rank it against the other feeds without any special casing.

Two streams are available and both write the same rows:

  --stream ds   (default)  wss://ds.arrow.trade      event-driven, FULL depth
  --stream hft             wss://socket.arrow.trade  throttled to --hft-latency ms

`ds` is the like-for-like comparison against the other brokers. `hft` is the
one with nanosecond server timestamps; it coalesces to a fixed interval, so
read its inter-arrival numbers as the interval you asked for, not as cadence.

Instruments are numeric tokens. The master at https://edge.arrow.trade/all is
public CSV, so the universe resolves without credentials; only the live socket
and the ATM quote need them.
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import os
import signal
import sys
import threading
import time
import urllib.request
from pathlib import Path
from typing import Any

import pandas as pd
from dotenv import dotenv_values

try:
    import duckdb
except ImportError:
    duckdb = None


BASE_DIR = Path(__file__).resolve().parent
IST = dt.timezone(dt.timedelta(hours=5, minutes=30))
UNDERLYING = "NIFTY"
RAW_TICKS_BASENAME = "arrow_live_ticks"
TICKS_DUCKDB_BASENAME = "arrow_live_ticks"
TICKS_1S_BASENAME = "arrow_live_1s"

INSTRUMENT_MASTER_URL = "https://edge.arrow.trade/all"
INSTRUMENT_MASTER_FILE = BASE_DIR / "arrow-instruments.csv"
TOKEN_CACHE_FILE = BASE_DIR / "access_token.txt"

NSEFO = "NSEFO"
# HFT exchange segments (see HFTDataStream docstring).
HFT_EXCH_NSE_FO = 1


RAW_COLUMNS = [
    "date",
    "datetime",
    "listed_at_ist",
    "received_at_ist",
    "token",
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
    "token",
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


def output_paths(out_dir: Path, day: dt.date, stream: str = "ds") -> dict[str, Path]:
    """`hft` writes its own files so both Arrow streams can run in one session
    without fighting over the same CSV and duckdb lock."""
    tag = day.isoformat()
    prefix = "arrow_hft" if stream == "hft" else "arrow"
    return {
        "raw_csv": out_dir / f"{prefix}_live_ticks_{tag}.csv",
        "raw_duckdb": out_dir / f"{prefix}_live_ticks_{tag}.duckdb",
        "agg_csv": out_dir / f"{prefix}_live_1s_{tag}.csv",
    }


def _safe_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        num = float(value)
    except (TypeError, ValueError):
        return None
    if num != num:  # NaN
        return None
    return num


def _safe_int(value: Any) -> int | None:
    num = _safe_float(value)
    if num is None:
        return None
    try:
        return int(num)
    except (TypeError, ValueError, OverflowError):
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
    """Same cross-broker option key the Dhan collector writes: NIFTY + YYYYMMDD + strike + CE/PE."""
    strike_num = _safe_float(strike)
    if strike_num is None:
        return None
    strike_txt = str(int(round(strike_num)))
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


# ---------------------------------------------------------------- credentials


def load_credentials(cred_dir: Path | None) -> dict[str, str | None]:
    cfg: dict[str, Any] = {}
    search_dirs = [p for p in (cred_dir, BASE_DIR, BASE_DIR.parent) if p is not None]
    for folder in search_dirs:
        env_path = folder / ".env"
        if env_path.exists():
            cfg.update({k: v for k, v in dotenv_values(env_path).items() if v not in (None, "")})

    def pick(*names: str) -> str | None:
        candidates: list[Any] = []
        for name in names:
            candidates.append(os.getenv(name))
        for name in names:
            candidates.append(cfg.get(name))
        return _first_non_blank(*candidates)

    return {
        "app_id": pick("ARROW_APP_ID", "ARROW_APPID"),
        "access_token": pick("ARROW_ACCESS_TOKEN", "ARROW_TOKEN"),
        "api_secret": pick("ARROW_API_SECRET", "ARROW_APP_SECRET"),
        "user_id": pick("ARROW_USER_ID", "ARROW_CLIENT_ID"),
        "password": pick("ARROW_PASSWORD"),
        "totp_secret": pick("ARROW_TOTP_SECRET"),
    }


def _read_cached_token() -> str | None:
    """Arrow tokens are day-scoped, so a token cached before today is not reused."""
    if not TOKEN_CACHE_FILE.exists():
        return None
    modified_day = dt.datetime.fromtimestamp(TOKEN_CACHE_FILE.stat().st_mtime, tz=IST).date()
    if modified_day < now_ist().date():
        return None
    text = TOKEN_CACHE_FILE.read_text(encoding="utf-8").strip()
    return text or None


def _write_cached_token(token: str) -> None:
    TOKEN_CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
    TOKEN_CACHE_FILE.write_text(token.strip() + "\n", encoding="utf-8")


def resolve_access_token(creds: dict[str, str | None], allow_auto_login: bool = True) -> str | None:
    """Explicit token wins, then today's cache, then TOTP auto-login."""
    token = _first_non_blank(creds.get("access_token"), _read_cached_token())
    if token:
        return token
    if not allow_auto_login:
        return None
    if not (creds.get("user_id") and creds.get("password") and creds.get("api_secret") and creds.get("totp_secret")):
        return None
    from pyarrow_client import ArrowClient

    client = ArrowClient(app_id=str(creds["app_id"]))
    response = client.auto_login(
        user_id=str(creds["user_id"]),
        password=str(creds["password"]),
        api_secret=str(creds["api_secret"]),
        totp_secret=str(creds["totp_secret"]),
    )
    token = _first_non_blank(response.get("token") if isinstance(response, dict) else None)
    if token:
        _write_cached_token(token)
        print("  Arrow auto-login succeeded; token cached for today.", flush=True)
    return token


# ------------------------------------------------------------ instrument master


def ensure_instrument_master(path: Path, refresh: bool = False) -> pd.DataFrame:
    path.parent.mkdir(parents=True, exist_ok=True)
    need_download = refresh or not path.exists()
    if path.exists():
        modified_day = dt.datetime.fromtimestamp(path.stat().st_mtime, tz=IST).date()
        if modified_day < now_ist().date():
            need_download = True
    if need_download:
        print(f"  downloading Arrow instrument master -> {path}", flush=True)
        urllib.request.urlretrieve(INSTRUMENT_MASTER_URL, path)
    df = pd.read_csv(path, low_memory=False)
    if df.empty or "Token" not in df.columns:
        raise RuntimeError(f"Invalid Arrow instrument master at {path}")
    return df


def prepare_instrument_master(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in ("Exchange", "Segment", "ExchSeg", "Symbol", "TradingSymbol", "OptionType", "Underlying"):
        if col in out.columns:
            out[col] = out[col].astype(str).str.strip()
    out["Token"] = out["Token"].astype(str).str.strip()
    out["Expiry"] = pd.to_datetime(out.get("Expiry"), format="%d-%b-%Y", errors="coerce").dt.date
    out["StrikePrice"] = pd.to_numeric(out.get("StrikePrice"), errors="coerce")
    out["LotSize"] = pd.to_numeric(out.get("LotSize"), errors="coerce")
    out["PricePrecision"] = pd.to_numeric(out.get("PricePrecision"), errors="coerce")
    for col in ("Lower Band", "Upper Band"):
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    return out


def band_midpoint_price(row: Any) -> float | None:
    """Circuit bands sit symmetrically around the previous close, so their midpoint
    is a usable ATM anchor when no authenticated quote is available."""
    upper = _safe_float(row.get("Upper Band")) if hasattr(row, "get") else None
    lower = _safe_float(row.get("Lower Band")) if hasattr(row, "get") else None
    if upper is None or lower is None or upper <= 0 or lower <= 0:
        return None
    return (upper + lower) / 2.0


def _extract_reference_price(payload: Any) -> float | None:
    keys = ("ltp", "LTP", "last_price", "lastTradedPrice", "close", "avg_price", "price")
    if isinstance(payload, dict):
        for key in keys:
            if key in payload:
                value = _safe_float(payload.get(key))
                if value is not None:
                    return value
        for value in payload.values():
            found = _extract_reference_price(value)
            if found is not None:
                return found
    elif isinstance(payload, list):
        for item in payload:
            found = _extract_reference_price(item)
            if found is not None:
                return found
    return None


def resolve_reference_price(client: Any, future_row: Any, atm_override: float | None = None) -> tuple[float, str]:
    if atm_override is not None:
        return float(atm_override), "manual_atm"

    if client is not None:
        from pyarrow_client import Exchange, QuoteMode

        # REST quotes carry the same paise-scaled integers the websocket sends,
        # while the instrument master's band columns are already in rupees.
        precision = _safe_int(future_row.get("PricePrecision"))
        divisor = float(10 ** (precision if precision is not None else 2))
        trading_symbol = str(future_row.get("TradingSymbol") or "").strip()
        for mode_name, mode in (("ltp", QuoteMode.LTP), ("full", QuoteMode.FULL), ("ohlcv", QuoteMode.OHLCV)):
            try:
                payload = client.get_quote(mode, trading_symbol, Exchange.NFO)
            except Exception:
                payload = None
            price = _extract_reference_price(payload)
            if price is not None:
                return price / divisor, f"quote_{mode_name}"

    fallback = band_midpoint_price(future_row)
    if fallback is not None:
        return fallback, "instrument_master_band_midpoint"

    raise RuntimeError("Could not resolve an Arrow reference price for the nearest NIFTY future.")


def resolve_nifty_universe(
    master: pd.DataFrame,
    client: Any,
    strikes: int,
    expiries: int,
    fut_months: int,
    atm_override: float | None = None,
) -> tuple[list[str], dict[str, dict[str, Any]], dict[str, Any]]:
    base = master[
        (master["ExchSeg"].astype(str).str.upper() == NSEFO)
        & (master["Underlying"].astype(str).str.upper() == UNDERLYING)
        & (master["Symbol"].astype(str).str.upper() == UNDERLYING)
    ].copy()
    if base.empty:
        raise RuntimeError("No NIFTY derivative rows found in the Arrow instrument master.")

    today = now_ist().date()
    base = base[base["Expiry"].notna()]
    option_type = base["OptionType"].astype(str).str.upper()
    options = base[option_type.isin({"CE", "PE"})].copy()
    # Arrow marks futures with OptionType 'XX' and StrikePrice -0.01.
    futures = base[~option_type.isin({"CE", "PE"})].copy()
    futures = futures[futures["Expiry"] >= today].sort_values(["Expiry", "Token"])
    options = options[options["Expiry"] >= today].sort_values(["Expiry", "StrikePrice", "Token"])
    if futures.empty or options.empty:
        raise RuntimeError("Arrow instrument master did not contain usable NIFTY futures/options rows.")

    chosen_futures = futures.head(max(1, int(fut_months or 1))).copy()
    reference_future = chosen_futures.iloc[0]
    reference_price, reference_source = resolve_reference_price(client, reference_future, atm_override=atm_override)

    option_expiries = sorted(e for e in options["Expiry"].dropna().unique().tolist() if e >= today)
    chosen_expiries = option_expiries[: max(1, int(expiries or 1))]
    nearest_expiry = chosen_expiries[0]
    option_slice = options[options["Expiry"] == nearest_expiry].copy()
    strike_values = sorted(v for v in option_slice["StrikePrice"].dropna().unique().tolist() if v > 0)
    if not strike_values:
        raise RuntimeError("Could not resolve Arrow option strikes for the selected expiries.")
    step_candidates = [b - a for a, b in zip(strike_values, strike_values[1:]) if b > a]
    step = min(step_candidates) if step_candidates else 50.0
    atm = float(atm_override) if atm_override is not None else round(reference_price / step) * step
    target_strikes = {round(atm + step * offset, 4) for offset in range(-int(strikes or 0), int(strikes or 0) + 1)}
    selected_options = options[
        options["Expiry"].isin(chosen_expiries)
        & options["StrikePrice"].round(4).isin(target_strikes)
    ].copy()
    if selected_options.empty:
        raise RuntimeError("No Arrow option contracts matched the requested ATM-centered strike window.")

    rows = pd.concat([chosen_futures, selected_options], ignore_index=True)
    rows = rows.drop_duplicates(subset=["Token"]).copy()

    tokens: list[str] = []
    meta_by_token: dict[str, dict[str, Any]] = {}
    for _, row in rows.iterrows():
        expiry = row.get("Expiry")
        raw_type = str(row.get("OptionType") or "").strip().upper()
        instrument_type = raw_type if raw_type in {"CE", "PE"} else "FUT"
        native_symbol = str(row.get("TradingSymbol") or row.get("Symbol") or row.get("Token")).strip()
        precision = _safe_int(row.get("PricePrecision"))
        token = str(row["Token"]).strip()
        meta = {
            "token": token,
            "exchange_segment": NSEFO,
            "native_symbol": native_symbol,
            "symbol": native_symbol,
            "canonical_symbol": canonical_symbol(
                UNDERLYING,
                expiry,
                row.get("StrikePrice") if instrument_type in {"CE", "PE"} else None,
                instrument_type,
                native_symbol,
            ),
            "underlying": UNDERLYING,
            "expiry": expiry.isoformat() if hasattr(expiry, "isoformat") else None,
            "strike": _strike_text(row.get("StrikePrice")) if instrument_type in {"CE", "PE"} else None,
            "instrument_type": instrument_type,
            "lot_size": _safe_int(row.get("LotSize")),
            # Arrow ships integer prices scaled by PricePrecision decimal places.
            "price_divisor": float(10 ** (precision if precision is not None else 2)),
        }
        meta_by_token[token] = meta
        tokens.append(token)

    universe_info = {
        "reference_price": round(float(reference_price), 4),
        "reference_source": reference_source,
        "atm": round(float(atm), 4),
        "step": round(float(step), 4),
        "future_symbols": chosen_futures["TradingSymbol"].dropna().astype(str).tolist(),
        "option_expiries": [e.isoformat() for e in chosen_expiries],
        "instrument_count": len(tokens),
    }
    return tokens, meta_by_token, universe_info


# ------------------------------------------------------------------- storage


def bucket_start(ts: dt.datetime, interval: str) -> dt.datetime:
    if interval == "1m":
        return ts.replace(second=0, microsecond=0)
    return ts.replace(microsecond=0)


class TickStorage:
    """Buffered writer.

    The obvious implementation -- open the CSV and a fresh duckdb connection per
    tick -- measures 56 ticks/sec, which is fine for the ~12/sec `ds` stream and
    hopeless for the HFT stream's ~650/sec. So handles stay open and duckdb rows
    are inserted in batches; the flush is time- and count-bounded so a crash
    costs at most FLUSH_SECONDS of data.
    """

    FLUSH_ROWS = 500
    FLUSH_SECONDS = 2.0

    def __init__(self, raw_csv_path: Path, duckdb_path: Path, rollup_csv_path: Path, interval: str,
                 duckdb_enabled: bool = True):
        self.raw_csv_path = raw_csv_path
        self.duckdb_path = duckdb_path
        self.rollup_csv_path = rollup_csv_path
        self.interval = interval
        self.duckdb_enabled = duckdb is not None and duckdb_enabled
        self.lock = threading.Lock()
        self.pending: dict[str, dict[str, Any]] = {}
        self.tick_count = 0
        self.dropped = 0
        self._init_csv(self.raw_csv_path, RAW_COLUMNS)
        self._init_csv(self.rollup_csv_path, ROLLUP_COLUMNS)
        self._raw_handle = self.raw_csv_path.open("a", newline="", encoding="utf-8")
        self._raw_writer = csv.writer(self._raw_handle)
        self._rollup_handle = self.rollup_csv_path.open("a", newline="", encoding="utf-8")
        self._rollup_writer = csv.writer(self._rollup_handle)
        self._raw_buffer: list[list[Any]] = []
        self._rollup_buffer: list[list[Any]] = []
        self._last_flush = time.time()
        self._conn = None
        if self.duckdb_enabled:
            self._init_duckdb()
            self._conn = duckdb.connect(str(self.duckdb_path))

    def _init_csv(self, path: Path, header: list[str]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            return
        with path.open("w", newline="", encoding="utf-8") as handle:
            csv.writer(handle).writerow(header)

    def _init_duckdb(self) -> None:
        conn = duckdb.connect(str(self.duckdb_path))
        conn.execute("""
            CREATE TABLE IF NOT EXISTS live_ticks (
                date VARCHAR,
                datetime VARCHAR,
                listed_at_ist VARCHAR,
                received_at_ist VARCHAR,
                token VARCHAR,
                exchange_segment VARCHAR,
                symbol VARCHAR,
                native_symbol VARCHAR,
                canonical_symbol VARCHAR,
                underlying VARCHAR,
                expiry VARCHAR,
                strike VARCHAR,
                instrument_type VARCHAR,
                ltp DOUBLE,
                ltq BIGINT,
                avg_price DOUBLE,
                volume BIGINT,
                total_sell_quantity BIGINT,
                total_buy_quantity BIGINT,
                open_interest BIGINT,
                oi_day_high BIGINT,
                oi_day_low BIGINT,
                open_price DOUBLE,
                high_price DOUBLE,
                low_price DOUBLE,
                close_price DOUBLE,
                bid_price DOUBLE,
                ask_price DOUBLE,
                bid_qty BIGINT,
                ask_qty BIGINT,
                bid_orders BIGINT,
                ask_orders BIGINT,
                message_type VARCHAR,
                raw_json VARCHAR
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS live_1s (
                date VARCHAR,
                datetime VARCHAR,
                listed_at_ist VARCHAR,
                first_received_at_ist VARCHAR,
                last_received_at_ist VARCHAR,
                token VARCHAR,
                exchange_segment VARCHAR,
                symbol VARCHAR,
                native_symbol VARCHAR,
                canonical_symbol VARCHAR,
                underlying VARCHAR,
                expiry VARCHAR,
                strike VARCHAR,
                instrument_type VARCHAR,
                ltp DOUBLE,
                open_price DOUBLE,
                high_price DOUBLE,
                low_price DOUBLE,
                close_price DOUBLE,
                volume BIGINT,
                bid_price DOUBLE,
                ask_price DOUBLE,
                bid_qty BIGINT,
                ask_qty BIGINT,
                open_interest BIGINT,
                message_type VARCHAR
            )
        """)
        conn.close()

    def persist(self, tick: dict[str, Any]) -> None:
        with self.lock:
            self._append_raw(tick)
            self._update_rollup(tick)
            self.tick_count += 1
            if (len(self._raw_buffer) >= self.FLUSH_ROWS
                    or (time.time() - self._last_flush) >= self.FLUSH_SECONDS):
                self._flush()

    def close(self) -> None:
        with self.lock:
            for key in sorted(self.pending):
                self._append_rollup(self.pending[key])
            self.pending.clear()
            self._flush()
            for handle in (self._raw_handle, self._rollup_handle):
                try:
                    handle.close()
                except Exception:
                    pass
            if self._conn is not None:
                try:
                    self._conn.close()
                except Exception:
                    pass
                self._conn = None

    def _flush(self) -> None:
        """Caller holds self.lock."""
        self._raw_handle.flush()
        self._rollup_handle.flush()
        if self._conn is not None:
            for table, cols, buf in (("live_ticks", RAW_COLUMNS, self._raw_buffer),
                                     ("live_1s", ROLLUP_COLUMNS, self._rollup_buffer)):
                if not buf:
                    continue
                try:
                    self._conn.executemany(
                        f"INSERT INTO {table} VALUES ({', '.join(['?'] * len(cols))})", buf)
                except Exception as exc:
                    # The CSV is the source of truth; never lose ticks over duckdb.
                    self.dropped += len(buf)
                    print(f"  warning: duckdb insert failed ({exc}); CSV still complete.", flush=True)
                buf.clear()
        else:
            self._raw_buffer.clear()
            self._rollup_buffer.clear()
        self._last_flush = time.time()

    def _append_raw(self, tick: dict[str, Any]) -> None:
        row = [tick.get(col) for col in RAW_COLUMNS]
        self._raw_writer.writerow(row)
        if self._conn is not None:
            self._raw_buffer.append(row)

    def _update_rollup(self, tick: dict[str, Any]) -> None:
        listed_at = dt.datetime.fromisoformat(str(tick["listed_at_ist"]))
        bucket_dt = bucket_start(listed_at, self.interval)
        key = tick["canonical_symbol"]
        bucket = self.pending.get(key)
        if bucket is not None and bucket["datetime"] != bucket_dt.isoformat():
            self._append_rollup(bucket)
            bucket = None

        ltp = _safe_float(tick.get("ltp"))
        if bucket is None and ltp is None:
            # Ignore OI-only / non-price packets as standalone 1s rows.
            return
        if bucket is None:
            bucket = {
                "date": bucket_dt.date().isoformat(),
                "datetime": bucket_dt.isoformat(),
                "listed_at_ist": tick.get("listed_at_ist"),
                "first_received_at_ist": tick.get("received_at_ist"),
                "last_received_at_ist": tick.get("received_at_ist"),
                "token": tick.get("token"),
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
        out = [row.get(col) for col in ROLLUP_COLUMNS]
        self._rollup_writer.writerow(out)
        if self._conn is not None:
            self._rollup_buffer.append(out)


# ----------------------------------------------------------------- normalize


def _epoch_seconds_to_ist(value: Any, fallback: dt.datetime) -> dt.datetime:
    """Arrow stamps last-trade time as epoch seconds. Some feeds send it already
    shifted into IST, so pick whichever reading lands closer to the receive time."""
    epoch = _safe_int(value)
    if epoch is None or epoch <= 0:
        return fallback
    utc_based = dt.datetime.fromtimestamp(epoch, tz=dt.timezone.utc).astimezone(IST)
    ist_epoch_based = dt.datetime(1970, 1, 1, tzinfo=IST) + dt.timedelta(seconds=epoch)
    if abs((ist_epoch_based - fallback).total_seconds()) < abs((utc_based - fallback).total_seconds()):
        return ist_epoch_based
    return utc_based


def _scaled(value: Any, divisor: float) -> float | None:
    num = _safe_float(value)
    if num is None:
        return None
    return num / divisor if divisor else num


def _tick_to_dict(tick: Any) -> dict[str, Any]:
    if isinstance(tick, dict):
        return dict(tick)
    out: dict[str, Any] = {}
    for attr in (
        "token", "ltp", "mode", "open", "high", "low", "close", "volume",
        "net_change", "change_flag", "ltq", "avg_price", "total_buy_quantity",
        "total_sell_quantity", "ltt", "time", "oi", "oi_day_high", "oi_day_low",
        "upper_limit", "lower_limit", "bids", "asks",
    ):
        value = getattr(tick, attr, None)
        if value is not None:
            out[attr] = value
    return out


def normalize_ds_tick(tick: Any, meta_by_token: dict[str, dict[str, Any]], received_at: dt.datetime) -> dict[str, Any] | None:
    """MarketTick from DataStream (ds.arrow.trade) -> capture row."""
    payload = _tick_to_dict(tick)
    token = str(payload.get("token") or "").strip()
    meta = meta_by_token.get(token)
    if not meta:
        return None
    divisor = float(meta.get("price_divisor") or 100.0)
    listed_at = _epoch_seconds_to_ist(payload.get("ltt") or payload.get("time"), received_at)
    bids = payload.get("bids") or []
    asks = payload.get("asks") or []
    best_bid = bids[0] if bids else {}
    best_ask = asks[0] if asks else {}
    return {
        "date": listed_at.date().isoformat(),
        "datetime": listed_at.isoformat(),
        "listed_at_ist": listed_at.isoformat(),
        "received_at_ist": received_at.isoformat(),
        "token": token,
        "exchange_segment": meta["exchange_segment"],
        "symbol": meta["symbol"],
        "native_symbol": meta["native_symbol"],
        "canonical_symbol": meta["canonical_symbol"],
        "underlying": meta["underlying"],
        "expiry": meta["expiry"],
        "strike": meta["strike"],
        "instrument_type": meta["instrument_type"],
        "ltp": _scaled(payload.get("ltp"), divisor),
        "ltq": _safe_int(payload.get("ltq")),
        "avg_price": _scaled(payload.get("avg_price"), divisor),
        "volume": _safe_int(payload.get("volume")),
        "total_sell_quantity": _safe_int(payload.get("total_sell_quantity")),
        "total_buy_quantity": _safe_int(payload.get("total_buy_quantity")),
        "open_interest": _safe_int(payload.get("oi")),
        "oi_day_high": _safe_int(payload.get("oi_day_high")),
        "oi_day_low": _safe_int(payload.get("oi_day_low")),
        "open_price": _scaled(payload.get("open"), divisor),
        "high_price": _scaled(payload.get("high"), divisor),
        "low_price": _scaled(payload.get("low"), divisor),
        "close_price": _scaled(payload.get("close"), divisor),
        "bid_price": _scaled(best_bid.get("price") if isinstance(best_bid, dict) else None, divisor),
        "ask_price": _scaled(best_ask.get("price") if isinstance(best_ask, dict) else None, divisor),
        "bid_qty": _safe_int(best_bid.get("quantity") if isinstance(best_bid, dict) else None),
        "ask_qty": _safe_int(best_ask.get("quantity") if isinstance(best_ask, dict) else None),
        "bid_orders": _safe_int(best_bid.get("orders") if isinstance(best_bid, dict) else None),
        "ask_orders": _safe_int(best_ask.get("orders") if isinstance(best_ask, dict) else None),
        "message_type": payload.get("mode") or "quote",
        "raw_json": json.dumps(payload, default=str),
    }


def normalize_hft_tick(payload: dict[str, Any], meta_by_token: dict[str, dict[str, Any]], received_at: dt.datetime) -> dict[str, Any] | None:
    """LTP/full dict from HFTDataStream (socket.arrow.trade) -> capture row."""
    if not isinstance(payload, dict):
        return None
    token = str(payload.get("token") or "").strip()
    meta = meta_by_token.get(token)
    if not meta:
        return None
    divisor = float(meta.get("price_divisor") or 100.0)
    # The full packet's `ts` is NOT a wall clock: observed values sit around
    # 3.8e14 ns (~4 days), i.e. a monotonic/uptime counter, and treating it as
    # epoch-ns yields 1970 dates. Its deltas are still useful for sequencing, so
    # it is preserved in raw_json; the listed time comes from `ltt`, which is
    # genuine epoch seconds - the same field the ds stream uses.
    listed_at = _epoch_seconds_to_ist(payload.get("ltt"), received_at)
    bid_px = payload.get("bid_px") or []
    ask_px = payload.get("ask_px") or []
    bid_size = payload.get("bid_size") or []
    ask_size = payload.get("ask_size") or []
    bid_ord = payload.get("bid_ord") or []
    ask_ord = payload.get("ask_ord") or []
    is_full = _safe_int(payload.get("pkt_type")) == 2
    return {
        "date": listed_at.date().isoformat(),
        "datetime": listed_at.isoformat(),
        "listed_at_ist": listed_at.isoformat(),
        "received_at_ist": received_at.isoformat(),
        "token": token,
        "exchange_segment": meta["exchange_segment"],
        "symbol": meta["symbol"],
        "native_symbol": meta["native_symbol"],
        "canonical_symbol": meta["canonical_symbol"],
        "underlying": meta["underlying"],
        "expiry": meta["expiry"],
        "strike": meta["strike"],
        "instrument_type": meta["instrument_type"],
        "ltp": _scaled(payload.get("ltp"), divisor),
        "ltq": _safe_int(payload.get("ltq")),
        "avg_price": _scaled(payload.get("vwap"), divisor),
        "volume": _safe_int(payload.get("volume")),
        "total_sell_quantity": _safe_int(payload.get("tsq")),
        "total_buy_quantity": _safe_int(payload.get("tbq")),
        "open_interest": _safe_int(payload.get("oi")),
        "oi_day_high": None,
        "oi_day_low": None,
        "open_price": _scaled(payload.get("open"), divisor),
        "high_price": _scaled(payload.get("high"), divisor),
        "low_price": _scaled(payload.get("low"), divisor),
        "close_price": _scaled(payload.get("close"), divisor),
        "bid_price": _scaled(bid_px[0] if bid_px else None, divisor),
        "ask_price": _scaled(ask_px[0] if ask_px else None, divisor),
        "bid_qty": _safe_int(bid_size[0] if bid_size else None),
        "ask_qty": _safe_int(ask_size[0] if ask_size else None),
        "bid_orders": _safe_int(bid_ord[0] if bid_ord else None),
        "ask_orders": _safe_int(ask_ord[0] if ask_ord else None),
        "message_type": "hft_full" if is_full else "hft_ltpc",
        "raw_json": json.dumps(payload, default=str),
    }


# ---------------------------------------------------------------------- main


def parse_until(value: str) -> tuple[int, int]:
    hh, mm = str(value).split(":", 1)
    return int(hh), int(mm)


def print_universe(universe_info: dict[str, Any], meta_by_token: dict[str, dict[str, Any]]) -> None:
    print(f"  reference price  : {universe_info['reference_price']}", flush=True)
    print(f"  reference source : {universe_info.get('reference_source', 'unknown')}", flush=True)
    print(f"  ATM / step       : {universe_info['atm']} / {universe_info['step']}", flush=True)
    print(f"  option expiries  : {', '.join(universe_info['option_expiries'])}", flush=True)
    print(f"  instruments      : {universe_info['instrument_count']}", flush=True)
    sample = list(meta_by_token.values())[:12]
    for item in sample:
        print(f"    - {item['token']} | {item['native_symbol']} | {item['canonical_symbol']}", flush=True)
    if len(meta_by_token) > len(sample):
        print(f"    ... {len(meta_by_token) - len(sample)} more", flush=True)


def _chunked(items: list[Any], size: int):
    for start in range(0, len(items), size):
        yield items[start:start + size]


def main() -> int:
    ap = argparse.ArgumentParser(description="Capture Arrow live NIFTY market data to raw tick and 1-second CSV files.")
    ap.add_argument("--strikes", type=int, default=15, help="ATM-centered strikes either side")
    ap.add_argument("--expiries", type=int, default=1, help="nearest option expiries to subscribe")
    ap.add_argument("--fut-months", type=int, default=3, help="number of NIFTY futures to subscribe")
    ap.add_argument("--interval", default="1s", choices=["1s", "1m"], help="aggregation bucket size for saved output")
    ap.add_argument("--out", default=str(BASE_DIR), help="output directory (defaults to Arrow/)")
    ap.add_argument("--cred-dir", default=None, help="folder holding .env for Arrow credentials")
    ap.add_argument("--until", default="15:32", help="stop time in IST HH:MM")
    ap.add_argument("--atm", type=float, default=None, help="optional manual ATM override")
    ap.add_argument("--stream", default="ds", choices=["ds", "hft"], help="ds = event-driven data stream, hft = throttled low-latency stream")
    ap.add_argument("--mode", default="FULL", choices=["LTP", "LTPC", "QUOTE", "FULL"], help="ds stream subscription mode")
    ap.add_argument("--hft-latency", type=int, default=100, help="hft stream tick interval in ms (50-60000)")
    ap.add_argument("--dry-run", action="store_true", help="show resolved Arrow universe, then exit")
    ap.add_argument("--preflight", action="store_true", help="validate credentials + master + universe, then exit")
    ap.add_argument("--refresh-master", action="store_true", help="force download the latest Arrow instrument master")
    ap.add_argument("--no-duckdb", action="store_true", help="skip the duckdb sidecar (default for --stream hft; build it later from the CSV)")
    ap.add_argument("--duckdb", action="store_true", help="force the duckdb sidecar on, even for --stream hft")
    args = ap.parse_args()

    out_dir = Path(args.out).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    cred_dir = Path(args.cred_dir).resolve() if args.cred_dir else None

    print("=" * 72)
    print(f"  ARROW live websocket capture  |  {now_ist().strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print(f"  stream={args.stream}  interval={args.interval}  out={out_dir}")
    print("=" * 72)

    creds = load_credentials(cred_dir)
    app_id = creds.get("app_id")
    access_token = resolve_access_token(creds) if app_id else None

    client = None
    if app_id and access_token:
        from pyarrow_client import ArrowClient

        client = ArrowClient(app_id=str(app_id))
        client.set_token(str(access_token))

    master = prepare_instrument_master(ensure_instrument_master(INSTRUMENT_MASTER_FILE, refresh=args.refresh_master))
    tokens, meta_by_token, universe_info = resolve_nifty_universe(
        master,
        client,
        strikes=args.strikes,
        expiries=args.expiries,
        fut_months=args.fut_months,
        atm_override=args.atm,
    )
    print_universe(universe_info, meta_by_token)

    if args.dry_run:
        return 0

    if not app_id or not access_token:
        raise RuntimeError(
            "Arrow credentials missing. Set ARROW_APP_ID and ARROW_ACCESS_TOKEN in .env, "
            "or set ARROW_USER_ID / ARROW_PASSWORD / ARROW_API_SECRET / ARROW_TOTP_SECRET for auto-login."
        )

    if args.preflight:
        print("  Arrow credentials present and universe resolved.", flush=True)
        return 0

    day = now_ist().date()
    wait_for_market_open(day)
    outputs = output_paths(out_dir, day, args.stream)
    # Batched duckdb inserts sustain ~1.3k ticks/sec; CSV alone does ~180k. The hft
    # stream can exceed the former, so default it to CSV and build the sidecar
    # afterwards with /api/tools/snapshot-duckdb.
    use_duckdb = args.duckdb or not (args.no_duckdb or args.stream == "hft")
    if not use_duckdb:
        print("  duckdb sidecar disabled (CSV only) - build it after the session if you want it.", flush=True)
    storage = TickStorage(outputs["raw_csv"], outputs["raw_duckdb"], outputs["agg_csv"], args.interval,
                          duckdb_enabled=use_duckdb)
    stop_h, stop_m = parse_until(args.until)
    should_stop = {"value": False}
    streams_holder: dict[str, Any] = {"streams": None}

    from pyarrow_client import ArrowStreams, DataMode

    streams = ArrowStreams(appID=str(app_id), token=str(access_token), debug=False)
    streams_holder["streams"] = streams

    def teardown() -> None:
        current = streams_holder.get("streams")
        if current is None:
            return
        streams_holder["streams"] = None
        for socket in (current.data_stream, current.hft_data_stream, current.order_stream):
            try:
                socket.disconnect()
            except Exception:
                pass

    def shutdown(signum=None, frame=None):
        should_stop["value"] = True
        print("\n  stopping Arrow websocket capture...", flush=True)
        teardown()
        storage.close()
        raise SystemExit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    # emit() first: parsing on the receive path injects jitter of the same order
    # as the differences this capture exists to measure.
    def on_ds_ticks(tick: Any) -> None:
        received_at = now_ist()
        row = normalize_ds_tick(tick, meta_by_token, received_at)
        if row is not None:
            storage.persist(row)

    def on_hft_tick(payload: dict[str, Any]) -> None:
        received_at = now_ist()
        row = normalize_hft_tick(payload, meta_by_token, received_at)
        if row is not None:
            storage.persist(row)

    def on_hft_response(response: dict[str, Any]) -> None:
        code = str(response.get("error_code") or "")
        if code and code != "SUCCESS":
            print(f"  Arrow hft {response.get('request_type_str')} {code}: {response.get('error_msg')}", flush=True)
        else:
            print(
                f"  Arrow hft {response.get('request_type_str')} ok "
                f"({response.get('success_count')} accepted, {response.get('error_count')} rejected)",
                flush=True,
            )

    int_tokens = [int(t) for t in tokens]

    if args.stream == "hft":
        streams.hft_data_stream.on_ltp_tick = on_hft_tick
        streams.hft_data_stream.on_full_tick = on_hft_tick
        streams.hft_data_stream.on_response = on_hft_response
        print("  connecting Arrow hft data stream...", flush=True)
        streams.connect_hft_data_stream()
    else:
        streams.data_stream.on_ticks = on_ds_ticks
        print("  connecting Arrow data stream...", flush=True)
        streams.connect_data_stream()

    deadline = time.time() + 20
    while time.time() < deadline:
        socket = streams.hft_data_stream if args.stream == "hft" else streams.data_stream
        if socket.connected():
            break
        time.sleep(0.25)
    else:
        print("  warning: Arrow socket did not report connected within 20s; subscribing anyway.", flush=True)

    if args.stream == "hft":
        for chunk in _chunked(int_tokens, 500):
            streams.hft_data_stream.subscribe_by_segment(
                "full",
                {HFT_EXCH_NSE_FO: chunk},
                latency=max(50, min(int(args.hft_latency), 60000)),
            )
    else:
        mode = getattr(DataMode, args.mode, DataMode.FULL)
        for chunk in _chunked(int_tokens, 500):
            streams.subscribe_market_data(mode, chunk)

    print(f"  subscribed {len(int_tokens)} Arrow tokens; capturing until {args.until} IST.", flush=True)

    last_report = time.time()
    try:
        while not should_stop["value"]:
            current = now_ist()
            if (current.hour, current.minute) >= (stop_h, stop_m):
                print(f"\n  reached {args.until} IST - stopping.", flush=True)
                break
            if time.time() - last_report >= 60:
                last_report = time.time()
                print(f"  arrow ticks captured: {storage.tick_count}", flush=True)
            time.sleep(0.5)
    except KeyboardInterrupt:
        should_stop["value"] = True
    finally:
        teardown()
        storage.close()

    if outputs["agg_csv"].exists():
        print(f"\n  saved {outputs['agg_csv']}", flush=True)
    if outputs["raw_csv"].exists():
        print(f"  raw ticks: {outputs['raw_csv']}", flush=True)
    if outputs["raw_duckdb"].exists():
        print(f"  duckdb   : {outputs['raw_duckdb']}", flush=True)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\nInterrupted by user.")
    except Exception as exc:
        print(f"Startup failed: {exc}")
        sys.exit(1)

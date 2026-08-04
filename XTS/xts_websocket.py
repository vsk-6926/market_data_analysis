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
from collections import defaultdict
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import requests
import socketio
from dotenv import dotenv_values


BASE_DIR = Path(__file__).resolve().parent
ROOT_DIR = BASE_DIR.parent
ENV_FILE = ROOT_DIR / ".env"

RAW_TICKS_BASENAME = "xts_live_ticks"
ONE_SECOND_BASENAME = "xts_live_1s"

DEFAULT_ROOT = "https://xts.mithunsec.com/apibinarymarketdata"
DEFAULT_SOURCE = "WEBAPI"
DEFAULT_BROADCAST_MODE = "Full"
DEFAULT_UNDERLYING = "NIFTY"
NSEFO = 2

IST = dt.timezone(dt.timedelta(hours=5, minutes=30))

KNOWN_XTS_PATH_SUFFIXES = (
    "/apibinarymarketdata",
    "/apimarketdata",
)


def today_tag(explicit_date: str | None = None) -> str:
    return explicit_date or dt.datetime.now(IST).date().isoformat()


def daily_output_paths(run_date: str | None = None) -> dict[str, Path]:
    tag = today_tag(run_date)
    return {
        "ticks_csv": BASE_DIR / f"{RAW_TICKS_BASENAME}_{tag}.csv",
        "ticks_1s_csv": BASE_DIR / f"{ONE_SECOND_BASENAME}_{tag}.csv",
    }


def parse_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return default


def load_xts_config(env_path: Path) -> dict[str, Any]:
    file_cfg = dotenv_values(env_path) if env_path.exists() else {}

    def pick(name: str, default: Any = None) -> Any:
        value = os.getenv(name)
        if value is not None and str(value).strip() != "":
            return value
        value = file_cfg.get(name)
        if value is not None and str(value).strip() != "":
            return value
        return default

    cfg = {
        "root": str(pick("XTS_ROOT", DEFAULT_ROOT)).rstrip("/"),
        "app_key": pick("XTS_APP_KEY"),
        "secret_key": pick("XTS_SECRET_KEY"),
        "source": pick("XTS_SOURCE", DEFAULT_SOURCE),
        "broadcast_mode": pick("XTS_BROADCAST_MODE", DEFAULT_BROADCAST_MODE),
        "verify_ssl": not parse_bool(pick("XTS_DISABLE_SSL", "true"), default=True),
        "underlying": pick("XTS_UNDERLYING", DEFAULT_UNDERLYING),
    }
    return cfg


def normalize_xts_root(root: str) -> str:
    value = str(root or "").strip().rstrip("/")
    if not value:
        return DEFAULT_ROOT
    lowered = value.lower()
    for suffix in KNOWN_XTS_PATH_SUFFIXES:
        if lowered.endswith(suffix):
            return value[: -len(suffix)]
    return value


def require_config(cfg: dict[str, Any], *, preflight: bool = False) -> None:
    missing = [name for name, value in {
        "XTS_APP_KEY": cfg["app_key"],
        "XTS_SECRET_KEY": cfg["secret_key"],
    }.items() if not str(value or "").strip()]
    if missing:
        mode = "preflight" if preflight else "capture"
        raise SystemExit(f"XTS {mode} missing config: {', '.join(missing)}")


def canonical_month(expiry_date: dt.date) -> str:
    return expiry_date.strftime("%b").upper()


def build_canonical_symbol(underlying: str, expiry_date: dt.date | None, option_type: str | None = None, strike: float | None = None, series: str | None = None) -> str:
    und = str(underlying or "").upper()
    if not expiry_date:
        return und
    yy = expiry_date.strftime("%y")
    mon = canonical_month(expiry_date)
    if (series or "").upper().startswith("FUT"):
        return f"{und}{yy}{mon}FUT"
    strike_text = ""
    if strike is not None:
        strike_text = str(int(strike)) if float(strike).is_integer() else str(strike)
    suffix = str(option_type or "").upper()
    return f"{und}{yy}{mon}{strike_text}{suffix}"


def parse_expiry_token(value: Any) -> tuple[str, dt.date | None]:
    raw = str(value or "").strip()
    if not raw:
        return raw, None
    try:
        return raw, dt.datetime.fromisoformat(raw.replace("Z", "+00:00")).date()
    except ValueError:
        pass
    formats = ("%d%b%Y", "%d-%b-%Y", "%Y-%m-%d", "%d/%m/%Y", "%d %b %Y")
    for fmt in formats:
        try:
            return raw, dt.datetime.strptime(raw, fmt).date()
        except ValueError:
            pass
    digits = "".join(ch for ch in raw if ch.isdigit())
    if len(digits) == 8:
        for fmt in ("%Y%m%d", "%d%m%Y"):
            try:
                return raw, dt.datetime.strptime(digits, fmt).date()
            except ValueError:
                pass
    return raw, None


def xts_expiry_param(raw: str, parsed_date: dt.date | None) -> str:
    text = str(raw or "").strip()
    if parsed_date is not None:
        return parsed_date.strftime("%d%b%Y")
    return text


def pick_best_expiries(values: list[Any], count: int) -> list[tuple[str, dt.date | None]]:
    today = dt.datetime.now(IST).date()
    parsed = []
    seen = set()
    for value in values:
        raw, parsed_date = parse_expiry_token(value)
        if raw in seen:
            continue
        seen.add(raw)
        parsed.append((raw, parsed_date))
    future_first = [item for item in parsed if item[1] and item[1] >= today]
    future_first.sort(key=lambda item: item[1])
    if future_first:
        return future_first[: max(1, count)]
    parsed.sort(key=lambda item: item[0])
    return parsed[: max(1, count)]


def first_value(data: Any, keys: list[str]) -> Any:
    if isinstance(data, dict):
        for key in keys:
            if key in data and data[key] not in (None, ""):
                return data[key]
        for value in data.values():
            found = first_value(value, keys)
            if found not in (None, ""):
                return found
    elif isinstance(data, list):
        for item in data:
            found = first_value(item, keys)
            if found not in (None, ""):
                return found
    return None


def first_number(data: Any, keys: list[str]) -> float | None:
    value = first_value(data, keys)
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def parse_timestamp(value: Any) -> dt.datetime:
    if value is None or value == "":
        return dt.datetime.now(IST)
    if isinstance(value, (int, float)):
        numeric = float(value)
        if numeric > 1e18:
            return dt.datetime.fromtimestamp(numeric / 1e9, tz=dt.timezone.utc).astimezone(IST)
        if numeric > 1e15:
            return dt.datetime.fromtimestamp(numeric / 1e6, tz=dt.timezone.utc).astimezone(IST)
        if numeric > 1e12:
            return dt.datetime.fromtimestamp(numeric / 1e3, tz=dt.timezone.utc).astimezone(IST)
        if numeric > 1e10:
            return dt.datetime.fromtimestamp(numeric / 1e3, tz=dt.timezone.utc).astimezone(IST)
        return dt.datetime.fromtimestamp(numeric, tz=dt.timezone.utc).astimezone(IST)
    text = str(value).strip()
    if not text:
        return dt.datetime.now(IST)
    if text.isdigit():
        return parse_timestamp(int(text))
    text = text.replace("Z", "+00:00")
    try:
        parsed = dt.datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=IST)
        return parsed.astimezone(IST)
    except ValueError:
        pass
    for fmt in ("%d/%m/%Y %H:%M:%S", "%d-%m-%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return dt.datetime.strptime(text, fmt).replace(tzinfo=IST)
        except ValueError:
            pass
    return dt.datetime.now(IST)


def xts_exchange_dt(epoch, received_at):
    """Resolve an XTS exchange timestamp to IST.

    XTS deployments differ: some send Unix seconds/millis, some send seconds
    since 1980-01-01. We pick whichever interpretation lands closest to the
    locally-clocked receive time (within 3 days); otherwise fall back to the
    receive time. The raw epoch is preserved separately for later calibration.
    """
    try:
        e = float(epoch)
    except (TypeError, ValueError):
        return received_at
    if e <= 0:
        return received_at
    base_1980 = dt.datetime(1980, 1, 1, tzinfo=dt.timezone.utc)
    candidates = []
    for scale in (1.0, 1e-3, 1e-6, 1e-9):
        secs = e * scale
        try:
            candidates.append(dt.datetime.fromtimestamp(secs, tz=dt.timezone.utc).astimezone(IST))
            candidates.append((base_1980 + dt.timedelta(seconds=secs)).astimezone(IST))
        except (OverflowError, OSError, ValueError):
            continue
    if not candidates:
        return received_at
    best = min(candidates, key=lambda d: abs((d - received_at).total_seconds()))
    if abs((best - received_at).total_seconds()) <= 3 * 86400:
        return best
    return received_at


class XTSAPIError(RuntimeError):
    pass


class XTSMarketDataClient:
    def __init__(self, root: str, app_key: str, secret_key: str, source: str = DEFAULT_SOURCE, verify_ssl: bool = False, timeout: int = 20):
        self.root = normalize_xts_root(root)
        self.app_key = app_key
        self.secret_key = secret_key
        self.source = source
        self.verify_ssl = verify_ssl
        self.timeout = timeout
        self.session = requests.Session()
        self.token: str | None = None
        self.user_id: str | None = None

    def _headers(self, auth: bool = True) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if auth:
            if not self.token:
                raise XTSAPIError("XTS auth token missing. Call login() first.")
            headers["authorization"] = self.token
        return headers

    def _request(self, method: str, path: str, *, params: dict[str, Any] | None = None, payload: dict[str, Any] | None = None, auth: bool = True) -> dict[str, Any]:
        url = f"{self.root}{path}"
        resp = self.session.request(
            method.upper(),
            url,
            params=params,
            json=payload,
            headers=self._headers(auth=auth),
            timeout=self.timeout,
            verify=self.verify_ssl,
        )
        try:
            body = resp.json()
        except ValueError as exc:
            raise XTSAPIError(f"XTS returned non-JSON response from {url}: {resp.text[:400]}") from exc
        if resp.status_code >= 400 or str(body.get("type", "")).lower() == "error":
            raise XTSAPIError(f"XTS {method.upper()} {path} failed: {body}")
        return body

    def login(self) -> dict[str, Any]:
        body = self._request(
            "POST",
            "/apibinarymarketdata/auth/login",
            payload={
                "appKey": self.app_key,
                "secretKey": self.secret_key,
                "source": self.source,
            },
            auth=False,
        )
        result = body.get("result") or {}
        self.token = str(result.get("token") or "").strip()
        self.user_id = str(result.get("userID") or "").strip()
        if not self.token or not self.user_id:
            raise XTSAPIError(f"Login succeeded but token/userID missing: {body}")
        return body

    def get_config(self) -> dict[str, Any]:
        return self._request("GET", "/apibinarymarketdata/config/clientConfig")

    def get_expiry_date(self, exchange_segment: int, series: str, symbol: str) -> dict[str, Any]:
        return self._request(
            "GET",
            "/apibinarymarketdata/instruments/instrument/expiryDate",
            params={"exchangeSegment": exchange_segment, "series": series, "symbol": symbol},
        )

    def get_future_symbol(self, exchange_segment: int, series: str, symbol: str, expiry_date: str) -> dict[str, Any]:
        return self._request(
            "GET",
            "/apibinarymarketdata/instruments/instrument/futureSymbol",
            params={
                "exchangeSegment": exchange_segment,
                "series": series,
                "symbol": symbol,
                "expiryDate": expiry_date,
            },
        )

    def get_option_symbol(self, exchange_segment: int, series: str, symbol: str, expiry_date: str, option_type: str, strike_price: float | int) -> dict[str, Any]:
        return self._request(
            "GET",
            "/apibinarymarketdata/instruments/instrument/optionsymbol",
            params={
                "exchangeSegment": exchange_segment,
                "series": series,
                "symbol": symbol,
                "expiryDate": expiry_date,
                "optionType": option_type,
                "strikePrice": int(strike_price) if float(strike_price).is_integer() else strike_price,
            },
        )

    def get_quote(self, instruments: list[dict[str, Any]], xts_message_code: int = 1501, publish_format: str = "JSON") -> dict[str, Any]:
        return self._request(
            "POST",
            "/apibinarymarketdata/instruments/quotes",
            payload={
                "instruments": instruments,
                "xtsMessageCode": xts_message_code,
                "publishFormat": publish_format,
            },
        )

    def send_subscription(self, instruments: list[dict[str, Any]], xts_message_code: int = 1501) -> dict[str, Any]:
        return self._request(
            "POST",
            "/apibinarymarketdata/instruments/subscription",
            payload={"instruments": instruments, "xtsMessageCode": xts_message_code},
        )


def iter_result_items(result: Any) -> list[Any]:
    if result is None:
        return []
    if isinstance(result, list):
        return result
    if isinstance(result, dict):
        if isinstance(result.get("listQuotes"), list):
            return result["listQuotes"]
        return [result]
    if isinstance(result, str):
        text = result.strip()
        if not text:
            return []
        try:
            parsed = json.loads(text)
        except ValueError:
            return [text]
        return iter_result_items(parsed)
    return [result]


def parse_instrument_response(result: Any, *, underlying: str, expiry_date: dt.date | None, series: str, option_type: str | None = None, strike: float | None = None) -> dict[str, Any] | None:
    for item in iter_result_items(result):
        if isinstance(item, str):
            try:
                item = json.loads(item)
            except ValueError:
                item = {"displayName": item}
        if not isinstance(item, dict):
            continue
        instrument_id = first_value(item, ["exchangeInstrumentID", "ExchangeInstrumentID", "instrumentID", "InstrumentID"])
        if instrument_id in (None, ""):
            continue
        exchange_segment = first_value(item, ["exchangeSegment", "ExchangeSegment"]) or NSEFO
        display_name = first_value(item, ["displayName", "DisplayName", "symbol", "Symbol", "name", "Name"])
        try:
            instrument_id = int(instrument_id)
        except (TypeError, ValueError):
            continue
        try:
            exchange_segment = int(exchange_segment)
        except (TypeError, ValueError):
            exchange_segment = NSEFO
        native_symbol = str(display_name or instrument_id)
        canonical_symbol = build_canonical_symbol(
            underlying=underlying,
            expiry_date=expiry_date,
            option_type=option_type,
            strike=strike,
            series=series,
        )
        return {
            "exchangeSegment": exchange_segment,
            "exchangeInstrumentID": instrument_id,
            "native_symbol": native_symbol,
            "canonical_symbol": canonical_symbol,
            "expiry": expiry_date.isoformat() if expiry_date else None,
            "strike": strike,
            "option_type": option_type,
            "series": series,
            "underlying": underlying,
        }
    return None


def extract_quote_price(result: Any, exchange_instrument_id: int | None = None) -> float | None:
    for item in iter_result_items(result):
        if isinstance(item, str):
            try:
                item = json.loads(item)
            except ValueError:
                continue
        if not isinstance(item, dict):
            continue
        if exchange_instrument_id is not None:
            current_id = first_value(item, ["exchangeInstrumentID", "ExchangeInstrumentID", "instrumentID", "InstrumentID"])
            if current_id not in (None, "") and int(current_id) != int(exchange_instrument_id):
                continue
        price = first_number(item, ["LastTradedPrice", "lastTradedPrice", "LastTradePrice", "ltp", "LTP", "Close", "close"])
        if price is not None:
            return price
    return None


def resolve_future(client: XTSMarketDataClient, *, underlying: str) -> dict[str, Any]:
    expiries_body = client.get_expiry_date(NSEFO, "FUTIDX", underlying)
    expiries = pick_best_expiries(iter_result_items(expiries_body.get("result")), 1)
    if not expiries:
        raise XTSAPIError(f"No FUTIDX expiries returned for {underlying}: {expiries_body}")
    expiry_token, expiry_date = expiries[0]
    future_body = client.get_future_symbol(NSEFO, "FUTIDX", underlying, xts_expiry_param(expiry_token, expiry_date))
    future_meta = parse_instrument_response(
        future_body.get("result"),
        underlying=underlying,
        expiry_date=expiry_date,
        series="FUTIDX",
    )
    if not future_meta:
        raise XTSAPIError(f"Could not parse future symbol response: {future_body}")
    return future_meta


def fetch_reference_price(client: XTSMarketDataClient, future_meta: dict[str, Any]) -> float:
    quote_body = client.get_quote(
        [{
            "exchangeSegment": future_meta["exchangeSegment"],
            "exchangeInstrumentID": future_meta["exchangeInstrumentID"],
        }],
        xts_message_code=1501,
        publish_format="JSON",
    )
    price = extract_quote_price(quote_body.get("result"), future_meta["exchangeInstrumentID"])
    if price is None:
        raise XTSAPIError(f"Could not extract future price from quote response: {quote_body}")
    return price


def resolve_option(client: XTSMarketDataClient, *, underlying: str, expiry_token: str, expiry_date: dt.date | None, option_type: str, strike: float) -> dict[str, Any] | None:
    try:
        body = client.get_option_symbol(NSEFO, "OPTIDX", underlying, xts_expiry_param(expiry_token, expiry_date), option_type, strike)
    except XTSAPIError:
        return None
    return parse_instrument_response(
        body.get("result"),
        underlying=underlying,
        expiry_date=expiry_date,
        series="OPTIDX",
        option_type=option_type,
        strike=strike,
    )


def build_nifty_universe(client: XTSMarketDataClient, *, underlying: str, strikes: int, expiries: int, strike_step: int) -> dict[str, Any]:
    future_meta = resolve_future(client, underlying=underlying)
    reference_price = fetch_reference_price(client, future_meta)
    atm = round(reference_price / strike_step) * strike_step

    expiries_body = client.get_expiry_date(NSEFO, "OPTIDX", underlying)
    chosen_expiries = pick_best_expiries(iter_result_items(expiries_body.get("result")), expiries)
    if not chosen_expiries:
        raise XTSAPIError(f"No OPTIDX expiries returned for {underlying}: {expiries_body}")

    symbols = [future_meta]
    missing = []
    for expiry_token, expiry_date in chosen_expiries:
        for offset in range(-strikes, strikes + 1):
            strike = atm + offset * strike_step
            for option_type in ("CE", "PE"):
                option_meta = resolve_option(
                    client,
                    underlying=underlying,
                    expiry_token=expiry_token,
                    expiry_date=expiry_date,
                    option_type=option_type,
                    strike=strike,
                )
                if option_meta:
                    symbols.append(option_meta)
                else:
                    missing.append(f"{expiry_token}:{strike}:{option_type}")

    unique = {}
    for item in symbols:
        unique[(item["exchangeSegment"], item["exchangeInstrumentID"])] = item
    return {
        "symbols": list(unique.values()),
        "future": future_meta,
        "reference_price": reference_price,
        "atm": atm,
        "step": strike_step,
        "expiries": [raw for raw, _ in chosen_expiries],
        "missing": missing,
    }


def flatten_message(data: Any) -> list[dict[str, Any]]:
    if data is None:
        return []
    if isinstance(data, str):
        try:
            data = json.loads(data)
        except ValueError:
            return []
    if isinstance(data, list):
        out = []
        for item in data:
            out.extend(flatten_message(item))
        return out
    if not isinstance(data, dict):
        return []
    nested = data.get("data")
    if isinstance(nested, list):
        out = []
        for item in nested:
            merged = {}
            if isinstance(item, dict):
                merged.update(data)
                merged.pop("data", None)
                merged.update(item)
                out.append(merged)
        return out
    return [data]


class TickStorage:
    def __init__(self, ticks_csv_path: Path, ticks_1s_csv_path: Path):
        self.ticks_csv_path = ticks_csv_path
        self.ticks_1s_csv_path = ticks_1s_csv_path
        self.lock = threading.Lock()
        self.pending_1s: dict[tuple[str, str], dict[str, Any]] = {}
        self._init_raw_csv()
        self._init_1s_csv()

    def _init_raw_csv(self) -> None:
        if self.ticks_csv_path.exists():
            return
        with self.ticks_csv_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow([
                "date",
                "datetime_ist",
                "received_at_ist",
                "exchange_timestamp_epoch",
                "canonical_symbol",
                "native_symbol",
                "exchange_segment",
                "exchange_instrument_id",
                "ltp",
                "bid_price",
                "ask_price",
                "open_price",
                "high_price",
                "low_price",
                "close_price",
                "volume",
                "open_interest",
                "message_type",
                "raw_json",
            ])

    def _init_1s_csv(self) -> None:
        if self.ticks_1s_csv_path.exists():
            return
        with self.ticks_1s_csv_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow([
                "date",
                "datetime_ist",
                "first_received_at_ist",
                "last_received_at_ist",
                "canonical_symbol",
                "native_symbol",
                "exchange_segment",
                "exchange_instrument_id",
                "ltp",
                "open_price",
                "high_price",
                "low_price",
                "close_price",
                "volume",
                "bid_price",
                "ask_price",
                "open_interest",
                "message_type",
                "tick_count",
            ])

    def persist(self, tick: dict[str, Any]) -> None:
        with self.lock:
            self._write_raw(tick)
            self._update_1s(tick)
            self._flush_ready(current_second=tick["datetime"].replace(microsecond=0))

    def close(self) -> None:
        with self.lock:
            self._flush_ready(force=True)

    def _write_raw(self, tick: dict[str, Any]) -> None:
        with self.ticks_csv_path.open("a", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow([
                tick["datetime"].date().isoformat(),
                tick["datetime"].isoformat(),
                tick["received_at_ist"],
                tick.get("exchange_timestamp_epoch"),
                tick["canonical_symbol"],
                tick["native_symbol"],
                tick["exchange_segment"],
                tick["exchange_instrument_id"],
                tick["ltp"],
                tick["bid_price"],
                tick["ask_price"],
                tick["open_price"],
                tick["high_price"],
                tick["low_price"],
                tick["close_price"],
                tick["volume"],
                tick["open_interest"],
                tick["message_type"],
                tick["raw_json"],
            ])

    def _update_1s(self, tick: dict[str, Any]) -> None:
        second = tick["datetime"].replace(microsecond=0)
        key = (second.isoformat(), tick["canonical_symbol"])
        agg = self.pending_1s.get(key)
        if agg is None:
            agg = {
                "date": second.date().isoformat(),
                "datetime_ist": second.isoformat(),
                "first_received_at_ist": tick["received_at_ist"],
                "last_received_at_ist": tick["received_at_ist"],
                "canonical_symbol": tick["canonical_symbol"],
                "native_symbol": tick["native_symbol"],
                "exchange_segment": tick["exchange_segment"],
                "exchange_instrument_id": tick["exchange_instrument_id"],
                "ltp": tick["ltp"],
                "open_price": tick["ltp"],
                "high_price": tick["ltp"],
                "low_price": tick["ltp"],
                "close_price": tick["ltp"],
                "volume": tick["volume"],
                "bid_price": tick["bid_price"],
                "ask_price": tick["ask_price"],
                "open_interest": tick["open_interest"],
                "message_type": tick["message_type"],
                "tick_count": 1,
            }
            self.pending_1s[key] = agg
            return

        ltp = tick["ltp"]
        if ltp is not None:
            if agg["open_price"] is None:
                agg["open_price"] = ltp
            agg["high_price"] = ltp if agg["high_price"] is None else max(agg["high_price"], ltp)
            agg["low_price"] = ltp if agg["low_price"] is None else min(agg["low_price"], ltp)
            agg["close_price"] = ltp
            agg["ltp"] = ltp
        for field in ("volume", "bid_price", "ask_price", "open_interest", "message_type"):
            if tick[field] is not None:
                agg[field] = tick[field]
        agg["last_received_at_ist"] = tick["received_at_ist"]
        agg["tick_count"] += 1

    def _flush_ready(self, current_second: dt.datetime | None = None, force: bool = False) -> None:
        ready_keys = []
        for key, row in self.pending_1s.items():
            row_second = parse_timestamp(row["datetime_ist"])
            if force or (current_second is not None and row_second < current_second):
                ready_keys.append(key)
        if not ready_keys:
            return
        ready_keys.sort()
        with self.ticks_1s_csv_path.open("a", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            for key in ready_keys:
                row = self.pending_1s.pop(key)
                writer.writerow([
                    row["date"],
                    row["datetime_ist"],
                    row["first_received_at_ist"],
                    row["last_received_at_ist"],
                    row["canonical_symbol"],
                    row["native_symbol"],
                    row["exchange_segment"],
                    row["exchange_instrument_id"],
                    row["ltp"],
                    row["open_price"],
                    row["high_price"],
                    row["low_price"],
                    row["close_price"],
                    row["volume"],
                    row["bid_price"],
                    row["ask_price"],
                    row["open_interest"],
                    row["message_type"],
                    row["tick_count"],
                ])



# ---------------------------------------------------------------------------
# XTS binary marketdata packet parsing.
#
# The apibinarymarketdata socket delivers ALL market data through the
# `xts-binary-packet` event as little-endian binary, optionally raw-deflate
# (pako) compressed. Layout reconstructed from the official SDK's
# TouchlineEvent / MarketDepthEvent / OpenInterestEvent deserializers.
# Validated by round-tripping synthetic packets.
# ---------------------------------------------------------------------------
import struct
import zlib

XTS_MSG_VERSION_SEQ = 4  # ApplicationMessageVersion.Version_1_0_1_2983


class _BinaryReader:
    __slots__ = ("buf", "pos")

    def __init__(self, data):
        self.buf = bytes(data)
        self.pos = 0

    def _u(self, fmt, size):
        value = struct.unpack_from(fmt, self.buf, self.pos)[0]
        self.pos += size
        return value

    def read_int8(self):   return self._u('<b', 1)
    def read_uint8(self):  return self._u('<B', 1)
    def read_int16(self):  return self._u('<h', 2)
    def read_uint16(self): return self._u('<H', 2)
    def read_int32(self):  return self._u('<i', 4)
    def read_uint32(self): return self._u('<I', 4)
    def read_int64(self):  return self._u('<q', 8)
    def read_uint64(self): return self._u('<Q', 8)
    def read_double(self): return self._u('<d', 8)


def _read_depth_row(r):
    size = r.read_int32()
    price = r.read_double()
    orders = r.read_int32()
    bmm = r.read_int16()
    return {"size": size, "price": price, "orders": orders, "bmm": bmm}


def _read_common_head(r):
    message_version = r.read_uint16()
    r.read_uint16()   # applicationType
    r.read_uint64()   # tokenID
    if message_version >= XTS_MSG_VERSION_SEQ:
        r.read_uint64()   # sequenceNumber
        r.read_int32()    # skipBytes
    seg = r.read_int16()
    instr = r.read_int32()
    exch_ts = r.read_uint64()
    return message_version, seg, instr, exch_ts


def _parse_touchline(r):
    _mv, seg, instr, exch_ts = _read_common_head(r)
    bid = _read_depth_row(r)
    ask = _read_depth_row(r)
    lut = r.read_uint64()
    ltp = r.read_double()
    ltq = r.read_int32()
    tbq = r.read_uint32()
    tsq = r.read_uint32()
    ttq = r.read_uint32()
    atp = r.read_double()
    ltt = r.read_int64()
    _pct = r.read_double()
    o = r.read_double(); h = r.read_double(); l = r.read_double(); c = r.read_double()
    _tvt = r.read_double()
    return {
        "code": 1501, "exchange_segment": seg, "exchange_instrument_id": instr,
        "exchange_timestamp": exch_ts, "last_update_time": lut,
        "ltp": ltp, "ltq": ltq,
        "bid_price": bid["price"], "ask_price": ask["price"],
        "bid_qty": bid["size"], "ask_qty": ask["size"],
        "total_buy_qty": tbq, "total_sell_qty": tsq, "volume": ttq,
        "avg_traded_price": atp, "last_traded_time": ltt,
        "open_price": o, "high_price": h, "low_price": l, "close_price": c,
    }


def _parse_market_depth(r):
    _mv, seg, instr, exch_ts = _read_common_head(r)
    bid_count = r.read_int32()
    bids = [_read_depth_row(r) for _ in range(max(0, bid_count))]
    ask_count = r.read_int32()
    asks = [_read_depth_row(r) for _ in range(max(0, ask_count))]
    _read_depth_row(r)  # touchline bid summary
    _read_depth_row(r)  # touchline ask summary
    lut = r.read_uint64()
    ltp = r.read_double()
    r.read_int32()          # ltq
    r.read_uint32()         # totalBuyQuantity
    r.read_uint32()         # totalSellQuantity
    ttq = r.read_uint32()   # totalTradedQuantity
    r.read_double()         # averageTradedPrice
    r.read_int64()          # lastTradedTime
    r.read_double()         # percentChange
    o = r.read_double(); h = r.read_double(); l = r.read_double(); c = r.read_double()
    return {
        "code": 1502, "exchange_segment": seg, "exchange_instrument_id": instr,
        "exchange_timestamp": exch_ts, "last_update_time": lut, "ltp": ltp, "volume": ttq,
        "open_price": o, "high_price": h, "low_price": l, "close_price": c,
        "bid_price": bids[0]["price"] if bids else None,
        "ask_price": asks[0]["price"] if asks else None,
        "depth_bids": bids, "depth_asks": asks,
    }


def _parse_open_interest(r):
    message_version = r.read_uint16()
    r.read_uint16()   # applicationType
    r.read_uint64()   # tokenID
    if message_version >= XTS_MSG_VERSION_SEQ:
        r.read_uint64()
        r.read_int32()
    seg = r.read_int16()
    instr = r.read_int32()
    exch_ts = r.read_uint64()
    r.read_int16()          # MarketType
    oi = r.read_int32()
    return {
        "code": 1510, "exchange_segment": seg, "exchange_instrument_id": instr,
        "exchange_timestamp": exch_ts, "open_interest": oi,
    }


_XTS_PARSERS = {1501: _parse_touchline, 1502: _parse_market_depth, 1510: _parse_open_interest}


def parse_xts_binary(data):
    """Split a raw xts-binary-packet payload into a list of decoded dicts."""
    if isinstance(data, str):
        # Belt-and-suspenders: some socketio builds surface binary as latin-1 text.
        data = data.encode("latin-1", errors="ignore")
    buf = bytes(data)
    n = len(buf)
    # XTS prepends a single Engine.IO binary message-type byte (0x04) ahead of
    # the packet stream; the SDK's socket layer normally strips it. Skip it here
    # so the gzip flag + 16-byte header + body land where the parser expects.
    offset = 1 if (n and buf[0] == 0x04) else 0
    out = []
    while offset < n:
        gzip_flag = buf[offset]
        offset += 1
        if offset + 16 > n:
            break
        code, _seg, _instr, _book, _mtype, unc, comp = struct.unpack_from('<HhihhHH', buf, offset)
        offset += 16
        try:
            if gzip_flag == 1:
                body = zlib.decompressobj(-15).decompress(buf[offset:offset + comp])
                offset += comp
                reader = _BinaryReader(body)
                reader.read_uint16()  # messageCode repeated at head of decompressed body
            else:
                body = buf[offset:offset + unc]
                offset += unc
                reader = _BinaryReader(body)
            parser = _XTS_PARSERS.get(code)
            if parser is not None:
                out.append(parser(reader))
        except (struct.error, zlib.error, IndexError):
            break
    return out


class XTSBinaryCollector:
    """Consumes the XTS `xts-binary-packet` stream and persists ticks with both
    the exchange timestamp and a locally-clocked receive time."""

    def __init__(self, client, universe, storage, *, broadcast_mode=DEFAULT_BROADCAST_MODE,
                 subscribe_oi=True, subscribe_depth=False, max_subscriptions=50, sub_chunk=50):
        self.client = client
        self.universe = universe
        self.storage = storage
        self.broadcast_mode = broadcast_mode
        self.subscribe_oi = subscribe_oi
        self.subscribe_depth = subscribe_depth
        self.max_subscriptions = max(1, int(max_subscriptions))
        self.sub_chunk = max(1, int(sub_chunk))
        self.debug_packets = 0
        self._subscribed = False
        self.instrument_map = {
            int(item["exchangeInstrumentID"]): item for item in universe["symbols"]
        }
        self.latest_state: dict[int, dict[str, Any]] = defaultdict(dict)
        self.running = threading.Event()
        self.running.set()
        self.packet_count = 0
        self.tick_count = 0
        self.sio = socketio.Client(
            reconnection=True, logger=False, engineio_logger=False,
            ssl_verify=self.client.verify_ssl,
        )

    # -- socket lifecycle ---------------------------------------------------
    def run(self) -> None:
        self._bind_handlers()
        query = urlencode({
            "token": self.client.token,
            "userID": self.client.user_id,
            "publishFormat": "JSON",          # affects REST /quotes only; socket stays binary
            "broadcastMode": self.broadcast_mode,
        })
        url = f"{self.client.root}/?{query}"
        # No wait_timeout kwarg: python-socketio 4.6.x (required to speak the XTS
        # server's Engine.IO v3 protocol) does not accept it.
        self.sio.connect(
            url,
            transports=["websocket"],
            socketio_path="apibinarymarketdata/socket.io",
        )
        self.sio.wait()

    def stop(self) -> None:
        self.running.clear()
        try:
            if self.sio.connected:
                self.sio.disconnect()
        except Exception:
            pass
        self.storage.close()

    def _bind_handlers(self) -> None:
        self.sio.on("connect", self._on_connect)
        self.sio.on("disconnect", self._on_disconnect)
        self.sio.on("message", self._on_message)
        self.sio.on("xts-binary-packet", self._on_binary_packet)

    def _subscribe_chunked(self, instruments: list[dict[str, Any]], code: int, label: str, chunk_size: int) -> None:
        """XTS caps each subscription request at 100 instruments (depth is tighter),
        so send in batches. A rejected batch is logged but never kills the socket."""
        total = len(instruments)
        ok = 0
        failed = 0
        for start in range(0, total, chunk_size):
            batch = instruments[start:start + chunk_size]
            try:
                resp = self.client.send_subscription(batch, code)
                ok += len(batch)
                detail = resp.get("description") or resp.get("type")
                print(f"XTS subscribe {code} ({label}) | batch {start // chunk_size + 1} "
                      f"| {len(batch)} instruments | {detail}", flush=True)
            except XTSAPIError as exc:
                failed += len(batch)
                print(f"[XTS subscribe {code} ({label}) batch {start // chunk_size + 1} FAILED] {exc}", flush=True)
        print(f"XTS subscribe {code} ({label}) done | subscribed={ok} failed={failed} of {total}", flush=True)

    def _prioritized_instruments(self) -> list[dict[str, Any]]:
        """Order the universe as future first, then strikes nearest ATM, and cap
        at the account's subscription limit. XTS accepts subscriptions in list
        order, so without this the 50-instrument budget gets spent on the far
        deep-ITM/OTM strikes at the top of the list instead of the liquid ATM ones."""
        atm = self.universe.get("atm") or 0
        try:
            atm = float(atm)
        except (TypeError, ValueError):
            atm = 0.0

        def sort_key(item):
            strike = item.get("strike")
            is_future = (item.get("series") == "FUTIDX") or strike in (None, "")
            try:
                dist = abs(float(strike) - atm)
            except (TypeError, ValueError):
                dist = 0.0
            return (0 if is_future else 1, dist)

        ordered = sorted(self.universe["symbols"], key=sort_key)
        return ordered[:self.max_subscriptions]

    def _on_connect(self) -> None:
        print("XTS market-data socket connected.")
        if self._subscribed:
            # XTS keeps the subscription bound to the session across brief
            # reconnects, and re-subscribing would just hit the 50/50 cap.
            print("XTS reconnected; reusing existing server-side subscription.", flush=True)
            return
        chosen = self._prioritized_instruments()
        total_available = len(self.universe["symbols"])
        instruments = [
            {"exchangeSegment": item["exchangeSegment"],
             "exchangeInstrumentID": item["exchangeInstrumentID"]}
            for item in chosen
        ]
        if len(instruments) < total_available:
            strikes = [i.get("strike") for i in chosen if i.get("strike") not in (None, "")]
            span = f"{min(strikes):g}–{max(strikes):g}" if strikes else "n/a"
            print(f"XTS account subscription cap = {self.max_subscriptions}; subscribing the future "
                  f"+ {len(instruments) - 1} nearest-ATM strikes (strike span {span}). "
                  f"{total_available - len(instruments)} far strikes omitted. "
                  f"Raise --max-subscriptions if your account tier allows more.", flush=True)
        self._subscribed = True
        # 1501 Touchline carries LTP, best bid/ask, OHLC, volume + exchange timestamp.
        self._subscribe_chunked(instruments, 1501, "touchline", self.sub_chunk)
        if self.subscribe_oi:
            self._subscribe_chunked(instruments, 1510, "open interest", self.sub_chunk)
        if self.subscribe_depth:
            # 20-level depth has a tighter per-connection ceiling; keep batches small.
            self._subscribe_chunked(instruments, 1502, "market depth", min(self.sub_chunk, 50))

    def _on_disconnect(self) -> None:
        print("XTS market-data socket disconnected.")

    def _on_message(self, data: Any) -> None:
        if isinstance(data, dict) and str(data.get("type", "")).lower() == "error":
            print(f"[XTS message error] {data}")

    # -- data path: clock receive time FIRST, then parse --------------------
    def _on_binary_packet(self, data: Any) -> None:
        received_at = dt.datetime.now(IST)
        if self.debug_packets and self.packet_count < self.debug_packets:
            self._dump_packet(data, self.packet_count)
        try:
            packets = parse_xts_binary(data)
        except Exception as exc:  # never let a bad packet kill the stream
            print(f"[XTS binary parse error] {exc}")
            return
        self.packet_count += 1
        received_iso = received_at.isoformat()
        for pkt in packets:
            tick = self._normalize(pkt, received_at, received_iso)
            if tick is not None:
                self.tick_count += 1
                self.storage.persist(tick)
        if self.packet_count <= 3 or self.packet_count % 500 == 0:
            print(f"[XTS] packets={self.packet_count} ticks={self.tick_count}", flush=True)

    def _dump_packet(self, data: Any, idx: int) -> None:
        try:
            if isinstance(data, str):
                raw = data.encode("latin-1", errors="ignore")
            else:
                raw = bytes(data)
        except Exception as exc:
            print(f"[XTS DEBUG #{idx}] type={type(data).__name__} not bytes-convertible: {exc} | repr={repr(data)[:120]}", flush=True)
            return
        n = len(raw)
        head = raw[:220]
        print(f"\n[XTS DEBUG #{idx}] type={type(data).__name__} len={n}", flush=True)
        print(f"  hex[0:{len(head)}]: {head.hex()}", flush=True)
        if n >= 1:
            gzip_flag = raw[0]
            print(f"  byte0 (gzip flag?) = {gzip_flag}", flush=True)
        if n >= 17:
            for order, tag in (("<", "little"), (">", "big")):
                try:
                    code, seg, instr, book, mtype, unc, comp = struct.unpack_from(order + "HhihhHH", raw, 1)
                    print(f"  hdr@1 {tag}-endian: code={code} seg={seg} instr={instr} book={book} "
                          f"mtype={mtype} uncSize={unc} compSize={comp}", flush=True)
                except Exception as exc:
                    print(f"  hdr@1 {tag}-endian: unpack failed: {exc}", flush=True)
        parsed = parse_xts_binary(raw)
        codes = [p.get("code") for p in parsed]
        print(f"  parse_xts_binary -> {len(parsed)} packets, codes={codes}", flush=True)

    def _normalize(self, pkt, received_at, received_iso):
        instrument_id = pkt.get("exchange_instrument_id")
        if instrument_id in (None, ""):
            return None
        try:
            instrument_id = int(instrument_id)
        except (TypeError, ValueError):
            return None
        meta = self.instrument_map.get(instrument_id)
        if not meta:
            return None

        # Merge partial updates (1501/1502/1510) into a per-instrument state.
        state = self.latest_state[instrument_id]
        for key in ("ltp", "bid_price", "ask_price", "open_price", "high_price",
                    "low_price", "close_price", "volume", "open_interest",
                    "exchange_timestamp"):
            if pkt.get(key) is not None:
                state[key] = pkt[key]

        exch_ts = state.get("exchange_timestamp")
        event_dt = xts_exchange_dt(exch_ts, received_at) if exch_ts else received_at

        return {
            "datetime": event_dt,                      # exchange (listed) time in IST
            "received_at_ist": received_iso,           # locally clocked receive time
            "exchange_timestamp_epoch": exch_ts,       # raw exchange epoch (seconds)
            "canonical_symbol": meta["canonical_symbol"],
            "native_symbol": meta["native_symbol"],
            "exchange_segment": meta["exchangeSegment"],
            "exchange_instrument_id": instrument_id,
            "ltp": state.get("ltp"),
            "bid_price": state.get("bid_price"),
            "ask_price": state.get("ask_price"),
            "open_price": state.get("open_price"),
            "high_price": state.get("high_price"),
            "low_price": state.get("low_price"),
            "close_price": state.get("close_price"),
            "volume": state.get("volume"),
            "open_interest": state.get("open_interest"),
            "message_type": f"xts-{pkt.get('code')}",
            "raw_json": json.dumps(pkt, default=str),
        }


def preflight(cfg: dict[str, Any], *, strikes: int, expiries: int, strike_step: int) -> dict[str, Any]:
    require_config(cfg, preflight=True)
    client = XTSMarketDataClient(
        root=cfg["root"],
        app_key=str(cfg["app_key"]),
        secret_key=str(cfg["secret_key"]),
        source=str(cfg["source"]),
        verify_ssl=bool(cfg["verify_ssl"]),
    )
    login_body = client.login()
    config_body = client.get_config()
    universe = build_nifty_universe(
        client,
        underlying=str(cfg["underlying"]),
        strikes=strikes,
        expiries=expiries,
        strike_step=strike_step,
    )
    return {
        "login": login_body,
        "config": config_body,
        "universe": universe,
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="XTS market-data websocket capture for NIFTY future + ATM option chain.")
    ap.add_argument("--preflight", action="store_true", help="validate XTS login and universe resolution, then exit")
    ap.add_argument("--date", default=None, help="output date tag override (YYYY-MM-DD)")
    ap.add_argument("--strikes", type=int, default=15, help="ATM-centered option strikes either side")
    ap.add_argument("--expiries", type=int, default=1, help="number of nearest expiries to subscribe")
    ap.add_argument("--strike-step", type=int, default=50, help="strike step to use for ATM-centered option selection")
    ap.add_argument("--root", default=None, help="override XTS base URL")
    ap.add_argument("--source", default=None, help="override XTS source, usually WEBAPI")
    ap.add_argument("--no-oi", action="store_true", help="do not subscribe to the 1510 open-interest stream")
    ap.add_argument("--with-depth", action="store_true", help="also subscribe to 1502 full market depth")
    ap.add_argument("--max-subscriptions", type=int, default=50, help="max instruments this XTS account can subscribe (session cap; this account is 50)")
    ap.add_argument("--sub-chunk", type=int, default=50, help="instruments per subscription request (<= account cap)")
    ap.add_argument("--debug-packets", type=int, default=0, help="hex-dump the first N raw xts-binary packets for layout diagnosis, then keep running")
    args = ap.parse_args()

    cfg = load_xts_config(ENV_FILE)
    if args.root:
        cfg["root"] = str(args.root).rstrip("/")
    if args.source:
        cfg["source"] = str(args.source)

    if args.preflight:
        info = preflight(cfg, strikes=args.strikes, expiries=args.expiries, strike_step=args.strike_step)
        universe = info["universe"]
        print("XTS preflight passed.")
        print(f"  root: {cfg['root']}")
        print(f"  userID: {info['login'].get('result', {}).get('userID')}")
        print(f"  future: {universe['future']['canonical_symbol']}")
        print(f"  reference price: {universe['reference_price']}")
        print(f"  atm: {universe['atm']}")
        print(f"  step: {universe['step']}")
        print(f"  expiries: {universe['expiries']}")
        print(f"  subscribed instruments: {len(universe['symbols'])}")
        if universe["missing"]:
            print(f"  unresolved option contracts: {len(universe['missing'])} (showing first 10)")
            for item in universe["missing"][:10]:
                print(f"    - {item}")
        return

    require_config(cfg)
    info = preflight(cfg, strikes=args.strikes, expiries=args.expiries, strike_step=args.strike_step)
    universe = info["universe"]

    outputs = daily_output_paths(args.date)
    storage = TickStorage(outputs["ticks_csv"], outputs["ticks_1s_csv"])
    client = XTSMarketDataClient(
        root=cfg["root"],
        app_key=str(cfg["app_key"]),
        secret_key=str(cfg["secret_key"]),
        source=str(cfg["source"]),
        verify_ssl=bool(cfg["verify_ssl"]),
    )
    client.token = str(info["login"]["result"]["token"])
    client.user_id = str(info["login"]["result"]["userID"])

    print("=" * 72)
    print(f"  XTS live capture  |  {dt.datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print(f"  out={outputs['ticks_csv']}  1s={outputs['ticks_1s_csv']}")
    print("=" * 72)
    print(f"  future={universe['future']['canonical_symbol']}  reference={universe['reference_price']:.2f}  ATM={universe['atm']:.0f}  step={universe['step']}")
    print(f"  expiries={universe['expiries']}  instruments={len(universe['symbols'])}")
    if universe["missing"]:
        print(f"  warning: unresolved option contracts={len(universe['missing'])} (continuing with resolved instruments)")

    collector = XTSBinaryCollector(
        client=client,
        universe=universe,
        storage=storage,
        broadcast_mode=str(cfg["broadcast_mode"]),
        subscribe_oi=not args.no_oi,
        subscribe_depth=args.with_depth,
        max_subscriptions=args.max_subscriptions,
        sub_chunk=args.sub_chunk,
    )
    collector.debug_packets = args.debug_packets

    def shutdown(signum=None, frame=None):
        print("\nStopping XTS capture...")
        collector.stop()
        raise SystemExit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    try:
        collector.run()
    finally:
        storage.close()


if __name__ == "__main__":
    main()
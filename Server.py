"""Minimal report-only server for standalone broker capture files."""

from __future__ import annotations

import csv
import io
from itertools import combinations
import json
import math
import os
import re
import sys
import subprocess
import time
import urllib.request
from pathlib import Path

import pandas as pd
import uvicorn
from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from pydantic import BaseModel

try:
    import duckdb
except ImportError:
    duckdb = None

HERE = Path(__file__).parent
sys.path.insert(0, str(HERE))


def _json_safe(value):
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return value
    if isinstance(value, dict):
        return {k: _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    return value


def _debug_event(hypothesis_id: str, location: str, msg: str, data: dict | None = None, run_id: str = "pre", session_id: str = "dhan-delay-ui"):
    env_path = HERE / ".dbg" / f"{session_id}.env"
    server_url = "http://127.0.0.1:7777/event"
    active_session_id = session_id
    try:
        if env_path.exists():
            for line in env_path.read_text(encoding="utf-8").splitlines():
                if line.startswith("DEBUG_SERVER_URL="):
                    server_url = line.split("=", 1)[1].strip() or server_url
                elif line.startswith("DEBUG_SESSION_ID="):
                    active_session_id = line.split("=", 1)[1].strip() or active_session_id
        payload = {
            "sessionId": active_session_id,
            "runId": run_id,
            "hypothesisId": hypothesis_id,
            "location": location,
            "msg": f"[DEBUG] {msg}",
            "data": data or {},
            "ts": int(time.time() * 1000),
        }
        req = urllib.request.Request(
            server_url,
            data=json.dumps(payload).encode(),
            headers={"Content-Type": "application/json"},
        )
        urllib.request.urlopen(req, timeout=0.5).read()
    except Exception:
        pass


class SafeJSONResponse(JSONResponse):
    def render(self, content):
        return super().render(_json_safe(content))


app = FastAPI(title="Feed Bench Reports", default_response_class=SafeJSONResponse)


@app.get("/")
def index():
    return FileResponse(
        HERE / "Index.html",
        headers={"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
                 "Pragma": "no-cache", "Expires": "0"},
    )


@app.get("/api/config")
def config():
    return {"defaults": _default_paths()}


@app.post("/api/tools/snapshot-duckdb")
def create_duckdb_snapshots(r: SnapshotReq):
    items = {
        "fyers_csv": _build_duckdb_snapshot(r.fyers_csv),
        "dhan_csv": _build_duckdb_snapshot(r.dhan_csv),
        "nubra_csv": _build_duckdb_snapshot(r.nubra_csv),
        "xts_csv": _build_duckdb_snapshot(r.xts_csv),
    }
    created = {key: value["snapshot"] for key, value in items.items() if value and value.get("status") == "created" and value.get("snapshot")}
    return {"ok": True, "snapshots": items, "created": created}


class AnalyzeDirReq(BaseModel):
    date: str | None = None
    ref: str = "fyers"
    from_ts: str | None = None
    to_ts: str | None = None


class AnalyzeFilesReq(BaseModel):
    fyers_csv: str | None = None
    dhan_csv: str | None = None
    nubra_csv: str | None = None
    # nubra_parquet: str
    symbol: str = ""
    ref: str = "dhan"
    from_ts: str | None = None
    to_ts: str | None = None


class BrowseRowsReq(BaseModel):
    fyers_csv: str | None = None
    dhan_csv: str | None = None
    nubra_csv: str | None = None
    # nubra_parquet: str | None = None
    gdfl_parquet: str | None = None
    xts_csv: str | None = None
    symbol: str
    from_ts: str | None = None
    to_ts: str | None = None
    limit: int = 500


class SecondCompareReq(BaseModel):
    fyers_csv: str | None = None
    nubra_csv: str | None = None
    # nubra_parquet: str | None = None
    symbol: str
    from_ts: str | None = None
    to_ts: str | None = None
    bins: int = 21


class AlphaBotCompareReq(BaseModel):
    alpha_csv: str | None = None
    fyers_csv: str | None = None
    dhan_csv: str | None = None
    nubra_csv: str | None = None
    # nubra_parquet: str | None = None
    gdfl_parquet: str | None = None
    from_ts: str | None = None
    to_ts: str | None = None
    broker_filter: str | None = None
    tolerance_seconds: int = 120
    limit: int = 200


class DecisionCompareReq(BaseModel):
    reference: str = "fyers"
    fyers_csv: str | None = None
    dhan_csv: str | None = None
    nubra_csv: str | None = None
    # nubra_parquet: str | None = None
    gdfl_parquet: str | None = None
    xts_csv: str | None = None
    alpha_csv: str | None = None
    symbol: str = ""
    from_ts: str | None = None
    to_ts: str | None = None
    alphabot_broker: str | None = None
    tolerance_seconds: int = 120
    limit: int = 200
    

class ReceiveCompareReq(BaseModel):
    fyers_csv: str | None = None
    dhan_csv: str | None = None
    nubra_csv: str | None = None
    # nubra_parquet: str | None = None
    xts_csv: str | None = None
    symbol: str
    from_ts: str | None = None
    to_ts: str | None = None


class ReceiveRowsReq(BaseModel):
    fyers_csv: str | None = None
    dhan_csv: str | None = None
    nubra_csv: str | None = None    
    # nubra_parquet: str | None = None
    xts_csv: str | None = None
    symbol: str
    from_ts: str | None = None
    to_ts: str | None = None
    limit: int = 300


class PairedBrowseReq(BaseModel):
    fyers_csv: str | None = None
    dhan_csv: str | None = None
    nubra_csv: str | None = None
    left_broker: str = "fyers"
    right_broker: str = "nubra"
    symbol: str
    from_ts: str | None = None
    to_ts: str | None = None
    limit: int = 300


class SnapshotReq(BaseModel):
    fyers_csv: str | None = None
    dhan_csv: str | None = None
    nubra_csv: str | None = None
    xts_csv: str | None = None


class ExportColumn(BaseModel):
    key: str
    label: str | None = None


class ExportRowsReq(BaseModel):
    section: str
    symbol: str | None = None
    columns: list[ExportColumn] = []
    rows: list[dict] = []


def _latest(paths: list[Path]):
    existing = [p for p in paths if p.exists()]
    if not existing:
        return None
    return str(max(existing, key=lambda p: p.stat().st_mtime).relative_to(HERE))


def _latest_glob(relative_dir: str, patterns: list[str]):
    base = HERE / relative_dir
    if not base.exists():
        return None
    found = []
    for pat in patterns:
        found.extend(base.glob(pat))
    return _latest(found)


def _default_paths():
    fyers_csv = _latest_glob("FYERS", ["fyers_live_1s_*.csv", "fyers_live_1s.csv", "fyers_live_ticks_*.csv", "fyers_live_ticks.csv"])
    nubra_1s_csv = (
        _latest_glob("Nubra", ["nubra_live_1s_*.csv", "20[0-9][0-9]-[0-9][0-9]-[0-9][0-9].csv"])
        or _latest_glob(".", ["nubra_live_1s_*.csv", "20[0-9][0-9]-[0-9][0-9]-[0-9][0-9].csv"])
    )
    nubra_ticks_csv = (_latest_glob("Nubra", ["nubra_live_ticks_*.csv", "nubra_live_ticks.csv"])
                       or _latest_glob(".", ["nubra_live_ticks_*.csv", "nubra_live_ticks.csv"]))
    nubra_csv = nubra_1s_csv or nubra_ticks_csv
    # nubra_files = list((HERE / "Nubra").glob("*.parquet")) if (HERE / "Nubra").exists() else []
    gdfl_files = list((HERE / "GDFL").glob("*.parquet")) if (HERE / "GDFL").exists() else []
    xts_csv = _latest_glob("XTS", ["xts_live_1s_*.csv", "xts_live_ticks_*.csv"])
    dhan_files = [
        p for p in (HERE / "Dhan").glob("*.csv")
        if "api-scrip-master" not in p.name.lower()
    ] if (HERE / "Dhan").exists() else []
    dhan_files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    preferred_dhan = [
        p for p in dhan_files
        if p.name.startswith("dhan_live_1s_") or p.name == "dhan_live_1s.csv"
    ]
    dhan_tick_files = [p for p in dhan_files if "completeness" not in p.name.lower()]
    alpha_files = [HERE / "alphabot_data.csv", *sorted(HERE.glob("*alphabot*.csv"))]
    return {
        "fyers_csv": fyers_csv,
        "nubra_csv": nubra_csv,
        "nubra_parquet": nubra_csv,   # backward-compat: same path, so older cached UI still fills the field
        # "nubra_parquet": _latest(nubra_files),
        "gdfl_parquet": _latest(gdfl_files),
        "xts_csv": xts_csv,
        "dhan_csv": _latest(preferred_dhan or dhan_tick_files),
        "alphabot_csv": _latest(alpha_files),
        "symbol": "24400PE",
    }


def _captures_dir(day: str | None):
    root = HERE / "captures"
    if day:
        return root / day
    return max(root.glob("*"), key=lambda p: p.stat().st_mtime) if root.exists() and list(root.glob("*")) else root


def _resolve_path(path_str: str | None):
    if not path_str:
        return None
    p = Path(path_str)
    if not p.is_absolute():
        p = HERE / p
    return p


def _snapshot_duckdb_path(path: Path):
    return path.with_suffix(".duckdb")


def _snapshot_table_name(path: Path):
    name = path.name.lower()
    return "live_1s" if "live_1s" in name else "live_ticks"


def _build_duckdb_snapshot(path_str: str | None):
    path = _resolve_path(path_str)
    if not path or not path.exists():
        return None
    if path.suffix.lower() != ".csv":
        return {"source": str(path.relative_to(HERE)) if path.is_relative_to(HERE) else str(path), "snapshot": None, "status": "skipped"}
    if duckdb is None:
        return {"source": str(path.relative_to(HERE)) if path.is_relative_to(HERE) else str(path), "snapshot": None, "status": "duckdb_missing"}
    target = _snapshot_duckdb_path(path)
    table = _snapshot_table_name(path)
    conn = duckdb.connect(str(target))
    try:
        conn.execute(
            f"CREATE OR REPLACE TABLE {table} AS "
            f"SELECT * FROM read_csv_auto({_sql_text(str(path))}, header=true, all_varchar=true, ignore_errors=true)"
        )
    finally:
        conn.close()
    try:
        src_text = str(path.relative_to(HERE))
    except ValueError:
        src_text = str(path)
    try:
        snap_text = str(target.relative_to(HERE))
    except ValueError:
        snap_text = str(target)
    return {"source": src_text, "snapshot": snap_text, "status": "created"}


def _parse_bound(v: str | None):
    if not v:
        return None
    ts = pd.to_datetime(v)
    if getattr(ts, "tzinfo", None) is None:
        ts = ts.tz_localize("Asia/Kolkata")
    return ts.tz_convert("Asia/Kolkata")


def _apply_range(df: pd.DataFrame, start: str | None, end: str | None):
    if df.empty or "timestamp_ist" not in df.columns:
        return df
    out = df.copy()
    lo = _parse_bound(start)
    hi = _parse_bound(end)
    if lo is not None:
        out = out[out["timestamp_ist"] >= lo]
    if hi is not None:
        out = out[out["timestamp_ist"] <= hi]
    return out


def _csv_columns(path: Path):
    try:
        with path.open("r", encoding="utf-8", newline="") as handle:
            return next(csv.reader(handle), [])
    except UnicodeDecodeError:
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            return next(csv.reader(handle), [])


def _sql_ident(name: str):
    return '"' + str(name).replace('"', '""') + '"'


def _sql_text(value: str):
    return "'" + str(value).replace("'", "''") + "'"


def _symbol_filter_sql(query: str, columns: list[str], available: set[str]):
    q = str(query or "").strip().upper()
    if not q:
        return None
    checks = []
    q_sql = _sql_text(q)
    for col in columns:
        if col not in available:
            continue
        expr = f"UPPER(COALESCE(CAST({_sql_ident(col)} AS VARCHAR), ''))"
        checks.append(f"({expr} = {q_sql} OR POSITION({q_sql} IN {expr}) > 0)")
    if not checks:
        return None
    return "(" + " OR ".join(checks) + ")"


def _duckdb_sidecar_path(path: Path):
    m = re.match(r"^((?:fyers|nubra|dhan)_live)_(?:1s|ticks)(.*)\.csv$", path.name, flags=re.IGNORECASE)
    if not m:
        return None
    return path.with_name(f"{m.group(1)}_ticks{m.group(2)}.duckdb")


def _duckdb_table_for_path(path: Path):
    return "live_1s" if "live_1s" in path.name.lower() else "live_ticks"


def _duckdb_columns(path: Path):
    if duckdb is None or path.suffix.lower() != ".duckdb":
        return set()
    conn = None
    try:
        conn = duckdb.connect(str(path))
        table = _duckdb_table_for_path(path)
        rows = conn.execute(f"DESCRIBE {table}").fetchall()
        return {str(row[0]) for row in rows}
    except Exception:
        return set()
    finally:
        if conn is not None:
            conn.close()


def _duckdb_project(path: Path, select_exprs: list[tuple[str, str]], where_sql: str | None = None):
    if duckdb is None:
        return None
    conn = None
    try:
        if path.suffix.lower() == ".duckdb":
            conn = duckdb.connect(str(path))
            source_sql = _duckdb_table_for_path(path)
        else:
            sidecar = _duckdb_sidecar_path(path)
            if sidecar and sidecar.exists():
                conn = duckdb.connect(str(sidecar))
                source_sql = _duckdb_table_for_path(path)
            else:
                conn = duckdb.connect()
                source_sql = (
                    f"read_csv_auto({_sql_text(str(path))}, "
                    "header=true, all_varchar=true, ignore_errors=true)"
                )
        sql = "SELECT " + ", ".join(f"{expr} AS {_sql_ident(alias)}" for alias, expr in select_exprs)
        sql += f" FROM {source_sql}"
        if where_sql:
            sql += f" WHERE {where_sql}"
        return conn.execute(sql).df()
    except Exception:
        return None
    finally:
        if conn is not None:
            conn.close()


def _fmt_ts(series: pd.Series):
    s = series
    if not pd.api.types.is_datetime64_any_dtype(s):
        # After concatenating multiple brokers the column can degrade to object
        # dtype (mixed tz/resolution); normalize via UTC before formatting.
        s = pd.to_datetime(s, errors="coerce", utc=True)
        try:
            s = s.dt.tz_convert("Asia/Kolkata")
        except (TypeError, AttributeError):
            pass
    return s.dt.strftime("%Y-%m-%d %H:%M:%S.%f %Z").str.replace(r"\.0+ ", " ", regex=True)


def _to_ist_series(series: pd.Series):
    # format="ISO8601" parses ISO strings of ANY sub-second precision. Without it,
    # pandas 2.x infers one format from the first row and then coerces the rest to
    # NaT / object dtype (Fyers isoformat() drops trailing-zero microseconds, so a
    # single column mixes ".123456", ".1" and no-fraction timestamps).
    try:
        ts = pd.to_datetime(series, errors="coerce", format="ISO8601")
    except (ValueError, TypeError):
        ts = pd.to_datetime(series, errors="coerce", utc=True)
    if getattr(ts.dt, "tz", None) is None:
        return ts.dt.tz_localize("Asia/Kolkata")
    return ts.dt.tz_convert("Asia/Kolkata")


def _delay_ms(received: pd.Series, listed: pd.Series):
    if received is None or listed is None:
        return pd.Series([None] * len(listed), index=listed.index if listed is not None else received.index)
    delta = (received - listed).dt.total_seconds() * 1000.0
    return pd.to_numeric(delta, errors="coerce")


def _repair_dhan_legacy_times(timestamp_s: pd.Series, listed_s: pd.Series, received_s: pd.Series):
    timestamp_ist = _to_ist_series(timestamp_s)
    listed_ist = _to_ist_series(listed_s)
    received_ist = _to_ist_series(received_s)
    skew = (listed_ist - received_ist).dt.total_seconds()
    needs_fix = listed_ist.notna() & received_ist.notna() & (skew > (2 * 3600))
    if needs_fix.any():
        shift = pd.Timedelta(hours=5, minutes=30)
        timestamp_ist = timestamp_ist.where(~needs_fix, timestamp_ist - shift)
        listed_ist = listed_ist.where(~needs_fix, listed_ist - shift)
    return timestamp_ist, listed_ist, received_ist


def _match_symbol(series: pd.Series, query: str):
    q = (query or "").strip().upper()
    if not q:
        return pd.Series([True] * len(series), index=series.index)
    s = series.fillna("").astype(str).str.upper()
    return s.eq(q) | s.str.contains(q, regex=False)


_OPT_MONTHS = {"JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
               "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12}
_FYERS_MONTH_CHAR = {"1": 1, "2": 2, "3": 3, "4": 4, "5": 5, "6": 6, "7": 7,
                     "8": 8, "9": 9, "O": 10, "N": 11, "D": 12}


def _canon_option(underlying, year, month, day, strike, right):
    """Common cross-broker option key: NIFTY + YYYYMMDD + strike + CE/PE."""
    try:
        strike = int(round(float(strike)))
    except (TypeError, ValueError):
        return None
    if strike >= 100000:          # unscaled paise (older Nubra files) -> rupees
        strike //= 100
    if day:
        return f"{underlying}{year:04d}{month:02d}{day:02d}{strike}{right}"
    return f"{underlying}{year:04d}{month:02d}{strike}{right}"   # monthly (no day)


def canonicalize_fyers_symbol(symbol):
    """NSE:NIFTY2680424350CE (weekly) / NIFTY26AUG24350CE (monthly) / NIFTY26AUGFUT -> common key."""
    s = re.sub(r"^[A-Z]+:", "", str(symbol).strip().upper())
    m = re.match(r"^([A-Z]+)(.*)$", s)
    if not m:
        return s
    und, rest = m.group(1), m.group(2)
    if rest.endswith("FUT"):
        return f"{und}{rest[:-3]}FUT"
    if rest[-2:] in ("CE", "PE"):
        right = rest[-2:]
        rest = rest[:-2]
    else:
        return s
    if len(rest) < 3 or not rest[:2].isdigit():
        return s
    year = 2000 + int(rest[:2])
    tail = rest[2:]
    if tail[:3] in _OPT_MONTHS:                       # monthly: YY MMM STRIKE
        key = _canon_option(und, year, _OPT_MONTHS[tail[:3]], None, tail[3:] or 0, right)
        return key or s
    mc, dd, strike = tail[:1], tail[1:3], tail[3:]    # weekly: YY M DD STRIKE
    if mc in _FYERS_MONTH_CHAR and dd.isdigit() and strike.isdigit():
        key = _canon_option(und, year, _FYERS_MONTH_CHAR[mc], int(dd), strike, right)
        return key or s
    return s


def canonicalize_nubra_parts(symbol, expiry, strike, right):
    """Build the same common key from Nubra's decomposed expiry/strike/type columns."""
    m = re.match(r"^([A-Za-z]+)", str(symbol) if symbol is not None else "NIFTY")
    und = (m.group(1).upper() if m else "NIFTY")
    e = re.sub(r"[^0-9]", "", str(expiry))
    r = str(right).strip().upper()
    if len(e) == 8 and strike is not None and str(strike).strip() != "":
        key = _canon_option(und, int(e[:4]), int(e[4:6]), int(e[6:8]), strike, r)
        if key:
            return key
    return f"{und}{str(symbol)}"


_MON3 = {
    "JAN": "JAN", "FEB": "FEB", "MAR": "MAR", "APR": "APR",
    "MAY": "MAY", "JUN": "JUN", "JUL": "JUL", "AUG": "AUG",
    "SEP": "SEP", "OCT": "OCT", "NOV": "NOV", "DEC": "DEC",
}


def _dhan_to_canonical(symbol: str):
    s = str(symbol or "").strip().upper()
    opt = re.match(r"^([A-Z0-9]+)\s+(\d{1,2})\s+([A-Z]{3})\s+(\d+(?:\.\d+)?)\s+(CALL|PUT|CE|PE)$", s)
    if opt:
        und, day, mon, strike, right = opt.groups()
        right = "CE" if right in {"CALL", "CE"} else "PE"
        strike_text = str(int(float(strike))) if re.match(r"^\d+(?:\.0+)?$", strike) else strike
        return f"{und}20{pd.Timestamp.now(tz='Asia/Kolkata').year % 100:02d}{_OPT_MONTHS.get(mon, 0):02d}{int(day):02d}{strike_text}{right}"
    fut = re.match(r"^([A-Z]+)-([A-Z]{3})(\d{4})-FUT$", s)
    if fut:
        und, mon, year = fut.groups()
        return f"{und}{year[-2:]}{_MON3[mon]}FUT"
    fut2 = re.match(r"^([A-Z0-9]+)\s+([A-Z]{3})\s+FUT$", s)
    if fut2:
        und, mon = fut2.groups()
        year = pd.Timestamp.now(tz="Asia/Kolkata").year % 100
        return f"{und}{year:02d}{_MON3.get(mon, mon)}FUT"
    return s


def _gdfl_to_canonical(symbol: str):
    s = str(symbol or "").strip().upper()
    opt = re.match(r"^(?:OPTIDX|OPTSTK)_([A-Z0-9]+)_(\d{2})([A-Z]{3})(\d{4})_(CE|PE)_(\d+(?:\.\d+)?)$", s)
    if opt:
        und, _day, mon, year, option_type, strike = opt.groups()
        strike_text = str(int(float(strike))) if re.match(r"^\d+(?:\.0+)?$", strike) else strike
        return f"{und}{year[-2:]}{_MON3.get(mon, mon)}{strike_text}{option_type}"
    fut = re.match(r"^(?:FUTIDX|FUTSTK)_([A-Z0-9]+)_(\d{2})([A-Z]{3})(\d{4})_FUT$", s)
    if fut:
        und, _day, mon, year = fut.groups()
        return f"{und}{year[-2:]}{_MON3.get(mon, mon)}FUT"
    idx = re.match(r"^(?:IDX|SPOT)_([A-Z0-9]+)$", s)
    if idx:
        return idx.group(1)
    return s


def _load_fyers_rows(path_str: str | None, query: str):
    path = _resolve_path(path_str)
    if not path or not path.exists():
        return pd.DataFrame()
    headers = set(_csv_columns(path)) if path.suffix.lower() == ".csv" else (_duckdb_columns(path) if path.suffix.lower() == ".duckdb" else set())
    df = None
    if path.suffix.lower() in {".csv", ".duckdb"}:
        symbol_col = "symbol" if "symbol" in headers else ("native_symbol" if "native_symbol" in headers else None)
        timestamp_col = "datetime" if "datetime" in headers else None
        if symbol_col and timestamp_col:
            received_expr = (
                _sql_ident("received_at_ist") if "received_at_ist" in headers else
                _sql_ident("last_received_at_ist") if "last_received_at_ist" in headers else
                _sql_ident("first_received_at_ist") if "first_received_at_ist" in headers else
                _sql_ident(timestamp_col)
            )
            listed_expr = _sql_ident("listed_at_ist") if "listed_at_ist" in headers else _sql_ident(timestamp_col)
            df = _duckdb_project(
                path,
                [
                    ("symbol", _sql_ident(symbol_col)),
                    ("datetime", _sql_ident(timestamp_col)),
                    ("listed_at_ist", listed_expr),
                    ("received_at_ist", received_expr),
                    ("ltp", _sql_ident("ltp") if "ltp" in headers else "NULL"),
                    ("open_price", _sql_ident("open_price") if "open_price" in headers else "NULL"),
                    ("high_price", _sql_ident("high_price") if "high_price" in headers else "NULL"),
                    ("low_price", _sql_ident("low_price") if "low_price" in headers else "NULL"),
                    ("close_price", _sql_ident("close_price") if "close_price" in headers else "NULL"),
                    ("ask_price", _sql_ident("ask_price") if "ask_price" in headers else "NULL"),
                    ("bid_price", _sql_ident("bid_price") if "bid_price" in headers else "NULL"),
                    ("volume", _sql_ident("volume") if "volume" in headers else "NULL"),
                    ("open_interest", _sql_ident("open_interest") if "open_interest" in headers else "NULL"),
                    ("message_type", _sql_ident("message_type") if "message_type" in headers else "NULL"),
                ],
                _symbol_filter_sql(query, [symbol_col, "canonical_symbol"], headers),
            )
    if df is None:
        df = pd.read_csv(path)
    if df.empty or "symbol" not in df.columns or "datetime" not in df.columns:
        return pd.DataFrame()
    out = df.copy()
    out["native_symbol"] = out["symbol"].astype(str)
    out["canonical_symbol"] = out["native_symbol"].map(canonicalize_fyers_symbol)
    out = out[_match_symbol(out["canonical_symbol"], query) | _match_symbol(out["native_symbol"], query)]
    if out.empty:
        return out
    out["timestamp_ist"] = _to_ist_series(out["datetime"])
    listed_col = out["listed_at_ist"] if "listed_at_ist" in out.columns else out["datetime"]
    if "received_at_ist" in out.columns:
        received_col = out["received_at_ist"]
    elif "last_received_at_ist" in out.columns:
        # The 1-second FYERS rollup stores first/last receive times, not a generic received_at_ist.
        # Use the last receive time because the row LTP/listed time also reflects the last tick in that bucket.
        received_col = out["last_received_at_ist"]
    elif "first_received_at_ist" in out.columns:
        received_col = out["first_received_at_ist"]
    else:
        received_col = out["datetime"]
    out["listed_at_ist"] = _to_ist_series(listed_col)
    out["received_at_ist"] = _to_ist_series(received_col)
    out["receive_delay_ms"] = _delay_ms(out["received_at_ist"], out["listed_at_ist"])
    out["broker"] = "fyers"
    out["ltp"] = pd.to_numeric(out.get("ltp"), errors="coerce")
    out["o"] = pd.to_numeric(out.get("open_price"), errors="coerce")
    out["h"] = pd.to_numeric(out.get("high_price"), errors="coerce")
    out["l"] = pd.to_numeric(out.get("low_price"), errors="coerce")
    out["c"] = pd.to_numeric(out.get("close_price", out.get("ltp")), errors="coerce")
    out["ask"] = pd.to_numeric(out.get("ask_price"), errors="coerce")
    out["bid"] = pd.to_numeric(out.get("bid_price"), errors="coerce")
    out["volume"] = pd.to_numeric(out.get("volume"), errors="coerce")
    out["oi"] = pd.to_numeric(out.get("open_interest"), errors="coerce")
    out["message_type"] = out.get("message_type")
    return out[["broker", "timestamp_ist", "listed_at_ist", "received_at_ist", "receive_delay_ms", "canonical_symbol", "native_symbol", "ltp", "o", "h", "l", "c", "ask", "bid", "volume", "oi", "message_type"]]


def _load_dhan_rows(path_str: str | None, query: str):
    path = _resolve_path(path_str)
    if not path or not path.exists():
        return pd.DataFrame()
    # #region debug-point A:dhan-loader-entry
    _debug_event("A", "Server.py:_load_dhan_rows", "enter dhan loader", {
        "path": str(path),
        "query": str(query or ""),
        "suffix": path.suffix.lower(),
    })
    # #endregion
    headers = set(_csv_columns(path)) if path.suffix.lower() == ".csv" else (_duckdb_columns(path) if path.suffix.lower() == ".duckdb" else set())
    df = None
    if path.suffix.lower() in {".csv", ".duckdb"}:
        native_col = "native_symbol" if "native_symbol" in headers else ("symbol" if "symbol" in headers else None)
        timestamp_col = "datetime" if "datetime" in headers else ("timestamp" if "timestamp" in headers else None)
        if native_col and timestamp_col:
            received_expr = (
                _sql_ident("received_at_ist") if "received_at_ist" in headers else
                _sql_ident("last_received_at_ist") if "last_received_at_ist" in headers else
                _sql_ident("first_received_at_ist") if "first_received_at_ist" in headers else
                "NULL"
            )
            listed_expr = _sql_ident("listed_at_ist") if "listed_at_ist" in headers else _sql_ident(timestamp_col)
            df = _duckdb_project(
                path,
                [
                    ("native_symbol", _sql_ident(native_col)),
                    ("canonical_symbol", _sql_ident("canonical_symbol") if "canonical_symbol" in headers else "NULL"),
                    ("expiry", _sql_ident("expiry") if "expiry" in headers else "NULL"),
                    ("strike", _sql_ident("strike") if "strike" in headers else "NULL"),
                    ("instrument_type", _sql_ident("instrument_type") if "instrument_type" in headers else "NULL"),
                    ("datetime", _sql_ident(timestamp_col)),
                    ("listed_at_ist", listed_expr),
                    ("received_at_ist", received_expr),
                    ("ltp", _sql_ident("ltp") if "ltp" in headers else "NULL"),
                    ("open_price", _sql_ident("open_price") if "open_price" in headers else (_sql_ident("open") if "open" in headers else "NULL")),
                    ("high_price", _sql_ident("high_price") if "high_price" in headers else (_sql_ident("high") if "high" in headers else "NULL")),
                    ("low_price", _sql_ident("low_price") if "low_price" in headers else (_sql_ident("low") if "low" in headers else "NULL")),
                    ("close_price", _sql_ident("close_price") if "close_price" in headers else (_sql_ident("close") if "close" in headers else "NULL")),
                    ("ask_price", _sql_ident("ask_price") if "ask_price" in headers else (_sql_ident("ask1_price") if "ask1_price" in headers else "NULL")),
                    ("bid_price", _sql_ident("bid_price") if "bid_price" in headers else (_sql_ident("bid1_price") if "bid1_price" in headers else "NULL")),
                    ("volume", _sql_ident("volume") if "volume" in headers else "NULL"),
                    ("open_interest", _sql_ident("open_interest") if "open_interest" in headers else (_sql_ident("oi") if "oi" in headers else "NULL")),
                    ("message_type", _sql_ident("message_type") if "message_type" in headers else (_sql_ident("instrument_type") if "instrument_type" in headers else "NULL")),
                ],
                _symbol_filter_sql(query, [native_col, "canonical_symbol"], headers),
            )
    if df is None:
        df = pd.read_csv(path)
    # #region debug-point B:dhan-loader-read
    _debug_event("B", "Server.py:_load_dhan_rows", "raw dhan frame loaded", {
        "rows": int(len(df)) if df is not None else -1,
        "columns": list(df.columns[:20]) if df is not None else [],
    })
    # #endregion
    if df.empty:
        return pd.DataFrame()
    out = df.copy()
    native_col = "native_symbol" if "native_symbol" in out.columns else ("symbol" if "symbol" in out.columns else None)
    if native_col is None:
        return pd.DataFrame()
    out["native_symbol"] = out[native_col].astype(str)
    if "canonical_symbol" in out.columns:
        out["canonical_symbol"] = out["canonical_symbol"].fillna(out["native_symbol"]).astype(str)
    elif {"expiry", "strike", "instrument_type"}.issubset(set(out.columns)):
        out["canonical_symbol"] = out.apply(
            lambda r: canonicalize_nubra_parts(r.get("native_symbol"), r.get("expiry"), r.get("strike"), r.get("instrument_type")),
            axis=1,
        )
    else:
        out["canonical_symbol"] = out["native_symbol"].map(_dhan_to_canonical)
    out = out[_match_symbol(out["canonical_symbol"], query) | _match_symbol(out["native_symbol"], query)]
    # #region debug-point C:dhan-loader-filter
    _debug_event("C", "Server.py:_load_dhan_rows", "dhan rows after symbol filter", {
        "rows": int(len(out)),
        "sample_native": out["native_symbol"].head(3).astype(str).tolist() if not out.empty else [],
        "sample_canonical": out["canonical_symbol"].head(3).astype(str).tolist() if not out.empty else [],
    })
    # #endregion
    if out.empty:
        return out
    timestamp_col = "datetime" if "datetime" in out.columns else ("timestamp" if "timestamp" in out.columns else None)
    if timestamp_col is None:
        return pd.DataFrame()
    listed_col = out["listed_at_ist"] if "listed_at_ist" in out.columns else out[timestamp_col]
    if "received_at_ist" in out.columns:
        received_col = out["received_at_ist"]
    elif "last_received_at_ist" in out.columns:
        received_col = out["last_received_at_ist"]
    elif "first_received_at_ist" in out.columns:
        received_col = out["first_received_at_ist"]
    else:
        received_col = pd.Series([None] * len(out), index=out.index)
    out["timestamp_ist"], out["listed_at_ist"], out["received_at_ist"] = _repair_dhan_legacy_times(
        out[timestamp_col],
        listed_col,
        received_col,
    )
    out["receive_delay_ms"] = _delay_ms(out["received_at_ist"], out["listed_at_ist"])
    if "live_1s" in path.name.lower():
        tiny_negative = out["receive_delay_ms"].between(-1000, -0.000001, inclusive="both")
        out.loc[tiny_negative, "receive_delay_ms"] = 0.0
    # #region debug-point D:dhan-loader-timestamps
    _debug_event("D", "Server.py:_load_dhan_rows", "dhan timestamp mapping sample", {
        "timestamp_col": timestamp_col,
        "has_listed_at_ist": "listed_at_ist" in out.columns,
        "has_received_at_ist": "received_at_ist" in out.columns,
        "sample": out[["native_symbol", "timestamp_ist", "listed_at_ist", "received_at_ist", "receive_delay_ms"]]
        .head(5)
        .astype(object)
        .where(pd.notnull(out[["native_symbol", "timestamp_ist", "listed_at_ist", "received_at_ist", "receive_delay_ms"]].head(5)), None)
        .to_dict(orient="records"),
    })
    # #endregion
    out["broker"] = "dhan"
    out["ltp"] = pd.to_numeric(out.get("ltp"), errors="coerce")
    out["o"] = pd.to_numeric(out.get("open_price", out.get("open")), errors="coerce")
    out["h"] = pd.to_numeric(out.get("high_price", out.get("high")), errors="coerce")
    out["l"] = pd.to_numeric(out.get("low_price", out.get("low")), errors="coerce")
    out["c"] = pd.to_numeric(out.get("close_price", out.get("close", out.get("ltp"))), errors="coerce")
    out["ask"] = pd.to_numeric(out.get("ask_price", out.get("ask1_price")), errors="coerce")
    out["bid"] = pd.to_numeric(out.get("bid_price", out.get("bid1_price")), errors="coerce")
    out["volume"] = pd.to_numeric(out.get("volume"), errors="coerce")
    out["oi"] = pd.to_numeric(out.get("open_interest", out.get("oi")), errors="coerce")
    out["message_type"] = out.get("message_type", out.get("instrument_type"))
    out = out[["broker", "timestamp_ist", "listed_at_ist", "received_at_ist", "receive_delay_ms", "canonical_symbol", "native_symbol", "ltp", "o", "h", "l", "c", "ask", "bid", "volume", "oi", "message_type"]]
    metrics = ["ltp", "o", "h", "l", "c", "ask", "bid", "volume", "oi"]
    out = out.dropna(subset=metrics, how="all")
    if "live_1s" in path.name.lower():
        out = out.dropna(subset=["ltp"])
    # #region debug-point E:dhan-loader-output
    _debug_event("E", "Server.py:_load_dhan_rows", "dhan loader output", {
        "rows": int(len(out)),
        "ltp_non_null": int(out["ltp"].notna().sum()) if "ltp" in out.columns else 0,
        "delay_non_null": int(out["receive_delay_ms"].notna().sum()) if "receive_delay_ms" in out.columns else 0,
        "delay_negative": int((pd.to_numeric(out["receive_delay_ms"], errors="coerce") < 0).sum()) if "receive_delay_ms" in out.columns else 0,
    })
    # #endregion
    return out


def _load_nubra_rows(path_str: str | None, query: str):
    path = _resolve_path(path_str)
    if not path or not path.exists():
        return pd.DataFrame()
    # #region debug-point A:nubra-loader-entry
    _debug_event("A", "Server.py:_load_nubra_rows", "enter nubra loader", {
        "path": str(path),
        "query": str(query or ""),
        "suffix": path.suffix.lower(),
    }, session_id="nubra-ltp-missing")
    # #endregion
    suffix = path.suffix.lower()
    if suffix in {".csv", ".duckdb"} and duckdb is not None:
        headers = set(_csv_columns(path)) if suffix == ".csv" else _duckdb_columns(path)
        if "symbol" in headers and "datetime_IST" in headers:
            df = _duckdb_project(
                path,
                [
                    ("symbol", _sql_ident("symbol")),
                    ("datetime_IST", _sql_ident("datetime_IST")),
                    ("listed_at_ist", _sql_ident("listed_at_ist") if "listed_at_ist" in headers else _sql_ident("datetime_IST")),
                    ("received_at_IST", _sql_ident("received_at_IST") if "received_at_IST" in headers else _sql_ident("datetime_IST")),
                    ("expiry", _sql_ident("expiry") if "expiry" in headers else "NULL"),
                    ("strike", _sql_ident("strike") if "strike" in headers else "NULL"),
                    ("instrument_type", _sql_ident("instrument_type") if "instrument_type" in headers else "NULL"),
                    ("ltp", _sql_ident("ltp") if "ltp" in headers else "NULL"),
                    ("open", _sql_ident("open") if "open" in headers else "NULL"),
                    ("high", _sql_ident("high") if "high" in headers else "NULL"),
                    ("low", _sql_ident("low") if "low" in headers else "NULL"),
                    ("close", _sql_ident("close") if "close" in headers else "NULL"),
                    ("l1ask", _sql_ident("l1ask") if "l1ask" in headers else "NULL"),
                    ("l1bid", _sql_ident("l1bid") if "l1bid" in headers else "NULL"),
                    ("cumulative_volume", _sql_ident("cumulative_volume") if "cumulative_volume" in headers else "NULL"),
                    ("cumulative_oi", _sql_ident("cumulative_oi") if "cumulative_oi" in headers else "NULL"),
                ],
                _symbol_filter_sql(query, ["symbol"], headers),
            )
        else:
            df = pd.DataFrame()
    elif suffix == ".csv":
        df = pd.read_csv(path)
    elif suffix in {".parquet", ".pq"}:
        df = pd.read_parquet(path)
    else:
        try:
            df = pd.read_csv(path)
        except Exception:
            df = pd.read_parquet(path)
    # #region debug-point B:nubra-loader-read
    _debug_event("B", "Server.py:_load_nubra_rows", "raw nubra frame loaded", {
        "rows": int(len(df)) if df is not None else -1,
        "columns": list(df.columns[:25]) if df is not None else [],
    }, session_id="nubra-ltp-missing")
    # #endregion
    if df.empty or "symbol" not in df.columns or "datetime_IST" not in df.columns:
        return pd.DataFrame()
    out = df.copy()
    out["native_symbol"] = out["symbol"].astype(str)
    out["canonical_symbol"] = out.apply(
        lambda r: canonicalize_nubra_parts(r.get("symbol"), r.get("expiry"), r.get("strike"), r.get("instrument_type")),
        axis=1,
    )
    out = out[_match_symbol(out["canonical_symbol"], query) | _match_symbol(out["native_symbol"], query)]
    # #region debug-point C:nubra-loader-filter
    _debug_event("C", "Server.py:_load_nubra_rows", "nubra rows after symbol filter", {
        "rows": int(len(out)),
        "sample_native": out["native_symbol"].head(3).astype(str).tolist() if not out.empty else [],
        "sample_canonical": out["canonical_symbol"].head(3).astype(str).tolist() if not out.empty else [],
    }, session_id="nubra-ltp-missing")
    # #endregion
    if out.empty:
        return out
    out["timestamp_ist"] = _to_ist_series(out["datetime_IST"])
    listed_col = out["listed_at_ist"] if "listed_at_ist" in out.columns else out["datetime_IST"]
    received_col = out["received_at_IST"] if "received_at_IST" in out.columns else out["datetime_IST"]
    out["listed_at_ist"] = _to_ist_series(listed_col)
    out["received_at_ist"] = _to_ist_series(received_col)
    out["receive_delay_ms"] = _delay_ms(out["received_at_ist"], out["listed_at_ist"])
    out["broker"] = "nubra"
    nubra_ltp = pd.to_numeric(out.get("ltp"), errors="coerce").astype("float64")
    nubra_close = pd.to_numeric(out.get("close"), errors="coerce").astype("float64")
    out["ltp"] = nubra_ltp.fillna(nubra_close)
    out["o"] = pd.to_numeric(out.get("open"), errors="coerce")
    out["h"] = pd.to_numeric(out.get("high"), errors="coerce")
    out["l"] = pd.to_numeric(out.get("low"), errors="coerce")
    out["c"] = pd.to_numeric(out.get("close"), errors="coerce")
    out["ask"] = pd.to_numeric(out.get("l1ask"), errors="coerce")
    out["bid"] = pd.to_numeric(out.get("l1bid"), errors="coerce")
    out["volume"] = pd.to_numeric(out.get("cumulative_volume"), errors="coerce")
    out["oi"] = pd.to_numeric(out.get("cumulative_oi"), errors="coerce")
    out["message_type"] = out.get("instrument_type")
    # #region debug-point D:nubra-loader-price-map
    _debug_event("D", "Server.py:_load_nubra_rows", "nubra price mapping sample", {
        "has_ltp_col": "ltp" in out.columns,
        "has_close_col": "close" in out.columns,
        "sample": out[["native_symbol", "ltp", "o", "h", "l", "c", "receive_delay_ms"]]
        .head(5)
        .astype(object)
        .where(pd.notnull(out[["native_symbol", "ltp", "o", "h", "l", "c", "receive_delay_ms"]].head(5)), None)
        .to_dict(orient="records"),
    }, session_id="nubra-ltp-missing")
    # #endregion
    out = out[["broker", "timestamp_ist", "listed_at_ist", "received_at_ist", "receive_delay_ms", "canonical_symbol", "native_symbol", "ltp", "o", "h", "l", "c", "ask", "bid", "volume", "oi", "message_type"]]
    metrics = ["ltp", "o", "h", "l", "c", "ask", "bid", "volume", "oi"]
    out = out.dropna(subset=metrics, how="all")
    # #region debug-point E:nubra-loader-output
    _debug_event("E", "Server.py:_load_nubra_rows", "nubra loader output", {
        "rows": int(len(out)),
        "ltp_non_null": int(out["ltp"].notna().sum()) if "ltp" in out.columns else 0,
        "close_non_null": int(out["c"].notna().sum()) if "c" in out.columns else 0,
        "delay_non_null": int(out["receive_delay_ms"].notna().sum()) if "receive_delay_ms" in out.columns else 0,
    }, session_id="nubra-ltp-missing")
    # #endregion
    return out


def _load_xts_rows(path_str: str | None, query: str):
    path = _resolve_path(path_str)
    if not path or not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path)
    if df.empty:
        return pd.DataFrame()
    symbol_col = "canonical_symbol" if "canonical_symbol" in df.columns else None
    ts_col = "datetime_ist" if "datetime_ist" in df.columns else None
    if symbol_col is None or ts_col is None:
        return pd.DataFrame()
    out = df.copy()
    out["native_symbol"] = out.get("native_symbol", out[symbol_col]).astype(str)
    out["canonical_symbol"] = out[symbol_col].astype(str)
    out = out[_match_symbol(out["canonical_symbol"], query) | _match_symbol(out["native_symbol"], query)]
    if out.empty:
        return out
    out["timestamp_ist"] = _to_ist_series(out[ts_col])
    out["listed_at_ist"] = out["timestamp_ist"]
    received_col = out["received_at_ist"] if "received_at_ist" in out.columns else out[ts_col]
    out["received_at_ist"] = _to_ist_series(received_col)
    out["receive_delay_ms"] = _delay_ms(out["received_at_ist"], out["listed_at_ist"])
    out["broker"] = "xts"
    out["ltp"] = pd.to_numeric(out.get("ltp"), errors="coerce")
    out["o"] = pd.to_numeric(out.get("open_price"), errors="coerce")
    out["h"] = pd.to_numeric(out.get("high_price"), errors="coerce")
    out["l"] = pd.to_numeric(out.get("low_price"), errors="coerce")
    out["c"] = pd.to_numeric(out.get("close_price", out.get("ltp")), errors="coerce")
    out["ask"] = pd.to_numeric(out.get("ask_price"), errors="coerce")
    out["bid"] = pd.to_numeric(out.get("bid_price"), errors="coerce")
    out["volume"] = pd.to_numeric(out.get("volume"), errors="coerce")
    out["oi"] = pd.to_numeric(out.get("open_interest"), errors="coerce")
    out["message_type"] = out.get("message_type")
    out = out[["broker", "timestamp_ist", "listed_at_ist", "received_at_ist", "receive_delay_ms", "canonical_symbol", "native_symbol", "ltp", "o", "h", "l", "c", "ask", "bid", "volume", "oi", "message_type"]]
    metrics = ["ltp", "o", "h", "l", "c", "ask", "bid", "volume", "oi"]
    return out.dropna(subset=metrics, how="all")


def _load_gdfl_rows(path_str: str | None, query: str):
    path = _resolve_path(path_str)
    if not path or not path.exists():
        return pd.DataFrame()
    df = pd.read_parquet(path)
    if df.empty or "symbol" not in df.columns:
        return pd.DataFrame()
    timestamp_col = "time_date" if "time_date" in df.columns else ("timestamp" if "timestamp" in df.columns else None)
    if not timestamp_col:
        return pd.DataFrame()
    out = df.copy()
    out["native_symbol"] = out["symbol"].astype(str)
    out["canonical_symbol"] = out["native_symbol"].map(_gdfl_to_canonical)
    out = out[_match_symbol(out["canonical_symbol"], query) | _match_symbol(out["native_symbol"], query)]
    if out.empty:
        return out
    out["timestamp_ist"] = pd.to_datetime(out[timestamp_col], unit="ms", utc=True, errors="coerce").dt.tz_convert("Asia/Kolkata")
    out["listed_at_ist"] = out["timestamp_ist"]
    out["received_at_ist"] = pd.NaT
    out["receive_delay_ms"] = None
    out["broker"] = "gdfl"
    out["ltp"] = pd.to_numeric(out.get("ltp"), errors="coerce")
    out["o"] = pd.to_numeric(out.get("open"), errors="coerce")
    out["h"] = pd.to_numeric(out.get("high"), errors="coerce")
    out["l"] = pd.to_numeric(out.get("low"), errors="coerce")
    out["c"] = pd.to_numeric(out.get("prev_close", out.get("ltp")), errors="coerce")
    out["ask"] = pd.to_numeric(out.get("ask"), errors="coerce")
    out["bid"] = pd.to_numeric(out.get("bid"), errors="coerce")
    out["volume"] = pd.to_numeric(out.get("volume", out.get("tick_volume")), errors="coerce")
    out["oi"] = pd.to_numeric(out.get("oi"), errors="coerce")
    out["message_type"] = out.get("option_type", out.get("instrument_type"))
    out = out[["broker", "timestamp_ist", "listed_at_ist", "received_at_ist", "receive_delay_ms", "canonical_symbol", "native_symbol", "ltp", "o", "h", "l", "c", "ask", "bid", "volume", "oi", "message_type"]]
    metrics = ["ltp", "o", "h", "l", "c", "ask", "bid", "volume", "oi"]
    return out.dropna(subset=metrics, how="all")


def _run_analyze(args: list[str]) -> dict:
    p = subprocess.run(
        [sys.executable, str(HERE / "Analyze.py"), *args],
        capture_output=True,
        text=True,
    )
    out = (p.stdout or "") + (("\n" + p.stderr) if p.stderr else "")
    return {"ok": p.returncode == 0, "code": p.returncode, "report": out.strip()}


def _load_alphabot_rows(path_str: str | None, start: str | None = None, end: str | None = None, broker_filter: str | None = None):
    path = _resolve_path(path_str)
    if not path or not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path)
    needed = {"Symbol", "Broker", "Entry Price", "Entry At", "Exit Price", "Exit At"}
    if df.empty or not needed.issubset(set(df.columns)):
        return pd.DataFrame()

    out = df.copy()
    out["native_symbol"] = out["Symbol"].astype(str).str.strip()
    out["canonical_symbol"] = out["native_symbol"].str.replace(r"^[A-Z]+:", "", regex=True)
    out["alphabot_broker"] = out["Broker"].astype(str).str.strip()
    out["entry_side"] = out.get("Entry Side")
    out["exit_side"] = out.get("Exit Side")
    out["entry_qty"] = pd.to_numeric(out.get("Entry Quantity"), errors="coerce")
    out["exit_qty"] = pd.to_numeric(out.get("Exit Quantity"), errors="coerce")
    out["entry_price"] = pd.to_numeric(out.get("Entry Price"), errors="coerce")
    out["exit_price"] = pd.to_numeric(out.get("Exit Price"), errors="coerce")
    out["pnl"] = pd.to_numeric(out.get("PnL"), errors="coerce")
    out["charges"] = pd.to_numeric(out.get("Total Tax & Charges"), errors="coerce")
    # AlphaBot exports can switch between HH:MM and HH:MM:SS. Let pandas parse both.
    out["entry_at"] = pd.to_datetime(out["Entry At"], errors="coerce", dayfirst=True)
    out["exit_at"] = pd.to_datetime(out["Exit At"], errors="coerce", dayfirst=True)
    out["canonical_symbol"] = out["canonical_symbol"].replace({"": pd.NA, "NAN": pd.NA})
    out = out.dropna(subset=["entry_at", "exit_at", "canonical_symbol", "entry_price", "exit_price"])
    out["entry_at"] = out["entry_at"].dt.tz_localize("Asia/Kolkata")
    out["exit_at"] = out["exit_at"].dt.tz_localize("Asia/Kolkata")

    if broker_filter:
        out = out[out["alphabot_broker"].str.upper() == str(broker_filter).strip().upper()]
    if out.empty:
        return out

    lo = _parse_bound(start)
    hi = _parse_bound(end)
    if lo is not None:
        out = out[out["exit_at"] >= lo]
    if hi is not None:
        out = out[out["entry_at"] <= hi]
    if out.empty:
        return out

    out = out.reset_index(drop=True)
    out["trade_id"] = out.index.astype(int)
    return out[[
        "trade_id",
        "native_symbol",
        "canonical_symbol",
        "alphabot_broker",
        "entry_side",
        "exit_side",
        "entry_qty",
        "exit_qty",
        "entry_price",
        "exit_price",
        "entry_at",
        "exit_at",
        "pnl",
        "charges",
    ]]


def _nearest_market_match(trades: pd.DataFrame, market: pd.DataFrame, when_col: str, broker: str, tolerance_seconds: int):
    base_cols = ["trade_id", "canonical_symbol", when_col]
    if trades.empty:
        return pd.DataFrame(columns=[
            "trade_id",
            f"{broker}_{when_col}_time",
            f"{broker}_{when_col}_ltp",
            f"{broker}_{when_col}_delta_sec",
        ])

    left = trades[base_cols].dropna(subset=[when_col]).copy()
    left[when_col] = pd.to_datetime(left[when_col]).astype("datetime64[ns, Asia/Kolkata]")
    left = left.sort_values([when_col, "canonical_symbol", "trade_id"]).reset_index(drop=True)
    right = market[["canonical_symbol", "timestamp_ist", "ltp"]].dropna(subset=["canonical_symbol", "timestamp_ist", "ltp"])
    if right.empty:
        out = left[["trade_id"]].copy()
        out[f"{broker}_{when_col}_time"] = pd.NaT
        out[f"{broker}_{when_col}_ltp"] = pd.NA
        out[f"{broker}_{when_col}_delta_sec"] = pd.NA
        return out

    right = right.copy()
    right["timestamp_ist"] = pd.to_datetime(right["timestamp_ist"]).astype("datetime64[ns, Asia/Kolkata]")
    right = right.sort_values(["timestamp_ist", "canonical_symbol"]).reset_index(drop=True)
    merged = pd.merge_asof(
        left,
        right,
        left_on=when_col,
        right_on="timestamp_ist",
        by="canonical_symbol",
        direction="nearest",
        tolerance=pd.Timedelta(seconds=max(1, int(tolerance_seconds))),
    )
    out = merged[["trade_id", "timestamp_ist", "ltp"]].copy()
    out[f"{broker}_{when_col}_delta_sec"] = (merged["timestamp_ist"] - merged[when_col]).abs().dt.total_seconds()
    out = out.rename(columns={
        "timestamp_ist": f"{broker}_{when_col}_time",
        "ltp": f"{broker}_{when_col}_ltp",
    })
    return out


def _alpha_stats(df: pd.DataFrame, broker: str):
    rows = []
    for side in ("entry", "exit"):
        diff_col = f"{broker}_{side}_diff"
        abs_col = f"{broker}_{side}_abs_diff"
        delta_col = f"{broker}_{side}_at_delta_sec"
        matched = df[diff_col].notna()
        rows.append({
            "broker": broker,
            "event": side,
            "matched_trades": int(matched.sum()),
            "mean_diff": round(float(df.loc[matched, diff_col].mean()), 6) if matched.any() else None,
            "median_diff": round(float(df.loc[matched, diff_col].median()), 6) if matched.any() else None,
            "mean_abs_diff": round(float(df.loc[matched, abs_col].mean()), 6) if matched.any() else None,
            "median_abs_diff": round(float(df.loc[matched, abs_col].median()), 6) if matched.any() else None,
            "max_abs_diff": round(float(df.loc[matched, abs_col].max()), 6) if matched.any() else None,
            "avg_time_delta_sec": round(float(df.loc[matched, delta_col].mean()), 3) if matched.any() else None,
        })
    return rows


# def _load_second_compare_rows(fyers_csv: str | None, nubra_parquet: str | None, symbol: str, from_ts: str | None, to_ts: str | None):
def _load_second_compare_rows(fyers_csv: str | None, nubra_csv: str | None, symbol: str, from_ts: str | None, to_ts: str | None):
    frames = [
        _load_fyers_rows(fyers_csv, symbol),
        # _load_nubra_rows(nubra_parquet, symbol),
        _load_nubra_rows(nubra_csv, symbol),
    ]
    frames = [f for f in frames if f is not None and not f.empty]
    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True)
    df = df[df["broker"].isin(["fyers", "nubra"])].copy()
    df = _apply_range(df, from_ts, to_ts)
    if df.empty:
        return df
    df["sec"] = df["timestamp_ist"].dt.floor("1s")
    return df


def _second_last_ltp(df: pd.DataFrame, broker: str):
    if df.empty:
        return pd.DataFrame(columns=["sec", "ltp", "timestamp_ist", "ticks"])
    out = df[df["broker"] == broker].dropna(subset=["ltp"]).copy()
    if out.empty:
        return pd.DataFrame(columns=["sec", "ltp", "timestamp_ist", "ticks"])
    out = out.sort_values(["sec", "timestamp_ist"])
    agg = out.groupby("sec", as_index=False).agg(
        ltp=("ltp", "last"),
        timestamp_ist=("timestamp_ist", "last"),
        ticks=("ltp", "size"),
    )
    return agg


def _build_histogram(series: pd.Series, bins: int):
    clean = pd.to_numeric(series, errors="coerce").dropna()
    if clean.empty:
        return []
    values = clean.astype(float)
    lo = float(values.min())
    hi = float(values.max())
    if lo == hi:
        return [{
            "start": lo,
            "end": hi,
            "label": f"{lo:.4f}",
            "count": int(len(values)),
        }]

    max_abs = max(abs(lo), abs(hi))
    start = -max_abs
    end = max_abs
    bucket_count = max(5, min(int(bins or 21), 61))
    width = (end - start) / bucket_count if end != start else 1.0
    out = []
    for idx in range(bucket_count):
        left = start + idx * width
        right = start + (idx + 1) * width
        if idx == bucket_count - 1:
            count = int(((values >= left) & (values <= right)).sum())
        else:
            count = int(((values >= left) & (values < right)).sum())
        out.append({
            "start": round(left, 6),
            "end": round(right, 6),
            "label": f"{left:.2f} to {right:.2f}",
            "count": count,
        })
    return out


def _round_or_none(value, digits=6):
    if value is None or pd.isna(value):
        return None
    return round(float(value), digits)


def _extract_date_tags(*values):
    tags = []
    for value in values:
        if not value:
            continue
        text = str(value)
        found = re.findall(r"\d{4}-\d{2}-\d{2}", text)
        # File names can contain an expiry date plus a session date; keep the trailing date for path-like values.
        if ("/" in text or "\\" in text or "." in Path(text).name) and len(found) > 1:
            found = [found[-1]]
        tags.extend(found)
    return sorted(set(tags))


def _score_broker_components(coverage_ratio, match_rate, median_abs_pct):
    coverage_score = max(0.0, min(100.0, coverage_ratio * 100.0))
    match_score = max(0.0, min(100.0, match_rate * 100.0))
    if median_abs_pct is None or pd.isna(median_abs_pct):
        closeness_score = 0.0
    else:
        diff_bps = float(median_abs_pct) * 10000.0
        closeness_score = max(0.0, min(100.0, 100.0 - diff_bps / 2.0))
    return {
        "coverage_score": round(coverage_score, 2),
        "match_score": round(match_score, 2),
        "closeness_score": round(closeness_score, 2),
    }


def _score_broker_candidate(coverage_ratio, match_rate, median_abs_pct):
    parts = _score_broker_components(coverage_ratio, match_rate, median_abs_pct)
    return round(parts["coverage_score"] * 0.45 + parts["match_score"] * 0.20 + parts["closeness_score"] * 0.35, 2)


def _score_alphabot_components(coverage_ratio, median_abs_pct, avg_time_delta_sec):
    coverage_score = max(0.0, min(100.0, coverage_ratio * 100.0))
    if median_abs_pct is None or pd.isna(median_abs_pct):
        closeness_score = 0.0
    else:
        diff_bps = float(median_abs_pct) * 10000.0
        closeness_score = max(0.0, min(100.0, 100.0 - diff_bps / 2.0))
    if avg_time_delta_sec is None or pd.isna(avg_time_delta_sec):
        timing_score = 0.0
    else:
        timing_score = max(0.0, min(100.0, 100.0 - float(avg_time_delta_sec) * 1.5))
    return {
        "coverage_score": round(coverage_score, 2),
        "closeness_score": round(closeness_score, 2),
        "timing_score": round(timing_score, 2),
    }


def _score_alphabot_candidate(coverage_ratio, median_abs_pct, avg_time_delta_sec):
    parts = _score_alphabot_components(coverage_ratio, median_abs_pct, avg_time_delta_sec)
    return round(parts["coverage_score"] * 0.40 + parts["closeness_score"] * 0.40 + parts["timing_score"] * 0.20, 2)


def _decision_metric_help(mode: str):
    common = [
        {
            "field": "Score",
            "meaning": "A weighted heuristic from 0 to 100. Higher is better, but it is not a ground-truth exchange accuracy score.",
            "good_sign": "Use it to rank candidates after checking the component scores and warnings.",
        },
        {
            "field": "Coverage %",
            "meaning": "How much of the reference timeline is also present in the candidate source.",
            "good_sign": "Higher is better. Low coverage usually means missing seconds, missing trades, or the wrong file/day.",
        },
        {
            "field": "Median Abs Diff",
            "meaning": "The typical absolute price gap between the candidate and the reference after alignment.",
            "good_sign": "Lower is better. It is more robust than mean diff because spikes do not dominate it.",
        },
        {
            "field": "Distribution",
            "meaning": "Shows whether price differences are centered near zero or skewed to one side.",
            "good_sign": "A tight cluster near zero is healthier than a wide or one-sided shape.",
        },
    ]
    if mode == "broker":
        return {
            "summary": "Practical feed benchmarking usually focuses on completeness, price agreement, and timestamp alignment against a chosen benchmark rather than trusting one absolute score.",
            "score_formula": "Broker score = 45% coverage + 20% exact-second match + 35% price closeness.",
            "fields": common + [
                {
                    "field": "Match %",
                    "meaning": "Among overlapping seconds, how often both sources end that second on exactly the same LTP.",
                    "good_sign": "Higher is better, but it is strict. Even tiny price timing differences can reduce it.",
                },
                {
                    "field": "Details Table",
                    "meaning": "Worst aligned seconds by absolute difference, with tick counts from both sources.",
                    "good_sign": "Use it to inspect whether bad score comes from a few spikes or a broad mismatch.",
                },
            ],
            "notes": [
                "Industry comparisons usually emphasize completeness and point-in-time price agreement before trusting a feed for trading decisions.",
                "This dashboard does not measure true exchange latency because the capture files do not contain exchange event timestamps for both brokers.",
            ],
        }
    return {
        "summary": "When AlphaBot is the reference, the main question is which feed better matches the prices and timestamps that your strategy actually traded around.",
        "score_formula": "AlphaBot score = 40% event coverage + 40% price closeness + 20% timestamp closeness.",
        "fields": common + [
            {
                "field": "Avg Time Delta",
                "meaning": "Average time gap in seconds between the AlphaBot event and the nearest matched market record.",
                "good_sign": "Lower is better. Big gaps mean the feed is not well aligned with your execution timestamps.",
            },
            {
                "field": "Matched Events",
                "meaning": "How many AlphaBot entry and exit events found a nearby market record within the chosen tolerance.",
                "good_sign": "Higher is better. If this is low, widen the tolerance or check same-day files.",
            },
        ],
        "notes": [
            "Execution-oriented comparison is usually closer to real trading usefulness than feed-vs-feed comparison alone.",
            "A feed can have low raw price error but still be operationally weak if timestamp matching is poor.",
        ],
    }


def _decision_input_warnings(reference: str, symbol: str | None, from_ts: str | None, to_ts: str | None, fyers_csv: str | None, dhan_csv: str | None, nubra_csv: str | None, gdfl_parquet: str | None, alpha_csv: str | None):
# def _decision_input_warnings(reference: str, symbol: str | None, from_ts: str | None, to_ts: str | None, fyers_csv: str | None, nubra_parquet: str | None, gdfl_parquet: str | None, alpha_csv: str | None):
    warnings = []
    if from_ts and to_ts:
        start = pd.to_datetime(from_ts, errors="coerce")
        end = pd.to_datetime(to_ts, errors="coerce")
        if pd.notna(start) and pd.notna(end) and start > end:
            warnings.append({
                "level": "high",
                "title": "Selected range is reversed",
                "message": "The 'From' timestamp is later than the 'To' timestamp, so the comparison window is invalid.",
            })
    date_tags = _extract_date_tags(fyers_csv, dhan_csv, nubra_csv, gdfl_parquet, alpha_csv, from_ts, to_ts)
    # date_tags = _extract_date_tags(fyers_csv, nubra_parquet, gdfl_parquet, alpha_csv, from_ts, to_ts)
    if len(date_tags) > 1:
        warnings.append({
            "level": "medium",
            "title": "Inputs appear to span multiple dates",
            "message": f"Detected dates: {', '.join(date_tags)}. Cross-day file selection often creates misleading comparisons.",
        })
    if reference in {"fyers", "dhan", "nubra", "gdfl"} and not str(symbol or "").strip():
        warnings.append({
            "level": "medium",
            "title": "Symbol is empty",
            "message": "Broker-vs-broker mode works best when you select one canonical symbol that exists in both files.",
        })
    return warnings


def _source_stats_from_df(df: pd.DataFrame, broker: str, path_str: str | None):
    if df is None or df.empty or "broker" not in df.columns:
        return {
            "source": broker,
            "path": path_str,
            "detected_dates": _extract_date_tags(path_str),
            "rows": 0,
            "active_seconds": 0,
            "from_ts": None,
            "to_ts": None,
        }
    sub = df[df["broker"] == broker].copy()
    if sub.empty:
        return {
            "source": broker,
            "path": path_str,
            "detected_dates": _extract_date_tags(path_str),
            "rows": 0,
            "active_seconds": 0,
            "from_ts": None,
            "to_ts": None,
        }
    return {
        "source": broker,
        "path": path_str,
        "detected_dates": _extract_date_tags(path_str),
        "rows": int(len(sub)),
        "active_seconds": int(sub["sec"].nunique()) if "sec" in sub else int(sub["timestamp_ist"].dt.floor("1s").nunique()),
        "from_ts": _fmt_scalar_ts(sub["timestamp_ist"].min()),
        "to_ts": _fmt_scalar_ts(sub["timestamp_ist"].max()),
    }


def _broker_compare_warnings(compares: list[dict], sources: list[dict]):
    warnings = []
    dates = sorted({d for item in sources for d in item.get("detected_dates", [])})
    if len(dates) > 1:
        warnings.append({
            "level": "medium",
            "title": "Source files may belong to different market days",
            "message": f"Detected file dates: {', '.join(dates)}. Compare same-day exports for a fair result.",
        })
    if any((compare.get("coverage_ratio") or 0) < 0.80 for compare in compares):
        warnings.append({
            "level": "high",
            "title": "Coverage is low",
            "message": "The candidate is missing a large part of the reference timeline. This usually matters more than a small price error.",
        })
    if any((compare.get("match_rate") or 0) < 0.50 for compare in compares):
        warnings.append({
            "level": "medium",
            "title": "Exact-second match rate is low",
            "message": "The two feeds often end the same second on different last prices. That can happen with timing drift, coalescing, or missing ticks.",
        })
    if any(compare.get("median_abs_pct") is not None and compare["median_abs_pct"] > 0.002 for compare in compares):
        warnings.append({
            "level": "medium",
            "title": "Typical price gap is meaningful",
            "message": "Median absolute difference is above 20 bps of the reference median price, which is large for a like-for-like feed comparison.",
        })
    return warnings


# def _alphabot_compare_warnings(candidates: list[dict], alpha_csv: str | None, fyers_csv: str | None, nubra_parquet: str | None, gdfl_parquet: str | None):
def _alphabot_compare_warnings(candidates: list[dict], alpha_csv: str | None, fyers_csv: str | None, dhan_csv: str | None, nubra_csv: str | None, gdfl_parquet: str | None):
    warnings = []
    dates = _extract_date_tags(alpha_csv, fyers_csv, dhan_csv, nubra_csv, gdfl_parquet)
    # dates = _extract_date_tags(alpha_csv, fyers_csv, nubra_parquet, gdfl_parquet)
    if len(dates) > 1:
        warnings.append({
            "level": "medium",
            "title": "AlphaBot and broker files may be from different days",
            "message": f"Detected file dates: {', '.join(dates)}. Entry/exit comparison is only trustworthy when all files refer to the same session.",
        })
    for candidate in candidates:
        if (candidate.get("coverage_ratio") or 0) < 0.80:
            warnings.append({
                "level": "high",
                "title": f"{candidate['candidate']} matched too few AlphaBot events",
                "message": "Low event coverage usually means missing same-day data, too narrow tolerance, or wrong symbols.",
            })
        if candidate.get("avg_time_delta_sec") is not None and candidate["avg_time_delta_sec"] > 15:
            warnings.append({
                "level": "medium",
                "title": f"{candidate['candidate']} has large timestamp gap",
                "message": "Even if prices look close, a large average time delta weakens the usefulness of the feed for execution analysis.",
            })
    return warnings


def _broker_verdict(compares: list[dict]):
    if not compares:
        return "No broker candidate aligned with the chosen reference."
    leader = sorted(compares, key=lambda x: x.get("score") or 0, reverse=True)[0]
    coverage = leader.get("coverage_ratio") or 0
    match_rate = leader.get("match_rate") or 0
    median_abs_diff = leader.get("median_abs_diff")
    if len(compares) == 1:
        if coverage >= 0.95 and match_rate >= 0.60:
            return f"{leader['candidate']} is strong against {leader['reference']}: overlap is high and price agreement is reasonably stable."
        if coverage < 0.80:
            return f"{leader['candidate']} is not reliable enough against {leader['reference']} yet because too much of the reference timeline is missing."
        if median_abs_diff is not None:
            return f"{leader['candidate']} overlaps well, but the decision still depends on whether a typical absolute gap of {median_abs_diff} is acceptable for your strategy."
        return f"{leader['candidate']} can be ranked, but the overlap is too thin to trust the result strongly."
    runner = sorted(compares, key=lambda x: x.get("score") or 0, reverse=True)[1]
    gap = round(float(leader.get("score") or 0) - float(runner.get("score") or 0), 2)
    if gap >= 5:
        return f"{leader['candidate']} is the clearest match against {leader['reference']} in this window, ahead of {runner['candidate']} by {gap} score points."
    return f"{leader['candidate']} currently leads against {leader['reference']}, but the gap to {runner['candidate']} is small enough that you should inspect raw differences before deciding."


def _alphabot_verdict(candidates: list[dict]):
    if not candidates:
        return "No broker matched enough AlphaBot events to make a comparison."
    leader = candidates[0]
    if len(candidates) == 1:
        return f"{leader['candidate']} is the only broker with usable AlphaBot matches in the selected window."
    runner = candidates[1]
    gap = round(float(leader["score"]) - float(runner["score"]), 2)
    if gap >= 5:
        return f"{leader['candidate']} is clearly closer to AlphaBot than {runner['candidate']} in this window."
    return f"{leader['candidate']} edges {runner['candidate']}, but the score gap is small so you should inspect the raw diff and timing fields as well."


# def _build_broker_compare_frame(fyers_csv: str | None, nubra_parquet: str | None, gdfl_parquet: str | None, symbol: str, from_ts: str | None, to_ts: str | None):
def _build_broker_compare_frame(fyers_csv: str | None, dhan_csv: str | None, nubra_csv: str | None, gdfl_parquet: str | None, symbol: str, from_ts: str | None, to_ts: str | None):
    frames = [
        _load_fyers_rows(fyers_csv, symbol),
        _load_dhan_rows(dhan_csv, symbol),
        _load_nubra_rows(nubra_csv, symbol),
        # _load_nubra_rows(nubra_parquet, symbol),
        _load_gdfl_rows(gdfl_parquet, symbol),
    ]
    frames = [f for f in frames if f is not None and not f.empty]
    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True)
    df = _apply_range(df, from_ts, to_ts)
    if df.empty:
        return df
    df = df[df["broker"].isin(["fyers", "dhan", "nubra", "gdfl"])].copy()
    df["sec"] = df["timestamp_ist"].dt.floor("1s")
    return df


def _build_receive_compare_frame(fyers_csv: str | None, dhan_csv: str | None, nubra_csv: str | None, xts_csv: str | None, symbol: str, from_ts: str | None, to_ts: str | None):
# def _build_receive_compare_frame(fyers_csv: str | None, nubra_parquet: str | None, xts_csv: str | None, symbol: str, from_ts: str | None, to_ts: str | None):
    frames = [
        _load_fyers_rows(fyers_csv, symbol),
        _load_dhan_rows(dhan_csv, symbol),
        _load_nubra_rows(nubra_csv, symbol),
        # _load_nubra_rows(nubra_parquet, symbol),
        _load_xts_rows(xts_csv, symbol),
    ]
    frames = [f for f in frames if f is not None and not f.empty]
    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True)
    df = _apply_range(df, from_ts, to_ts)
    if df.empty:
        return df
    df = df[df["broker"].isin(["fyers", "dhan", "nubra", "xts"])].copy()
    df["sec"] = df["timestamp_ist"].dt.floor("1s")
    return df


def _receive_delay_summary(df: pd.DataFrame, broker: str):
    out = df[df["broker"] == broker].copy()
    if out.empty:
        return None
    delays = pd.to_numeric(out["receive_delay_ms"], errors="coerce").dropna()
    listed = out["listed_at_ist"].dropna()
    received = out["received_at_ist"].dropna()
    return {
        "broker": broker,
        "rows": int(len(out)),
        "active_seconds": int(out["sec"].nunique()),
        "from_ts": _fmt_scalar_ts(out["timestamp_ist"].min()),
        "to_ts": _fmt_scalar_ts(out["timestamp_ist"].max()),
        "listed_from": _fmt_scalar_ts(listed.min()) if not listed.empty else None,
        "listed_to": _fmt_scalar_ts(listed.max()) if not listed.empty else None,
        "received_from": _fmt_scalar_ts(received.min()) if not received.empty else None,
        "received_to": _fmt_scalar_ts(received.max()) if not received.empty else None,
        "median_delay_ms": _round_or_none(delays.median() if not delays.empty else None, 3),
        "p90_delay_ms": _round_or_none(delays.quantile(0.90) if not delays.empty else None, 3),
        "max_delay_ms": _round_or_none(delays.max() if not delays.empty else None, 3),
        "min_delay_ms": _round_or_none(delays.min() if not delays.empty else None, 3),
    }


def _classify_source(path_str, loaded):
    """Explain what happened to one source: blank / missing / no matching rows / loaded."""
    if not path_str or not str(path_str).strip():
        return "blank (skipped)", 0
    p = _resolve_path(path_str)
    if p is None or not p.exists():
        return "missing file (skipped)", 0
    if loaded is None or (hasattr(loaded, "empty") and loaded.empty):
        return "no matching rows", 0
    return "loaded", int(len(loaded))


def _sample_symbols(broker, path_str, n=12):
    """Distinct canonical symbols present in a source file (no symbol filter),
    so the UI can tell the user what is actually available to search."""
    fn = {"fyers": _load_fyers_rows, "nubra": _load_nubra_rows, "xts": _load_xts_rows,
          "gdfl": _load_gdfl_rows, "dhan": _load_dhan_rows}.get(broker)
    if fn is None or not path_str:
        return []
    try:
        df = fn(path_str, "")
    except Exception:
        return []
    if df is None or getattr(df, "empty", True) or "canonical_symbol" not in df.columns:
        return []
    return sorted(df["canonical_symbol"].dropna().astype(str).unique().tolist())[:n]


def _source_report(broker, path_str, loaded):
    status, rows = _classify_source(path_str, loaded)
    report = {"broker": broker, "path": (str(path_str).strip() or None) if path_str else None,
              "status": status, "rows": rows}
    if status == "no matching rows":
        report["available"] = _sample_symbols(broker, path_str)
    return report


# def _receive_sources(fyers_csv, nubra_parquet, xts_csv, symbol):
def _receive_sources(fyers_csv, dhan_csv, nubra_csv, xts_csv, symbol):
    """Load the three receive-timing sources, returning (concat_df, per_source_reports).

    A blank or missing path is simply not loaded, so it cannot leak into the result.
    """
    loaded = {
        "fyers": _load_fyers_rows(fyers_csv, symbol),
        "dhan": _load_dhan_rows(dhan_csv, symbol),
        "nubra": _load_nubra_rows(nubra_csv, symbol),
        # "nubra": _load_nubra_rows(nubra_parquet, symbol),
        "xts": _load_xts_rows(xts_csv, symbol),
    }
    paths = {"fyers": fyers_csv, "dhan": dhan_csv, "nubra": nubra_csv, "xts": xts_csv}
    # paths = {"fyers": fyers_csv, "nubra": nubra_parquet, "xts": xts_csv}
    sources = [_source_report(b, paths[b], loaded[b]) for b in ("fyers", "dhan", "nubra", "xts")]
    frames = [f for f in loaded.values() if f is not None and not f.empty]
    if not frames:
        return pd.DataFrame(), sources
    df = pd.concat(frames, ignore_index=True)
    return df, sources


def _fmt_ms_ts(value):
    if value is None or pd.isna(value):
        return None
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        ts = ts.tz_localize("Asia/Kolkata")
    else:
        ts = ts.tz_convert("Asia/Kolkata")
    return ts.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + " " + (ts.strftime("%Z") or "IST")


def _receive_detail_rows(df: pd.DataFrame, limit: int):
    """Per-instrument listed(exchange)-vs-received timing rows, worst delay first."""
    if df.empty:
        return []
    out = df.copy()
    out["_delay"] = pd.to_numeric(out.get("receive_delay_ms"), errors="coerce")
    have_delay = out.dropna(subset=["_delay"])
    ordered = (have_delay if not have_delay.empty else out)
    ordered = _balanced_group_head(
        ordered,
        group_col="broker",
        limit=limit,
        sort_cols=["_delay", "timestamp_ist"],
        ascending=[False, False],
    )
    rows = []
    for _, r in ordered.iterrows():
        rows.append({
            "broker": r.get("broker"),
            "symbol": r.get("canonical_symbol") or r.get("native_symbol"),
            "listed_at_ist": _fmt_ms_ts(r.get("listed_at_ist")),
            "received_at_ist": _fmt_ms_ts(r.get("received_at_ist")),
            "delay_ms": _round_or_none(r.get("_delay"), 3),
            "ltp": _round_or_none(r.get("ltp"), 4),
        })
    return rows


def _balanced_group_head(df: pd.DataFrame, group_col: str, limit: int, sort_cols: list[str], ascending):
    if df.empty:
        return df
    target = max(1, min(int(limit or 300), 5000))
    groups = [g.copy() for _, g in df.groupby(group_col, sort=False)]
    if len(groups) <= 1:
        return df.sort_values(sort_cols, ascending=ascending).head(target)

    per_group = max(1, math.ceil(target / len(groups)))
    pieces = []
    for group in groups:
        pieces.append(group.sort_values(sort_cols, ascending=ascending).head(per_group))
    out = pd.concat(pieces, ignore_index=True)
    out = out.sort_values(sort_cols, ascending=ascending).head(target)
    return out


def _second_last_timing(df: pd.DataFrame, broker: str):
    out = df[df["broker"] == broker].dropna(subset=["ltp"]).copy()
    if out.empty:
        return pd.DataFrame(columns=["sec", "ltp", "listed_at_ist", "received_at_ist", "delay_ms"])
    for col in ("timestamp_ist", "listed_at_ist", "received_at_ist"):
        if col in out.columns:
            out[col] = _to_ist_series(out[col])
    out = out.sort_values(["sec", "timestamp_ist"])
    return out.groupby("sec", as_index=False).agg(
        ltp=("ltp", "last"),
        listed_at_ist=("listed_at_ist", "last"),
        received_at_ist=("received_at_ist", "last"),
        delay_ms=("receive_delay_ms", "last"),
    )


def _load_pair_source(broker: str, path_str: str | None, symbol: str):
    broker_key = str(broker or "").strip().lower()
    loaders = {
        "fyers": _load_fyers_rows,
        "dhan": _load_dhan_rows,
        "nubra": _load_nubra_rows,
    }
    loader = loaders.get(broker_key)
    if loader is None:
        return pd.DataFrame()
    return loader(path_str, symbol)


def _paired_compare_rows(
    left_broker: str,
    left_csv: str | None,
    right_broker: str,
    right_csv: str | None,
    symbol: str,
    from_ts: str | None,
    to_ts: str | None,
    limit: int,
):
    left_key = str(left_broker or "").strip().lower()
    right_key = str(right_broker or "").strip().lower()
    loaded = {
        left_key: _load_pair_source(left_key, left_csv, symbol),
        right_key: _load_pair_source(right_key, right_csv, symbol),
    }
    paths = {left_key: left_csv, right_key: right_csv}
    sources = [_source_report(broker, paths[broker], loaded[broker]) for broker in (left_key, right_key)]
    frames = [f for f in loaded.values() if f is not None and not f.empty]
    if not frames:
        return pd.DataFrame(), sources
    df = pd.concat(frames, ignore_index=True)
    df = _apply_range(df, from_ts, to_ts)
    if df.empty:
        return pd.DataFrame(), sources
    df["sec"] = df["timestamp_ist"].dt.floor("1s")
    left = _second_last_timing(df, left_key).rename(columns={
        "ltp": "left_ltp",
        "listed_at_ist": "left_listed_at_ist",
        "received_at_ist": "left_received_at_ist",
        "delay_ms": "left_delay_ms",
    })
    right = _second_last_timing(df, right_key).rename(columns={
        "ltp": "right_ltp",
        "listed_at_ist": "right_listed_at_ist",
        "received_at_ist": "right_received_at_ist",
        "delay_ms": "right_delay_ms",
    })
    paired = left.merge(right, on="sec", how="outer").sort_values("sec", ascending=False)
    if paired.empty:
        return paired, sources

    def _presence(row):
        has_left = pd.notna(row.get("left_ltp"))
        has_right = pd.notna(row.get("right_ltp"))
        if has_left and has_right:
            return "both"
        if has_left:
            return f"{left_key}_only"
        if has_right:
            return f"{right_key}_only"
        return "none"

    paired["presence"] = paired.apply(_presence, axis=1)
    paired["ltp_diff"] = pd.to_numeric(paired.get("right_ltp"), errors="coerce") - pd.to_numeric(paired.get("left_ltp"), errors="coerce")
    paired["ltp_abs_diff"] = paired["ltp_diff"].abs()
    for col in ("left_listed_at_ist", "right_listed_at_ist", "left_received_at_ist", "right_received_at_ist"):
        if col in paired.columns:
            paired[col] = _to_ist_series(paired[col])
    paired["listed_gap_ms"] = (paired["right_listed_at_ist"] - paired["left_listed_at_ist"]).dt.total_seconds().abs() * 1000.0
    paired["received_gap_ms"] = (paired["right_received_at_ist"] - paired["left_received_at_ist"]).dt.total_seconds().abs() * 1000.0
    paired["delay_gap_ms"] = (pd.to_numeric(paired["right_delay_ms"], errors="coerce") - pd.to_numeric(paired["left_delay_ms"], errors="coerce")).abs()
    return paired, sources


def _pairwise_receive_compare(df: pd.DataFrame, left: str, right: str):
    a = _second_last_timing(df, left).rename(columns={
        "ltp": "ltp_left",
        "listed_at_ist": "listed_left",
        "received_at_ist": "received_left",
        "delay_ms": "delay_left",
    })
    b = _second_last_timing(df, right).rename(columns={
        "ltp": "ltp_right",
        "listed_at_ist": "listed_right",
        "received_at_ist": "received_right",
        "delay_ms": "delay_right",
    })
    aligned = a.merge(b, on="sec", how="inner").sort_values("sec")
    if aligned.empty:
        return None
    aligned["price_abs_diff"] = (pd.to_numeric(aligned["ltp_left"], errors="coerce") - pd.to_numeric(aligned["ltp_right"], errors="coerce")).abs()
    aligned["listed_gap_ms"] = (aligned["listed_left"] - aligned["listed_right"]).dt.total_seconds().abs() * 1000.0
    aligned["received_gap_ms"] = (aligned["received_left"] - aligned["received_right"]).dt.total_seconds().abs() * 1000.0
    aligned["delay_gap_ms"] = (pd.to_numeric(aligned["delay_left"], errors="coerce") - pd.to_numeric(aligned["delay_right"], errors="coerce")).abs()
    top = aligned.sort_values(["received_gap_ms", "sec"], ascending=[False, True]).head(25).copy()
    top["sec"] = top["sec"].map(_fmt_scalar_ts)
    top_rows = top[[
        "sec",
        "ltp_left",
        "ltp_right",
        "price_abs_diff",
        "listed_gap_ms",
        "received_gap_ms",
        "delay_gap_ms",
    ]].to_dict(orient="records")
    return {
        "pair": f"{left} vs {right}",
        "left": left,
        "right": right,
        "overlap_seconds": int(len(aligned)),
        "median_price_abs_diff": _round_or_none(aligned["price_abs_diff"].median(), 6),
        "median_listed_gap_ms": _round_or_none(aligned["listed_gap_ms"].median(), 3),
        "median_received_gap_ms": _round_or_none(aligned["received_gap_ms"].median(), 3),
        "p90_received_gap_ms": _round_or_none(aligned["received_gap_ms"].quantile(0.90), 3),
        "median_delay_gap_ms": _round_or_none(aligned["delay_gap_ms"].median(), 3),
        "top_rows": top_rows,
    }


def _generic_second_compare(df: pd.DataFrame, ref_broker: str, cmp_broker: str):
    ref_df = _second_last_ltp(df, ref_broker).rename(columns={"ltp": "ltp_ref", "ticks": "ticks_ref"})
    cmp_df = _second_last_ltp(df, cmp_broker).rename(columns={"ltp": "ltp_cmp", "ticks": "ticks_cmp"})
    aligned = ref_df.merge(cmp_df, on="sec", how="outer").sort_values("sec")
    if aligned.empty:
        return None

    ref_present = aligned["ltp_ref"].notna()
    cmp_present = aligned["ltp_cmp"].notna()
    both_present = ref_present & cmp_present
    both = aligned.loc[both_present].copy()
    if not both.empty:
        both["diff"] = both["ltp_cmp"] - both["ltp_ref"]
        both["abs_diff"] = both["diff"].abs()
        both["is_same"] = both["diff"].round(6).eq(0)
        median_ref = both["ltp_ref"].abs().median()
        median_abs_pct = (both["abs_diff"].median() / median_ref) if median_ref and not pd.isna(median_ref) else None
    else:
        both = pd.DataFrame(columns=list(aligned.columns) + ["diff", "abs_diff", "is_same"])
        median_abs_pct = None

    reference_seconds = int(ref_present.sum())
    overlap_seconds = int(both_present.sum())
    matching_seconds = int(both["is_same"].sum()) if not both.empty else 0
    coverage_ratio = overlap_seconds / reference_seconds if reference_seconds else 0.0
    match_rate = matching_seconds / overlap_seconds if overlap_seconds else 0.0
    score_components = _score_broker_components(coverage_ratio, match_rate, median_abs_pct)

    top = []
    if not both.empty:
        top_df = both.sort_values(["abs_diff", "sec"], ascending=[False, True]).head(40).copy()
        top_df["sec"] = top_df["sec"].map(_fmt_scalar_ts)
        top_df["candidate"] = cmp_broker
        top = top_df[["candidate", "sec", "ltp_ref", "ltp_cmp", "diff", "abs_diff", "ticks_ref", "ticks_cmp"]].to_dict(orient="records")

    timeline = []
    if not both.empty:
        sample = both.copy()
        if len(sample) > 180:
            step = max(1, len(sample) // 180)
            sample = sample.iloc[::step].copy()
        sample["sec"] = sample["sec"].map(_fmt_scalar_ts)
        timeline = sample[["sec", "diff", "abs_diff"]].to_dict(orient="records")

    return {
        "reference": ref_broker,
        "candidate": cmp_broker,
        "reference_seconds": reference_seconds,
        "candidate_seconds": int(cmp_present.sum()),
        "overlap_seconds": overlap_seconds,
        "reference_only_seconds": int((ref_present & ~cmp_present).sum()),
        "candidate_only_seconds": int((cmp_present & ~ref_present).sum()),
        "matching_seconds": matching_seconds,
        "non_matching_seconds": int((~both["is_same"]).sum()) if not both.empty else 0,
        "coverage_ratio": _round_or_none(coverage_ratio, 6),
        "match_rate": _round_or_none(match_rate, 6),
        "median_diff": _round_or_none(both["diff"].median() if not both.empty else None, 6),
        "mean_diff": _round_or_none(both["diff"].mean() if not both.empty else None, 6),
        "median_abs_diff": _round_or_none(both["abs_diff"].median() if not both.empty else None, 6),
        "max_abs_diff": _round_or_none(both["abs_diff"].max() if not both.empty else None, 6),
        "median_abs_pct": _round_or_none(median_abs_pct, 8),
        "score_components": score_components,
        "score": _score_broker_candidate(coverage_ratio, match_rate, median_abs_pct),
        "distribution": _build_histogram(both["diff"] if "diff" in both else pd.Series(dtype=float), 21),
        "timeline": timeline,
        "top_rows": top,
    }


# def _build_alphabot_match_frame(alpha_csv: str | None, fyers_csv: str | None, nubra_parquet: str | None, gdfl_parquet: str | None, from_ts: str | None, to_ts: str | None, broker_filter: str | None, tolerance_seconds: int):
def _build_alphabot_match_frame(alpha_csv: str | None, fyers_csv: str | None, dhan_csv: str | None, nubra_csv: str | None, gdfl_parquet: str | None, from_ts: str | None, to_ts: str | None, broker_filter: str | None, tolerance_seconds: int):
    trades = _load_alphabot_rows(alpha_csv, from_ts, to_ts, broker_filter)
    if trades.empty:
        return trades
    symbols = sorted(set(trades["canonical_symbol"].dropna().astype(str)))
    fyers = _load_fyers_rows(fyers_csv, "").copy() if fyers_csv else pd.DataFrame()
    dhan = _load_dhan_rows(dhan_csv, "").copy() if dhan_csv else pd.DataFrame()
    nubra = _load_nubra_rows(nubra_csv, "").copy() if nubra_csv else pd.DataFrame()
    # nubra = _load_nubra_rows(nubra_parquet, "").copy() if nubra_parquet else pd.DataFrame()
    gdfl = _load_gdfl_rows(gdfl_parquet, "").copy() if gdfl_parquet else pd.DataFrame()
    if not fyers.empty:
        fyers = fyers[fyers["canonical_symbol"].isin(symbols)].copy()
    if not dhan.empty:
        dhan = dhan[dhan["canonical_symbol"].isin(symbols)].copy()
    if not nubra.empty:
        nubra = nubra[nubra["canonical_symbol"].isin(symbols)].copy()
    if not gdfl.empty:
        gdfl = gdfl[gdfl["canonical_symbol"].isin(symbols)].copy()

    out = trades.copy()
    for broker, frame in (("fyers", fyers), ("dhan", dhan), ("nubra", nubra), ("gdfl", gdfl)):
        for when_col in ("entry_at", "exit_at"):
            match = _nearest_market_match(out, frame, when_col, broker, tolerance_seconds)
            out = out.merge(match, on="trade_id", how="left")
    for broker in ("fyers", "dhan", "nubra", "gdfl"):
        for side in ("entry", "exit"):
            price_col = f"{side}_price"
            ltp_col = f"{broker}_{side}_at_ltp"
            diff_col = f"{broker}_{side}_diff"
            abs_col = f"{broker}_{side}_abs_diff"
            out[diff_col] = pd.to_numeric(out[ltp_col], errors="coerce") - pd.to_numeric(out[price_col], errors="coerce")
            out[abs_col] = out[diff_col].abs()
    return out


def _alphabot_candidate_summary(df: pd.DataFrame, broker: str):
    price_series = pd.concat([df["entry_price"], df["exit_price"]], ignore_index=True)
    diff_series = pd.concat([df[f"{broker}_entry_diff"], df[f"{broker}_exit_diff"]], ignore_index=True)
    abs_diff_series = pd.concat([df[f"{broker}_entry_abs_diff"], df[f"{broker}_exit_abs_diff"]], ignore_index=True)
    delta_series = pd.concat([df.get(f"{broker}_entry_at_delta_sec"), df.get(f"{broker}_exit_at_delta_sec")], ignore_index=True)
    matched_events = int(diff_series.notna().sum())
    total_events = int(len(df) * 2)
    coverage_ratio = matched_events / total_events if total_events else 0.0
    median_price = price_series.dropna().abs().median()
    median_abs_diff = abs_diff_series.dropna().median() if abs_diff_series.notna().any() else None
    median_abs_pct = (median_abs_diff / median_price) if median_price and median_abs_diff is not None else None
    avg_time_delta = delta_series.dropna().mean() if delta_series.notna().any() else None
    score_components = _score_alphabot_components(coverage_ratio, median_abs_pct, avg_time_delta)

    return {
        "candidate": broker,
        "matched_events": matched_events,
        "total_events": total_events,
        "coverage_ratio": _round_or_none(coverage_ratio, 6),
        "mean_diff": _round_or_none(diff_series.dropna().mean() if diff_series.notna().any() else None, 6),
        "median_diff": _round_or_none(diff_series.dropna().median() if diff_series.notna().any() else None, 6),
        "mean_abs_diff": _round_or_none(abs_diff_series.dropna().mean() if abs_diff_series.notna().any() else None, 6),
        "median_abs_diff": _round_or_none(median_abs_diff, 6),
        "max_abs_diff": _round_or_none(abs_diff_series.dropna().max() if abs_diff_series.notna().any() else None, 6),
        "median_abs_pct": _round_or_none(median_abs_pct, 8),
        "avg_time_delta_sec": _round_or_none(avg_time_delta, 3),
        "score_components": score_components,
        "score": _score_alphabot_candidate(coverage_ratio, median_abs_pct, avg_time_delta),
        "distribution": _build_histogram(diff_series, 21),
    }


def _fmt_scalar_ts(value):
    if value is None or pd.isna(value):
        return None
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        ts = ts.tz_localize("Asia/Kolkata")
    else:
        ts = ts.tz_convert("Asia/Kolkata")
    return ts.strftime("%Y-%m-%d %H:%M:%S %Z")


def _excel_cell(value):
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except TypeError:
        pass
    if isinstance(value, pd.Timestamp):
        return _fmt_ms_ts(value)
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return value
    return value


def _sanitize_filename_part(value: str | None, fallback: str):
    text = str(value or "").strip()
    if not text:
        text = fallback
    text = re.sub(r"[^A-Za-z0-9._-]+", "_", text).strip("._")
    return text or fallback


def _build_export_filename(section: str, symbol: str | None):
    section_part = _sanitize_filename_part(section, "export")
    symbol_part = _sanitize_filename_part(symbol, "rows")
    stamp = pd.Timestamp.now(tz="Asia/Kolkata").strftime("%Y%m%d_%H%M%S")
    return f"{section_part}_{symbol_part}_{stamp}.xlsx"


def _export_rows_frame(columns: list[ExportColumn], rows: list[dict]):
    ordered_keys = [c.key for c in columns if str(c.key).strip()]
    df = pd.DataFrame(rows or [])
    if ordered_keys:
        for key in ordered_keys:
            if key not in df.columns:
                df[key] = None
        df = df[ordered_keys]
    rename_map = {c.key: c.label for c in columns if c.label and c.label != c.key}
    if rename_map:
        df = df.rename(columns=rename_map)
    return df


def _xlsx_response(df: pd.DataFrame, filename: str, sheet_name: str = "data"):
    out = df.copy()
    if not out.empty:
        out = out.apply(lambda col: col.map(_excel_cell))
    else:
        for col in out.columns:
            out[col] = out[col].astype(object)
    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        out.to_excel(writer, index=False, sheet_name=(sheet_name[:31] or "data"))
    bio.seek(0)
    headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
    return StreamingResponse(
        bio,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers=headers,
    )


@app.post("/api/analyze/captures")
def analyze_captures(r: AnalyzeDirReq):
    d = _captures_dir(r.date)
    if not d.exists():
        return {"ok": False, "error": f"no such capture dir: {d}"}
    args = [str(d), "--ref", r.ref]
    if r.from_ts:
        args += ["--from-ts", r.from_ts]
    if r.to_ts:
        args += ["--to-ts", r.to_ts]
    res = _run_analyze(args)
    return {"ok": res["ok"], "report": res["report"], "error": None if res["ok"] else "analyze failed"}


@app.post("/api/analyze/files")
def analyze_files(r: AnalyzeFilesReq):
    # args = ["--nubra-parquet", r.nubra_parquet, "--symbol", r.symbol, "--ref", r.ref]
    args = ["--symbol", r.symbol, "--ref", r.ref]
    if r.fyers_csv:
        args += ["--fyers-csv", r.fyers_csv]
    if r.nubra_csv:
        args += ["--nubra-csv", r.nubra_csv]
    if r.dhan_csv:
        args += ["--dhan-csv", r.dhan_csv]
    if r.from_ts:
        args += ["--from-ts", r.from_ts]
    if r.to_ts:
        args += ["--to-ts", r.to_ts]
    res = _run_analyze(args)
    return {"ok": res["ok"], "report": res["report"], "error": None if res["ok"] else "analyze failed"}


@app.post("/api/report/second-compare")
def second_compare(r: SecondCompareReq):
    if not r.nubra_csv or not r.fyers_csv:
        return {"ok": False, "error": "both FYERS CSV and Nubra CSV are required"}
        # return {"ok": False, "error": "both FYERS CSV and Nubra parquet are required"}

    # df = _load_second_compare_rows(r.fyers_csv, r.nubra_parquet, r.symbol, r.from_ts, r.to_ts)
    df = _load_second_compare_rows(r.fyers_csv, r.nubra_csv, r.symbol, r.from_ts, r.to_ts)
    if df.empty:
        return {"ok": True, "summary": {}, "distribution": [], "top_differences": [], "error": None}

    nubra = _second_last_ltp(df, "nubra").rename(columns={
        "ltp": "ltp_nubra",
        "timestamp_ist": "timestamp_nubra",
        "ticks": "ticks_nubra",
    })
    fyers = _second_last_ltp(df, "fyers").rename(columns={
        "ltp": "ltp_fyers",
        "timestamp_ist": "timestamp_fyers",
        "ticks": "ticks_fyers",
    })

    aligned = nubra.merge(fyers, on="sec", how="outer").sort_values("sec")
    if aligned.empty:
        return {"ok": True, "summary": {}, "distribution": [], "top_differences": [], "error": None}

    nubra_present = aligned["ltp_nubra"].notna()
    fyers_present = aligned["ltp_fyers"].notna()
    both_present = nubra_present & fyers_present

    both = aligned.loc[both_present].copy()
    if not both.empty:
        both["diff"] = both["ltp_nubra"] - both["ltp_fyers"]
        both["abs_diff"] = both["diff"].abs()
        both["is_same"] = both["diff"].round(6).eq(0)
    else:
        both = pd.DataFrame(columns=list(aligned.columns) + ["diff", "abs_diff", "is_same"])

    summary = {
        "symbol": r.symbol,
        "from_ts": _fmt_scalar_ts(df["timestamp_ist"].min()),
        "to_ts": _fmt_scalar_ts(df["timestamp_ist"].max()),
        "seconds_in_union": int(len(aligned)),
        "nubra_present_seconds": int(nubra_present.sum()),
        "fyers_present_seconds": int(fyers_present.sum()),
        "both_present_seconds": int(both_present.sum()),
        "nubra_only_seconds": int((nubra_present & ~fyers_present).sum()),
        "fyers_only_seconds": int((fyers_present & ~nubra_present).sum()),
        "matching_seconds": int(both["is_same"].sum()) if not both.empty else 0,
        "non_matching_seconds": int((~both["is_same"]).sum()) if not both.empty else 0,
        "nubra_rows": int((df["broker"] == "nubra").sum()),
        "fyers_rows": int((df["broker"] == "fyers").sum()),
        "nubra_active_seconds": int(df.loc[df["broker"] == "nubra", "sec"].nunique()),
        "fyers_active_seconds": int(df.loc[df["broker"] == "fyers", "sec"].nunique()),
    }

    diff_stats = None
    if not both.empty:
        diff_stats = {
            "mean_diff": round(float(both["diff"].mean()), 6),
            "median_diff": round(float(both["diff"].median()), 6),
            "min_diff": round(float(both["diff"].min()), 6),
            "max_diff": round(float(both["diff"].max()), 6),
            "mean_abs_diff": round(float(both["abs_diff"].mean()), 6),
            "median_abs_diff": round(float(both["abs_diff"].median()), 6),
            "min_abs_diff": round(float(both["abs_diff"].min()), 6),
            "max_abs_diff": round(float(both["abs_diff"].max()), 6),
        }

    top = []
    if not both.empty:
        top_df = both.sort_values(["abs_diff", "sec"], ascending=[False, True]).head(25).copy()
        top_df["sec"] = top_df["sec"].map(_fmt_scalar_ts)
        top = top_df[[
            "sec",
            "ltp_nubra",
            "ltp_fyers",
            "diff",
            "abs_diff",
            "ticks_nubra",
            "ticks_fyers",
        ]].to_dict(orient="records")

    return {
        "ok": True,
        "summary": summary,
        "diff_stats": diff_stats,
        "distribution": _build_histogram(both["diff"] if "diff" in both else pd.Series(dtype=float), r.bins),
        "top_differences": top,
        "error": None,
    }


@app.post("/api/report/alphabot-compare")
def alphabot_compare(r: AlphaBotCompareReq):
    if not r.alpha_csv:
        return {"ok": False, "error": "AlphaBot CSV is required"}
    if not r.fyers_csv and not r.nubra_csv and not r.gdfl_parquet:
        return {"ok": False, "error": "at least one broker source is required"}

    out = _build_alphabot_match_frame(
        r.alpha_csv,
        r.fyers_csv,
        r.nubra_csv,
        # r.nubra_parquet,
        r.gdfl_parquet,
        r.from_ts,
        r.to_ts,
        r.broker_filter,
        r.tolerance_seconds,
    )
    if out.empty:
        return {"ok": True, "summary": {}, "stats": [], "rows": [], "error": None}

    summary = {
        "trade_count": int(len(out)),
        "unique_symbols": int(out["canonical_symbol"].nunique()),
        "alphabot_brokers": sorted(out["alphabot_broker"].dropna().astype(str).unique().tolist()),
        "from_ts": _fmt_scalar_ts(out["entry_at"].min()),
        "to_ts": _fmt_scalar_ts(out["exit_at"].max()),
        "fyers_entry_matches": int(out["fyers_entry_at_ltp"].notna().sum()) if "fyers_entry_at_ltp" in out else 0,
        "fyers_exit_matches": int(out["fyers_exit_at_ltp"].notna().sum()) if "fyers_exit_at_ltp" in out else 0,
        "nubra_entry_matches": int(out["nubra_entry_at_ltp"].notna().sum()) if "nubra_entry_at_ltp" in out else 0,
        "nubra_exit_matches": int(out["nubra_exit_at_ltp"].notna().sum()) if "nubra_exit_at_ltp" in out else 0,
        "gdfl_entry_matches": int(out["gdfl_entry_at_ltp"].notna().sum()) if "gdfl_entry_at_ltp" in out else 0,
        "gdfl_exit_matches": int(out["gdfl_exit_at_ltp"].notna().sum()) if "gdfl_exit_at_ltp" in out else 0,
        "tolerance_seconds": int(r.tolerance_seconds),
    }

    stats = [
        *_alpha_stats(out, "fyers"),
        *_alpha_stats(out, "nubra"),
        *_alpha_stats(out, "gdfl"),
    ]

    table = out.sort_values("entry_at").copy()
    table["entry_at"] = table["entry_at"].map(_fmt_scalar_ts)
    table["exit_at"] = table["exit_at"].map(_fmt_scalar_ts)
    for col in [
        "fyers_entry_at_time", "fyers_exit_at_time",
        "nubra_entry_at_time", "nubra_exit_at_time",
        "gdfl_entry_at_time", "gdfl_exit_at_time",
    ]:
        if col in table.columns:
            table[col] = table[col].map(_fmt_scalar_ts)
    table = table.astype(object).where(pd.notnull(table), None)
    rows = table.head(max(1, min(int(r.limit or 200), 2000))).to_dict(orient="records")

    return {
        "ok": True,
        "summary": summary,
        "stats": stats,
        "rows": rows,
        "error": None,
    }


@app.post("/api/report/receive-compare")
def receive_compare(r: ReceiveCompareReq):
    df = _build_receive_compare_frame(r.fyers_csv, r.dhan_csv, r.nubra_csv, r.xts_csv, r.symbol, r.from_ts, r.to_ts)
    # df = _build_receive_compare_frame(r.fyers_csv, r.nubra_parquet, r.xts_csv, r.symbol, r.from_ts, r.to_ts)
    if df.empty:
        return {
            "ok": True,
            "overview": {},
            "brokers": [],
            "pairs": [],
            "details": [],
            "error": None,
        }

    brokers = []
    for broker in ("fyers", "dhan", "nubra", "xts"):
        summary = _receive_delay_summary(df, broker)
        if summary is not None:
            brokers.append(summary)

    pairs = []
    loaded_brokers = sorted(df["broker"].dropna().astype(str).unique().tolist())
    for left, right in combinations(loaded_brokers, 2):
        pair = _pairwise_receive_compare(df, left, right)
        if pair is not None:
            pairs.append(pair)

    detail_rows = []
    for pair in pairs:
        for row in pair["top_rows"][:10]:
            detail_rows.append({"pair": pair["pair"], **row})

    overview = {
        "symbol": r.symbol,
        "from_ts": _fmt_scalar_ts(df["timestamp_ist"].min()),
        "to_ts": _fmt_scalar_ts(df["timestamp_ist"].max()),
        "brokers_loaded": sorted(df["broker"].dropna().astype(str).unique().tolist()),
        "rows": int(len(df)),
    }
    return {
        "ok": True,
        "overview": overview,
        "brokers": brokers,
        "pairs": pairs,
        "details": detail_rows,
        "error": None,
    }


@app.post("/api/report/receive-rows")
def receive_rows(r: ReceiveRowsReq):
    """Dedicated timing view: for each instrument, the exchange (listed) timestamp,
    the local receive timestamp, and the gap between them. Blank/missing sources
    are reported explicitly so it is obvious which feeds were actually used."""
    df, sources = _receive_sources(r.fyers_csv, r.dhan_csv, r.nubra_csv, r.xts_csv, r.symbol)
    # df, sources = _receive_sources(r.fyers_csv, r.nubra_parquet, r.xts_csv, r.symbol)
    if not df.empty:
        df = _apply_range(df, r.from_ts, r.to_ts)
    if df.empty:
        return {"ok": True, "overview": {}, "summary": [], "rows": [], "sources": sources, "error": None}
    df = df.copy()
    df["sec"] = df["timestamp_ist"].dt.floor("1s")
    summary = [s for s in (_receive_delay_summary(df, b) for b in ("fyers", "dhan", "nubra", "xts")) if s is not None]
    rows = _receive_detail_rows(df, r.limit)
    overview = {
        "symbol": r.symbol,
        "from_ts": _fmt_scalar_ts(df["timestamp_ist"].min()),
        "to_ts": _fmt_scalar_ts(df["timestamp_ist"].max()),
        "brokers_loaded": sorted(df["broker"].dropna().astype(str).unique().tolist()),
        "rows_total": int(len(df)),
        "rows_shown": len(rows),
    }
    return {"ok": True, "overview": overview, "summary": summary, "rows": rows, "sources": sources, "error": None}


@app.post("/api/browse/paired")
def browse_paired(r: PairedBrowseReq):
    left_broker = str(r.left_broker or "").strip().lower()
    right_broker = str(r.right_broker or "").strip().lower()
    if left_broker not in {"fyers", "dhan", "nubra"} or right_broker not in {"fyers", "dhan", "nubra"}:
        return {"ok": False, "error": "paired compare brokers must be chosen from: fyers, dhan, nubra"}
    if left_broker == right_broker:
        return {"ok": False, "error": "paired compare needs two different brokers"}

    path_map = {
        "fyers": r.fyers_csv,
        "dhan": r.dhan_csv,
        "nubra": r.nubra_csv,
    }
    paired, sources = _paired_compare_rows(
        left_broker,
        path_map.get(left_broker),
        right_broker,
        path_map.get(right_broker),
        r.symbol,
        r.from_ts,
        r.to_ts,
        r.limit,
    )
    if paired.empty:
        return {"ok": True, "overview": {}, "summary": {}, "rows": [], "sources": sources}

    target = max(1, min(int(r.limit or 300), 5000))
    both = paired[paired["presence"] == "both"].sort_values("sec", ascending=False)
    others = paired[paired["presence"] != "both"].sort_values("sec", ascending=False)
    display = pd.concat([both, others], ignore_index=True).head(target)

    summary = {
        "seconds": int(len(paired)),
        "both": int((paired["presence"] == "both").sum()),
        "left_only": int((paired["presence"] == f"{left_broker}_only").sum()),
        "right_only": int((paired["presence"] == f"{right_broker}_only").sum()),
        "median_abs_ltp_diff": _round_or_none(pd.to_numeric(paired["ltp_abs_diff"], errors="coerce").median(), 6),
        "median_received_gap_ms": _round_or_none(pd.to_numeric(paired["received_gap_ms"], errors="coerce").median(), 3),
        "median_delay_gap_ms": _round_or_none(pd.to_numeric(paired["delay_gap_ms"], errors="coerce").median(), 3),
    }
    overview = {
        "symbol": r.symbol,
        "left_broker": left_broker,
        "right_broker": right_broker,
        "from_ts": _fmt_scalar_ts(paired["sec"].min()),
        "to_ts": _fmt_scalar_ts(paired["sec"].max()),
        "rows_shown": int(len(display)),
    }
    out = display.copy()
    out["sec"] = out["sec"].map(_fmt_scalar_ts)
    for col in ["left_listed_at_ist", "left_received_at_ist", "right_listed_at_ist", "right_received_at_ist"]:
        out[col] = out[col].map(_fmt_ms_ts)
    out = out.astype(object).where(pd.notnull(out), None)
    return {
        "ok": True,
        "overview": overview,
        "summary": summary,
        "rows": out.to_dict(orient="records"),
        "sources": sources,
    }


@app.post("/api/report/decision")
def decision_compare(r: DecisionCompareReq):
    ref = str(r.reference or "").strip().lower()
    if ref not in {"fyers", "dhan", "nubra", "gdfl", "alphabot"}:
        return {"ok": False, "error": "reference must be one of: fyers, dhan, nubra, gdfl, alphabot"}
    # input_warnings = _decision_input_warnings(ref, r.symbol, r.from_ts, r.to_ts, r.fyers_csv, r.nubra_parquet, r.gdfl_parquet, r.alpha_csv)
    input_warnings = _decision_input_warnings(ref, r.symbol, r.from_ts, r.to_ts, r.fyers_csv, r.dhan_csv, r.nubra_csv, r.gdfl_parquet, r.alpha_csv)

    if ref in {"fyers", "dhan", "nubra", "gdfl"}:
        df = _build_broker_compare_frame(r.fyers_csv, r.dhan_csv, r.nubra_csv, r.gdfl_parquet, r.symbol, r.from_ts, r.to_ts)
        # df = _build_broker_compare_frame(r.fyers_csv, r.nubra_parquet, r.gdfl_parquet, r.symbol, r.from_ts, r.to_ts)
        sources = [
            _source_stats_from_df(df, "fyers", r.fyers_csv),
            _source_stats_from_df(df, "dhan", r.dhan_csv),
            # _source_stats_from_df(df, "nubra", r.nubra_parquet),
            _source_stats_from_df(df, "nubra", r.nubra_csv),
            _source_stats_from_df(df, "gdfl", r.gdfl_parquet),
        ]
        methodology = _decision_metric_help("broker")
        if df.empty:
            empty_warnings = input_warnings + [{
                "level": "high",
                "title": "No broker rows matched the filters",
                "message": "Common causes are wrong symbol, wrong day, reversed time range, or an outdated file path.",
            }]
            return {
                "ok": True,
                "mode": "broker",
                "overview": {},
                "ranking": [],
                "charts": {},
                "details": [],
                "warnings": empty_warnings,
                "methodology": methodology,
                "sources": sources,
                "verdict": "No result yet because the selected files and filters did not produce comparable rows.",
                "error": None,
            }
        candidates = sorted([b for b in df["broker"].dropna().unique().tolist() if b != ref])
        compares = [c for c in (_generic_second_compare(df, ref, candidate) for candidate in candidates) if c is not None]
        if not compares:
            empty_warnings = input_warnings + [{
                "level": "high",
                "title": "No candidate broker aligned with the reference",
                "message": "At least one broker file loaded, but none overlapped enough with the selected reference and symbol.",
            }]
            return {
                "ok": True,
                "mode": "broker",
                "overview": {},
                "ranking": [],
                "charts": {},
                "details": [],
                "warnings": empty_warnings,
                "methodology": methodology,
                "sources": sources,
                "verdict": "No result yet because no candidate broker aligned with the chosen reference.",
                "error": None,
            }
        compares = sorted(compares, key=lambda x: x.get("score") or 0, reverse=True)
        leader = compares[0]
        overview = {
            "mode": "broker",
            "reference": ref,
            "symbol": r.symbol,
            "from_ts": _fmt_scalar_ts(df["timestamp_ist"].min()),
            "to_ts": _fmt_scalar_ts(df["timestamp_ist"].max()),
            "winner": leader["candidate"] if leader["score"] > 0 else None,
            "best_score": leader["score"],
        }
        ranking = [{
            "candidate": compare["candidate"],
            "score": compare["score"],
            "coverage_pct": _round_or_none((compare["coverage_ratio"] or 0) * 100, 2),
            "match_pct": _round_or_none((compare["match_rate"] or 0) * 100, 2),
            "median_abs_diff": compare["median_abs_diff"],
            "max_abs_diff": compare["max_abs_diff"],
            "score_components": compare["score_components"],
        } for compare in compares]
        charts = {
            "score_bars": [{"label": compare["candidate"], "value": compare["score"]} for compare in compares],
            "component_bars": [
                {
                    "label": f"{leader['candidate']} coverage score",
                    "value": leader["score_components"]["coverage_score"],
                    "display": f"{_round_or_none((leader['coverage_ratio'] or 0) * 100, 2)}%",
                },
                {
                    "label": f"{leader['candidate']} match score",
                    "value": leader["score_components"]["match_score"],
                    "display": f"{_round_or_none((leader['match_rate'] or 0) * 100, 2)}%",
                },
                {
                    "label": f"{leader['candidate']} price closeness score",
                    "value": leader["score_components"]["closeness_score"],
                    "display": f"median abs diff {leader['median_abs_diff']}",
                },
            ],
            "distribution": leader["distribution"],
            "timeline": leader["timeline"],
        }
        detail_rows = []
        for compare in compares:
            detail_rows.extend(compare["top_rows"][:15])
        warnings = input_warnings + _broker_compare_warnings(compares, sources)
        return {
            "ok": True,
            "mode": "broker",
            "overview": overview,
            "ranking": ranking,
            "charts": charts,
            "details": detail_rows,
            "warnings": warnings,
            "methodology": methodology,
            "sources": sources,
            "verdict": _broker_verdict(compares),
            "error": None,
        }

    # out = _build_alphabot_match_frame(
    #     r.alpha_csv,
    #     r.fyers_csv,
    #     r.nubra_parquet,
    #     r.gdfl_parquet,
    #     r.from_ts,
    #     r.to_ts,
    #     r.alphabot_broker,
    #     r.tolerance_seconds,
    # )
    out = _build_alphabot_match_frame(
        r.alpha_csv,
        r.fyers_csv,
        r.dhan_csv,
        r.nubra_csv,
        r.gdfl_parquet,
        r.from_ts,
        r.to_ts,
        r.alphabot_broker,
        r.tolerance_seconds,
    )
    methodology = _decision_metric_help("alphabot")
    sources = [
        {
            "source": "alphabot",
            "path": r.alpha_csv,
            "detected_dates": _extract_date_tags(r.alpha_csv),
            "rows": int(len(out)) if not out.empty else 0,
            "active_seconds": int(len(out) * 2) if not out.empty else 0,
            "from_ts": _fmt_scalar_ts(out["entry_at"].min()) if not out.empty else None,
            "to_ts": _fmt_scalar_ts(out["exit_at"].max()) if not out.empty else None,
        },
        {
            "source": "fyers",
            "path": r.fyers_csv,
            "detected_dates": _extract_date_tags(r.fyers_csv),
            "rows": int(out["fyers_entry_at_ltp"].notna().sum() + out["fyers_exit_at_ltp"].notna().sum()) if not out.empty and "fyers_entry_at_ltp" in out else 0,
            "active_seconds": None,
            "from_ts": None,
            "to_ts": None,
        },
        {
            "source": "dhan",
            "path": r.dhan_csv,
            "detected_dates": _extract_date_tags(r.dhan_csv),
            "rows": int(out["dhan_entry_at_ltp"].notna().sum() + out["dhan_exit_at_ltp"].notna().sum()) if not out.empty and "dhan_entry_at_ltp" in out else 0,
            "active_seconds": None,
            "from_ts": None,
            "to_ts": None,
        },
        {
            "source": "nubra",
            "path": r.nubra_csv,
            "detected_dates": _extract_date_tags(r.nubra_csv),
            "rows": int(out["nubra_entry_at_ltp"].notna().sum() + out["nubra_exit_at_ltp"].notna().sum()) if not out.empty and "nubra_entry_at_ltp" in out else 0,
            "active_seconds": None,
            "from_ts": None,
            "to_ts": None,
        },
        # {
        #     "source": "nubra",
        #     "path": r.nubra_parquet,
        #     "detected_dates": _extract_date_tags(r.nubra_parquet),
        #     "rows": int(out["nubra_entry_at_ltp"].notna().sum() + out["nubra_exit_at_ltp"].notna().sum()) if not out.empty and "nubra_entry_at_ltp" in out else 0,
        #     "active_seconds": None,
        #     "from_ts": None,
        #     "to_ts": None,
        # },
        {
            "source": "gdfl",
            "path": r.gdfl_parquet,
            "detected_dates": _extract_date_tags(r.gdfl_parquet),
            "rows": int(out["gdfl_entry_at_ltp"].notna().sum() + out["gdfl_exit_at_ltp"].notna().sum()) if not out.empty and "gdfl_entry_at_ltp" in out else 0,
            "active_seconds": None,
            "from_ts": None,
            "to_ts": None,
        },
    ]
    if out.empty:
        empty_warnings = input_warnings + [{
            "level": "high",
            "title": "No AlphaBot rows matched the filters",
            "message": "Check the broker filter, date range, and whether the CSV is from the same day as the broker files.",
        }]
        return {
            "ok": True,
            "mode": "alphabot",
            "overview": {},
            "ranking": [],
            "charts": {},
            "details": [],
            "warnings": empty_warnings,
            "methodology": methodology,
            "sources": sources,
            "verdict": "No result yet because no AlphaBot trades survived the selected filters.",
            "error": None,
        }

    candidates = []
    for broker in ("fyers", "dhan", "nubra", "gdfl"):
        if f"{broker}_entry_diff" in out.columns:
            candidates.append(_alphabot_candidate_summary(out, broker))
    candidates = sorted(candidates, key=lambda x: x["score"], reverse=True)
    winner = candidates[0]["candidate"] if candidates else None
    overview = {
        "mode": "alphabot",
        "reference": "alphabot",
        "trade_count": int(len(out)),
        "alphabot_broker": r.alphabot_broker,
        "from_ts": _fmt_scalar_ts(out["entry_at"].min()),
        "to_ts": _fmt_scalar_ts(out["exit_at"].max()),
        "winner": winner,
        "best_score": candidates[0]["score"] if candidates else None,
    }
    ranking = [{
        "candidate": c["candidate"],
        "score": c["score"],
        "coverage_pct": _round_or_none((c["coverage_ratio"] or 0) * 100, 2),
        "median_abs_diff": c["median_abs_diff"],
        "avg_time_delta_sec": c["avg_time_delta_sec"],
        "score_components": c["score_components"],
    } for c in candidates]
    charts = {
        "score_bars": [{"label": c["candidate"], "value": c["score"]} for c in candidates],
        "component_bars": sum([[
            {
                "label": f"{c['candidate']} coverage score",
                "value": c["score_components"]["coverage_score"],
                "display": f"{_round_or_none((c['coverage_ratio'] or 0) * 100, 2)}% matched",
            },
            {
                "label": f"{c['candidate']} price closeness",
                "value": c["score_components"]["closeness_score"],
                "display": f"median abs diff {c['median_abs_diff']}",
            },
            {
                "label": f"{c['candidate']} timing score",
                "value": c["score_components"]["timing_score"],
                "display": f"avg gap {c['avg_time_delta_sec']} sec",
            },
        ] for c in candidates], []),
        "distribution": candidates[0]["distribution"] if candidates else [],
        "timeline": [],
    }
    detail = out.sort_values("entry_at").copy().head(max(1, min(int(r.limit or 200), 500)))
    detail["entry_at"] = detail["entry_at"].map(_fmt_scalar_ts)
    detail["exit_at"] = detail["exit_at"].map(_fmt_scalar_ts)
    for col in ["fyers_entry_at_time", "fyers_exit_at_time", "nubra_entry_at_time", "nubra_exit_at_time", "gdfl_entry_at_time", "gdfl_exit_at_time"]:
        if col in detail.columns:
            detail[col] = detail[col].map(_fmt_scalar_ts)
    detail = detail.astype(object).where(pd.notnull(detail), None)
    warnings = input_warnings + _alphabot_compare_warnings(candidates, r.alpha_csv, r.fyers_csv, r.dhan_csv, r.nubra_csv, r.gdfl_parquet)
    # warnings = input_warnings + _alphabot_compare_warnings(candidates, r.alpha_csv, r.fyers_csv, r.nubra_parquet, r.gdfl_parquet)
    return {
        "ok": True,
        "mode": "alphabot",
        "overview": overview,
        "ranking": ranking,
        "charts": charts,
        "details": detail.to_dict(orient="records"),
        "warnings": warnings,
        "methodology": methodology,
        "sources": sources,
        "verdict": _alphabot_verdict(candidates),
        "error": None,
    }


@app.post("/api/export/xlsx")
def export_rows_xlsx(r: ExportRowsReq):
    df = _export_rows_frame(r.columns, r.rows)
    filename = _build_export_filename(r.section, r.symbol)
    sheet_name = _sanitize_filename_part(r.section, "data")
    return _xlsx_response(df, filename=filename, sheet_name=sheet_name)


@app.post("/api/browse/rows")
def browse_rows(r: BrowseRowsReq):
    loaded = {
        "fyers": _load_fyers_rows(r.fyers_csv, r.symbol),
        "dhan": _load_dhan_rows(r.dhan_csv, r.symbol),
        "nubra": _load_nubra_rows(r.nubra_csv, r.symbol),
        # "nubra": _load_nubra_rows(r.nubra_parquet, r.symbol),
        "gdfl": _load_gdfl_rows(r.gdfl_parquet, r.symbol),
        "xts": _load_xts_rows(r.xts_csv, r.symbol),
    }
    paths = {"fyers": r.fyers_csv, "dhan": r.dhan_csv, "nubra": r.nubra_csv,
             "gdfl": r.gdfl_parquet, "xts": r.xts_csv}
    # paths = {"fyers": r.fyers_csv, "dhan": r.dhan_csv, "nubra": r.nubra_parquet,
    #          "gdfl": r.gdfl_parquet, "xts": r.xts_csv}
    # A blank or missing path is never loaded, so it cannot influence the result.
    sources = [_source_report(b, paths[b], loaded[b])
               for b in ("fyers", "dhan", "nubra", "gdfl", "xts") if paths[b]]
    frames = [f for f in loaded.values() if f is not None and not f.empty]
    if not frames:
        return {"ok": True, "rows": [], "count": 0, "brokers": {}, "sources": sources}
    df = pd.concat(frames, ignore_index=True)
    df = _apply_range(df, r.from_ts, r.to_ts)
    if df.empty:
        return {"ok": True, "rows": [], "count": 0, "brokers": {}, "sources": sources}
    broker_counts = {k: int(v) for k, v in df["broker"].value_counts().to_dict().items()}
    df = _balanced_group_head(
        df,
        group_col="broker",
        limit=r.limit,
        sort_cols=["timestamp_ist", "broker"],
        ascending=[False, True],
    )
    out = df.copy()
    out["timestamp_ist"] = _fmt_ts(out["timestamp_ist"])
    if "listed_at_ist" in out.columns:
        out["listed_at_ist"] = _fmt_ts(out["listed_at_ist"])
    if "received_at_ist" in out.columns:
        out["received_at_ist"] = _fmt_ts(out["received_at_ist"])
    out = out.astype(object).where(pd.notnull(out), None)
    return {"ok": True, "rows": out.to_dict(orient="records"), "count": int(len(out)), "brokers": broker_counts, "sources": sources}


if __name__ == "__main__":
    print("\n  Feed Bench Reports ->  http://127.0.0.1:8777")
    print("  BUILD: 2026-08-03b (nubra-csv normalizer + robust _fmt_ts + no-cache)")
    print("  Sources: FYERS/, Nubra/, Dhan/\n")
    uvicorn.run(app, host="127.0.0.1", port=8777, log_level="warning")

#!/usr/bin/env python3
"""Capture Quantick live market data to raw tick and 1-second CSV files.

Quantick is a market-data-only vendor (no order APIs). Its feed is Zerodha Kite
data re-broadcast: instrument tokens, field names (ttq, oi_day_high,
exchange_timestamp) and the NIFTY 50 token 256265 are all Kite's.

The account is IP-whitelisted to the new-portal server only, so
from this Mac the collector opens an SSH tunnel through `new-portal` and talks
to http://127.0.0.1:<port>. Every feed in this bench is then stamped by the
same clock, which is the only way the receive-time comparison means anything.

The output schema is the one Arrow/Dhan write (same RAW/ROLLUP columns and the
same `canonical_symbol` keys), so Server.py and Quantick/compare_feeds.py rank
Quantick against the other feeds with no special casing.

Universe (default):
  NFO  NIFTY ATM +/- --strikes for --expiries nearest expiries, --fut-months futures, NIFTY 50 index
  plus, unless --no-extras:
  NSE  NIFTY BANK index, RELIANCE/HDFCBANK/INFY/SBIN/TCS
  NFO  BANKNIFTY near future
  BSE  SENSEX index, RELIANCE/HDFCBANK/SBIN
  BFO  SENSEX near future + nearest-expiry options ATM +/- --extra-strikes
  MCX  CRUDEOIL/GOLDM/SILVERM/NATURALGAS near futures + CRUDEOIL options ATM +/- 5
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import signal
import socket
import subprocess
import sys
import threading
import time
from collections import Counter
from pathlib import Path
from typing import Any

from dotenv import dotenv_values

BASE_DIR = Path(__file__).resolve().parent
ROOT_DIR = BASE_DIR.parent
sys.path.insert(0, str(ROOT_DIR / "Arrow"))
import arrow_websocket as arrow  # noqa: E402  shared schema, canonical keys and TickStorage

from qtickclient import QTickClient, QTickError  # noqa: E402

IST = arrow.IST
DEFAULT_BASE_URL = None  # set QUANTICK_BASE_URL in .env
PREFIX = "quantick"

NSE_EQUITIES = ["RELIANCE", "HDFCBANK", "INFY", "SBIN", "TCS"]
BSE_EQUITIES = ["RELIANCE", "HDFCBANK", "SBIN"]
MCX_FUTURES = ["CRUDEOIL", "GOLDM", "SILVERM", "NATURALGAS"]

now_ist = arrow.now_ist
_safe_float = arrow._safe_float
_safe_int = arrow._safe_int


def output_paths(out_dir: Path, day: dt.date) -> dict[str, Path]:
    tag = day.isoformat()
    return {
        "raw_csv": out_dir / f"{PREFIX}_live_ticks_{tag}.csv",
        "raw_duckdb": out_dir / f"{PREFIX}_live_ticks_{tag}.duckdb",
        "agg_csv": out_dir / f"{PREFIX}_live_1s_{tag}.csv",
        "universe_json": out_dir / f"{PREFIX}_universe_{tag}.json",
    }


# ---------------------------------------------------------------- credentials


def load_credentials(cred_dir: Path | None) -> dict[str, str | None]:
    cfg: dict[str, Any] = {}
    for folder in [p for p in (cred_dir, BASE_DIR, ROOT_DIR) if p is not None]:
        env_path = folder / ".env"
        if env_path.exists():
            cfg.update({k: v for k, v in dotenv_values(env_path).items() if v not in (None, "")})

    def pick(name: str, default: str | None = None) -> str | None:
        return arrow._first_non_blank(os.getenv(name), cfg.get(name), default)

    return {
        "base_url": pick("QUANTICK_BASE_URL", DEFAULT_BASE_URL),
        "user": pick("QUANTICK_USER"),
        "password": pick("QUANTICK_PASSWORD"),
        "ssh_host": pick("QUANTICK_SSH_HOST"),
        "tunnel_port": pick("QUANTICK_TUNNEL_PORT", "18080"),
    }


# --------------------------------------------------------------------- tunnel


def _port_open(port: int) -> bool:
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=1.0):
            return True
    except OSError:
        return False


class SshTunnel:
    """`ssh -N -L` through the whitelisted host, restarted if it dies.

    qtickclient reconnects its own websocket with backoff, so a tunnel restart
    shows up as one short gap in the capture rather than a dead session.
    """

    def __init__(self, ssh_host: str, local_port: int, remote_host: str, remote_port: int):
        self.ssh_host = ssh_host
        self.local_port = local_port
        self.remote = f"{remote_host}:{remote_port}"
        self.proc: subprocess.Popen | None = None
        self.stopping = False
        self.restarts = 0
        self.external = False

    def _spawn(self) -> None:
        cmd = [
            "ssh", "-N", "-o", "BatchMode=yes", "-o", "ExitOnForwardFailure=yes",
            "-o", "ServerAliveInterval=10", "-o", "ServerAliveCountMax=3",
            "-L", f"127.0.0.1:{self.local_port}:{self.remote}", self.ssh_host,
        ]
        self.proc = subprocess.Popen(cmd, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
        deadline = time.time() + 15
        while time.time() < deadline:
            if _port_open(self.local_port):
                return
            if self.proc.poll() is not None:
                err = (self.proc.stderr.read() if self.proc.stderr else "").strip()
                raise RuntimeError(f"ssh tunnel via {self.ssh_host} exited: {err or self.proc.returncode}")
            time.sleep(0.2)
        raise RuntimeError(f"ssh tunnel via {self.ssh_host} did not open 127.0.0.1:{self.local_port} in 15s")

    def start(self) -> None:
        if _port_open(self.local_port):
            # Someone (e.g. a manual `ssh -L`) already forwards this port; use it, don't manage it.
            self.external = True
            print(f"  tunnel: reusing existing listener on 127.0.0.1:{self.local_port}", flush=True)
            return
        self._spawn()
        print(f"  tunnel: 127.0.0.1:{self.local_port} -> {self.ssh_host} -> {self.remote}", flush=True)
        threading.Thread(target=self._watchdog, daemon=True).start()

    def _watchdog(self) -> None:
        while not self.stopping:
            time.sleep(3)
            if self.stopping or self.proc is None or self.proc.poll() is None:
                continue
            self.restarts += 1
            print(f"  !! tunnel died (exit {self.proc.returncode}); restart #{self.restarts}", flush=True)
            try:
                self._spawn()
                print("  tunnel restored", flush=True)
            except Exception as exc:
                print(f"  !! tunnel restart failed: {exc}", flush=True)

    def stop(self) -> None:
        self.stopping = True
        if self.proc is not None and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.proc.kill()


# ------------------------------------------------------------------- universe


def live_reference(client: QTickClient, symbol: str, timeout: float = 15.0) -> float | None:
    """LTP from the live stream, on a second session so the main client's stream is untouched.
    Used when the account has no historical entitlement (Quantick switched ours to live-only
    on 2026-10-16)."""
    probe = QTickClient(client.base_url)
    got: dict[str, float] = {}
    done = threading.Event()

    def on_tick(_c: Any, tick: dict[str, Any]) -> None:
        if tick.get("ltp") is not None:
            got["ltp"] = float(tick["ltp"])
            done.set()

    try:
        probe.login(*client._qt_creds)
        probe.stream([symbol], on_tick=on_tick, own_while=True)
        done.wait(timeout)
    finally:
        probe.close(quit_loop=True)
    return got.get("ltp")


def reference_price(client: QTickClient, index_symbol: str, atm_override: float | None = None) -> tuple[float, str]:
    if atm_override:
        return float(atm_override), "manual --atm"
    try:
        return _historical_reference(client, index_symbol)
    except QTickError:
        ltp = live_reference(client, index_symbol)
        if ltp is not None:
            return ltp, f"{index_symbol} live LTP (historical not entitled)"
        saved = saved_reference(index_symbol)
        if saved is None:
            raise
        return saved

def saved_reference(index_symbol: str) -> tuple[float, str] | None:
    """Last session's level from the newest universe JSON (no live tick outside market hours)."""
    for path in sorted(BASE_DIR.glob("quantick_universe_*.json"), reverse=True):
        try:
            for chain in json.loads(path.read_text()).get("summary", {}).get("option_chains", []):
                if chain.get("reference_price"):
                    root_index = {"NIFTY": "NSE:NIFTY 50-IN", "SENSEX": "BSE:SENSEX-IN"}.get(chain.get("root"))
                    if root_index == index_symbol:
                        return float(chain["reference_price"]), f"{index_symbol} saved level from {path.stem[-10:]}"
        except (ValueError, OSError):
            continue
    return None


def _historical_reference(client: QTickClient, index_symbol: str) -> tuple[float, str]:
    today = now_ist().date()
    try:
        data = client.historical(symbol=index_symbol, from_date=today.isoformat(), to_date=today.isoformat(), interval="minute")
        candles = data.get("candles") or []
        if candles:
            return float(candles[-1]["close"]), f"{index_symbol} last minute close"
    except QTickError:
        pass
    data = client.historical(symbol=index_symbol, from_date=(today - dt.timedelta(days=10)).isoformat(), to_date=today.isoformat(), interval="day")
    candles = data.get("candles") or []
    if not candles:
        raise RuntimeError(f"no candles for {index_symbol}; cannot centre the option strikes")
    return float(candles[-1]["close"]), f"{index_symbol} last daily close"


def option_legs(client: QTickClient, root: str, exchange: str, ref_price: float, strikes: int, expiries: int) -> tuple[list[str], dict[str, Any]]:
    info = client.option_symbols(root, exchange=exchange)
    step = float(info.get("strike_step") or 0) or 50.0
    atm = round(ref_price / step) * step
    picked = (info.get("option_expiry") or [])[:max(1, expiries)]
    symbols: list[str] = []
    for expiry in picked:
        resp = client.option_symbols(root, exchange=exchange, expiry=expiry,
                                     min_strike=atm - strikes * step, max_strike=atm + strikes * step)
        for leg in resp.get("legs") or []:
            symbols.extend(s for s in (leg.get("CE"), leg.get("PE")) if s)
    return symbols, {"root": root, "exchange": exchange, "reference_price": ref_price, "atm": atm, "step": step, "expiries": picked}


def resolve_universe(client: QTickClient, args: argparse.Namespace) -> tuple[list[str], dict[str, str], dict[str, Any]]:
    """Return (symbols, root_by_symbol, summary). The root is what canonical keys are built from."""
    symbols: list[str] = []
    root_by_symbol: dict[str, str] = {}
    summary: dict[str, Any] = {"option_chains": [], "skipped": []}

    def add(sym: str, root: str) -> None:
        if sym not in root_by_symbol:
            symbols.append(sym)
            root_by_symbol[sym] = root

    def add_chain(root: str, exchange: str, index_symbol: str | None, future_symbol: str, strikes: int, expiries: int, atm: float | None = None) -> None:
        try:
            ref, source = reference_price(client, index_symbol or future_symbol, atm)
            legs, chain = option_legs(client, root, exchange, ref, strikes, expiries)
        except QTickError as exc:
            summary["skipped"].append(f"{exchange} {root} options: {type(exc).__name__}: {str(exc)[:160]}")
            return
        chain["reference_source"] = source
        chain["legs"] = len(legs)
        summary["option_chains"].append(chain)
        for sym in legs:
            add(sym, root)

    # NIFTY first: it is the only underlying every other collector in the bench captures.
    add("NSE:NIFTY 50-IN", "NIFTY")
    for series in ("I", "II", "III")[:max(0, min(3, args.fut_months))]:
        add(f"NFO:NIFTY-{series}", "NIFTY")
    add_chain("NIFTY", "NFO", "NSE:NIFTY 50-IN", "NFO:NIFTY-I", args.strikes, args.expiries, args.atm)

    if not args.no_extras:
        add("NSE:NIFTY BANK-IN", "BANKNIFTY")
        add("NFO:BANKNIFTY-I", "BANKNIFTY")
        for name in NSE_EQUITIES:
            add(f"NSE:{name}-EQ", name)
        add("BSE:SENSEX-IN", "SENSEX")
        add("BFO:SENSEX-I", "SENSEX")
        for name in BSE_EQUITIES:
            add(f"BSE:{name}-EQ", name)
        add_chain("SENSEX", "BFO", "BSE:SENSEX-IN", "BFO:SENSEX-I", args.extra_strikes, 1)
        for name in MCX_FUTURES:
            add(f"MCX:{name}-I", name)
        add_chain("CRUDEOIL", "MCX", None, "MCX:CRUDEOIL-I", 5, 1)

    summary["instrument_count"] = len(symbols)
    return symbols, root_by_symbol, summary


# ------------------------------------------------------------------ normalize


def _parse_ist(value: Any) -> dt.datetime | None:
    if not value:
        return None
    try:
        parsed = dt.datetime.fromisoformat(str(value).replace(" ", "T"))
    except ValueError:
        return None
    return parsed.replace(tzinfo=IST) if parsed.tzinfo is None else parsed.astimezone(IST)


def canonical_for(symbol: str, meta: dict[str, Any], root: str) -> str:
    """Same keys the Arrow/Dhan collectors write for NIFTY F&O; exchange-qualified for the rest."""
    itype = str(meta.get("instrument_type") or "").upper()
    expiry = _parse_ist(meta.get("expiry"))
    exchange = str(meta.get("exchange") or (symbol.split(":", 1)[0] if ":" in symbol else "")).upper()
    tsym = str(meta.get("tradingsymbol") or symbol).upper()
    if symbol.endswith("-IN"):
        return f"INDEX:{tsym}"
    if itype in {"CE", "PE"} and expiry:
        return arrow.canonical_symbol(root, expiry.date(), meta.get("strike"), itype, tsym)
    if itype == "FUT" and expiry:
        return arrow._canon_future(root, expiry.date())
    if itype == "EQ":
        return f"{exchange}:{tsym}-EQ"
    return tsym


def normalize_tick(tick: dict[str, Any], received_at: dt.datetime, meta_cache: dict[str, dict[str, Any]],
                   client: QTickClient, root_by_symbol: dict[str, str]) -> dict[str, Any] | None:
    symbol = tick.get("symbol")
    if not symbol:
        return None
    meta = meta_cache.get(symbol)
    if meta is None:
        info = client.instrument_info(symbol) or {}
        root = root_by_symbol.get(symbol) or str(info.get("tradingsymbol") or symbol)
        meta = {
            "token": info.get("instrument_token") or tick.get("instrument_token"),
            "exchange_segment": info.get("segment"),
            "native_symbol": info.get("tradingsymbol") or symbol,
            "canonical_symbol": canonical_for(symbol, info, root),
            "underlying": root,
            "expiry": (str(info.get("expiry"))[:10] if info.get("expiry") else None),
            "strike": arrow._strike_text(info.get("strike")),
            "instrument_type": info.get("instrument_type") or ("INDEX" if symbol.endswith("-IN") else None),
        }
        # Only cache once the subscribe reply's metadata is in; until then the key may be wrong.
        if info:
            meta_cache[symbol] = meta
    # listed = last-traded time, the same meaning as Arrow's ltt and Dhan's LTT_epoch, so the
    # bench's receive-vs-listed delay compares like with like. Quantick's `timestamp` is the LTT
    # (moves only when volume rises); `exchange_timestamp` is packet time and stays in raw_json.
    listed = _parse_ist(tick.get("timestamp")) or _parse_ist(tick.get("exchange_timestamp")) or received_at
    ohlc = tick.get("ohlc") or {}
    return {
        "date": listed.date().isoformat(),
        "datetime": listed.replace(microsecond=0).isoformat(),
        "listed_at_ist": listed.isoformat(),
        "received_at_ist": received_at.isoformat(),
        "token": meta["token"],
        "exchange_segment": meta["exchange_segment"],
        "symbol": symbol,
        "native_symbol": meta["native_symbol"],
        "canonical_symbol": meta["canonical_symbol"],
        "underlying": meta["underlying"],
        "expiry": meta["expiry"],
        "strike": meta["strike"],
        "instrument_type": meta["instrument_type"],
        "ltp": _safe_float(tick.get("ltp")),
        # Server 3.4.0 / client 3.2.0 "slim" ticks renamed these keys; accept both.
        "ltq": _safe_int(tick.get("ltq", tick.get("last_traded_quantity"))),
        "avg_price": _safe_float(tick.get("atp", tick.get("average_traded_price"))),
        "volume": _safe_int(tick.get("ttq")),
        "total_sell_quantity": _safe_int(tick.get("tsq", tick.get("total_sell_quantity"))),
        "total_buy_quantity": _safe_int(tick.get("tbq", tick.get("total_buy_quantity"))),
        "open_interest": _safe_int(tick.get("oi")),
        "oi_day_high": _safe_int(tick.get("oi_day_high")),
        "oi_day_low": _safe_int(tick.get("oi_day_low")),
        "open_price": _safe_float(ohlc.get("open")),
        "high_price": _safe_float(ohlc.get("high")),
        "low_price": _safe_float(ohlc.get("low")),
        # Kite semantics: ohlc.close is the PREVIOUS day's close, not today's last price.
        "close_price": _safe_float(ohlc.get("close")),
        "bid_price": _safe_float(tick.get("bid")),
        "ask_price": _safe_float(tick.get("ask")),
        "bid_qty": _safe_int(tick.get("bid_qty")),
        "ask_qty": _safe_int(tick.get("ask_qty")),
        "bid_orders": None,
        "ask_orders": None,
        "message_type": tick.get("mode", "full"),
        "raw_json": json.dumps(tick, separators=(",", ":"), default=str),
    }


# ----------------------------------------------------------------------- main


def print_universe(symbols: list[str], summary: dict[str, Any]) -> None:
    for chain in summary["option_chains"]:
        print(f"  {chain['exchange']:<4} {chain['root']:<9} ref={chain['reference_price']} ({chain['reference_source']})  "
              f"ATM={chain['atm']} step={chain['step']} expiries={','.join(chain['expiries'])} legs={chain['legs']}", flush=True)
    for line in summary["skipped"]:
        print(f"  skipped: {line}", flush=True)
    by_exch = Counter(s.split(":", 1)[0] if ":" in s else "NFO" for s in symbols)
    print(f"  instruments      : {len(symbols)}  ({', '.join(f'{k} {v}' for k, v in sorted(by_exch.items()))})", flush=True)
    for sym in [s for s in symbols if not s.endswith("-O")][:30]:
        print(f"    - {sym}", flush=True)


def main() -> int:
    ap = argparse.ArgumentParser(description="Capture Quantick live market data to raw tick and 1-second CSV files.")
    ap.add_argument("--strikes", type=int, default=15, help="NIFTY ATM-centered strikes either side")
    ap.add_argument("--expiries", type=int, default=1, help="nearest NIFTY option expiries to subscribe")
    ap.add_argument("--fut-months", type=int, default=3, help="number of NIFTY futures to subscribe (max 3)")
    ap.add_argument("--extra-strikes", type=int, default=10, help="SENSEX ATM-centered strikes either side")
    ap.add_argument("--no-extras", action="store_true", help="NIFTY only (no BANKNIFTY/equities/BSE/BFO/MCX)")
    ap.add_argument("--interval", default="1s", choices=["1s", "1m"], help="aggregation bucket size for saved output")
    ap.add_argument("--out", default=str(BASE_DIR), help="output directory (defaults to Quantick/)")
    ap.add_argument("--cred-dir", default=None, help="folder holding .env for Quantick credentials")
    ap.add_argument("--until", default="15:32", help="stop time in IST HH:MM (NSE/BSE close; MCX keeps trading after)")
    ap.add_argument("--atm", type=float, default=None, help="optional manual NIFTY ATM override")
    ap.add_argument("--no-tunnel", action="store_true", help="connect directly (only works from the whitelisted IP)")
    ap.add_argument("--no-bid-ask", action="store_true", help="skip L1 best bid/ask")
    ap.add_argument("--no-duckdb", action="store_true", help="CSV only")
    ap.add_argument("--dry-run", action="store_true", help="log in, resolve and print the universe, then exit")
    ap.add_argument("--preflight", action="store_true", help="same as --dry-run; used by StartDailyCapture.py")
    args = ap.parse_args()

    out_dir = Path(args.out).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    creds = load_credentials(Path(args.cred_dir).resolve() if args.cred_dir else None)

    print("=" * 72)
    print(f"  QUANTICK live websocket capture  |  {now_ist().strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print(f"  interval={args.interval}  out={out_dir}")
    print("=" * 72)

    if not creds["user"] or not creds["password"]:
        raise RuntimeError("Set QUANTICK_USER and QUANTICK_PASSWORD in .env")

    tunnel = None
    base_url = str(creds["base_url"]).rstrip("/")
    if creds["ssh_host"] and not args.no_tunnel:
        port = int(creds["tunnel_port"] or 18080)
        remote = base_url.split("://", 1)[1].split("/", 1)[0]
        remote_host, _, remote_port = remote.partition(":")
        tunnel = SshTunnel(str(creds["ssh_host"]), port, remote_host, int(remote_port or 80))
        tunnel.start()
        base_url = f"http://127.0.0.1:{port}"

    client = QTickClient(base_url)
    client._qt_creds = (str(creds["user"]), str(creds["password"]))  # for the live-reference probe session
    try:
        client.login(str(creds["user"]), str(creds["password"]))
        print("  login            : ok", flush=True)
        day = now_ist().date()
        if not (args.dry_run or args.preflight):
            # Pick strikes from a live index tick: historical is not entitled, and before
            # 09:15 there is no tick to centre on.
            arrow.wait_for_market_open(day)
            client.login(str(creds["user"]), str(creds["password"]))  # fresh token after the wait
        symbols, root_by_symbol, summary = resolve_universe(client, args)
        print_universe(symbols, summary)
        if args.dry_run or args.preflight:
            print("  Quantick login ok and universe resolved.", flush=True)
            return 0

        outputs = output_paths(out_dir, day)
        storage = arrow.TickStorage(outputs["raw_csv"], outputs["raw_duckdb"], outputs["agg_csv"], args.interval,
                                    duckdb_enabled=not args.no_duckdb)
        meta_cache: dict[str, dict[str, Any]] = {}
        seen_by_segment: Counter = Counter()
        errors: list[str] = []
        state = {"stop": False}

        def on_tick(_client: Any, tick: dict[str, Any]) -> None:
            received_at = now_ist()  # first statement: stamp before any parsing
            row = normalize_tick(tick, received_at, meta_cache, client, root_by_symbol)
            if row is None:
                return
            storage.persist(row)
            seen_by_segment[row["exchange_segment"] or "?"] += 1

        def on_error(_client: Any, err: Any) -> None:
            msg = f"{now_ist().strftime('%H:%M:%S')} {type(err).__name__}: {err}"
            errors.append(msg)
            print(f"  on_error: {msg[:300]}", flush=True)

        client.on_error = on_error

        def shutdown(signum=None, frame=None) -> None:
            state["stop"] = True

        signal.signal(signal.SIGINT, shutdown)
        signal.signal(signal.SIGTERM, shutdown)

        client.stream(symbols, on_tick=on_tick, own_while=True,
                      bid_ask=not args.no_bid_ask, level="L1")
        print(f"  streaming {len(symbols)} symbols -> {outputs['agg_csv'].name}", flush=True)

        stop_h, stop_m = arrow.parse_until(args.until)
        stop_at = dt.datetime.combine(day, dt.time(stop_h, stop_m), tzinfo=IST)
        last_report = time.time()
        reply_written = False
        while not state["stop"] and now_ist() < stop_at:
            time.sleep(1)
            if not reply_written and client.last_subscribe_result is not None:
                reply = client.last_subscribe_result
                raw = getattr(reply, "raw", None) or {}
                print(f"  subscribed={len(raw.get('subscribed') or [])} not_subscribed={raw.get('not_subscribed')} "
                      f"invalid={raw.get('invalid')} unavailable={raw.get('unavailable')}", flush=True)
                reply_written = True
            if time.time() - last_report >= 60:
                last_report = time.time()
                # After a Quantick server restart (2026-10-19 10:54, 502 then reconnect) the server
                # answered "invalid symbol (could not resolve instrument token)" for 45 of 66 contracts
                # until its instrument map reloaded, and the client never asks again. Re-ask each minute.
                invalid = [sym for sym, r in client.rejected_symbols().items()
                           if not r.is_entitlement and sym in root_by_symbol]
                if invalid:
                    print(f"  !! re-subscribing {len(invalid)} symbols the server called invalid: {invalid[:5]}...", flush=True)
                    try:
                        client.subscribe(invalid, bid_ask=not args.no_bid_ask, level="L1")
                    except Exception as exc:
                        print(f"  re-subscribe failed: {exc}", flush=True)
                tun = f" tunnel_restarts={tunnel.restarts}" if tunnel else ""
                print(f"  [{now_ist().strftime('%H:%M:%S')}] ticks={storage.tick_count} "
                      f"{dict(seen_by_segment)}{tun}", flush=True)

        print("  stopping...", flush=True)
        reply = client.last_subscribe_result
        outputs["universe_json"].write_text(json.dumps({
            "date": day.isoformat(),
            "base_url_via_tunnel": bool(tunnel),
            "symbols": symbols,
            "root_by_symbol": root_by_symbol,
            "summary": summary,
            "subscribe_reply": getattr(reply, "raw", None),
            "rejected": {k: str(getattr(v, "reason", v)) for k, v in client.rejected_symbols().items()},
            "canonical_by_symbol": {k: v["canonical_symbol"] for k, v in meta_cache.items()},
            "errors": errors[-200:],
            "tick_count": storage.tick_count,
            "tunnel_restarts": tunnel.restarts if tunnel else None,
        }, indent=2, default=str))
        client.close(quit_loop=True)
        storage.close()
        print(f"  done: {storage.tick_count} ticks -> {outputs['agg_csv']}", flush=True)
        return 0
    finally:
        try:
            client.close(quit_loop=True)
        except Exception:
            pass
        if tunnel is not None:
            tunnel.stop()


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\nInterrupted by user.")
    except Exception as exc:
        print(f"Startup failed: {type(exc).__name__}: {exc}")
        sys.exit(1)

#!/usr/bin/env python3
r"""ALPHABOTS live data subscriber (2026-10-20). ONE file; needs Python 3.9+ with numpy and websockets
(``pip install numpy websockets``). Windows, Linux and macOS.

It connects OUT to the abmd feed (TLS websocket, port 443, one token per subscriber), receives the ALPHABOTS NSE
tick-by-tick feed -- every tick and every 1-minute (optionally 1-second) bar of the day's NSE F&O contracts -- and
prints each one as a JSON line with the instrument resolved from the day's instrument master. Nothing is stored
unless asked (--csv-dir / --out). It reconnects by itself (network break, server restart, new session at 09:00).

    python alphabots_feed.py --token-file feed.token                          ticks + 1-minute bars as JSON lines
    python alphabots_feed.py --token-file feed.token --underlying NIFTY       only NIFTY contracts
    python alphabots_feed.py --token-file feed.token --bars 1m,1s --csv-dir data   also appended to CSV files per day
    python alphabots_feed.py --token-file feed.token --watch                  a live table of prices instead of JSON
    python alphabots_feed.py --token-file feed.token --list                   the day's instruments, then exit

Python API (import this file):

    from alphabots_feed import AlphabotsFeed
    feed = AlphabotsFeed(token_file="feed.token", underlying=["NIFTY"], on_tick=lambda t: print(t["symbol"], t["ltp"]))
    feed.start()                                     # background thread; feed.run() blocks instead
    feed.ltp("NIFTY 2026-10-27 25000 CE")            # latest price of a symbol (or token); feed.latest = {token: tick}
    feed.token("NIFTY", "2026-10-27", 25000, "CE")   # instrument lookups: instruments(), expiries(), symbol()
    feed.stop()

Every tick dict: type, feed, token, symbol, underlying, expiry, strike, right, nse_token, lot_size, ltp, bid, ask,
bid_qty, ask_qty, last_trade_qty, volume (cumulative for the day), oi, open, high, low, prev_close, exch_time,
recv_time, exch_ts_ns, recv_ts_ns. Every bar dict: type, feed, tf, token, symbol, ..., time (bar start, IST), open,
high, low, close, volume (cumulative for the day, like the ticks), oi, stale. Prices are in rupees.
"""
import argparse
import asyncio
import csv
import datetime as dt
import hashlib
import json
import logging
import os
import ssl
import struct
import sys
import threading
import time
import zlib
from pathlib import Path

import numpy as np

log = logging.getLogger("alphabots-feed")

DEFAULT_URL = os.environ.get("ALPHABOTS_FEED_URL", "").strip()  # wss://HOST:443, set in .env
DEFAULT_FEED = "alphabots"
IST = dt.timezone(dt.timedelta(hours=5, minutes=30))

# ---- wire format (copy of abmd/feed/protocol.py) -------------------------------------------------------------------
MAGIC, VERSION = b"AF", 1
KIND_RING, KIND_JOURNAL, KIND_FILE = 1, 2, 3
FLAG_ZLIB = 1
_HDR = struct.Struct("<2sBBHBxQ")
FRAME_HEADER = _HDR.size


def unpack_frame(data):
    magic, version, kind, stream_id, flags, offset = _HDR.unpack_from(data, 0)
    if magic != MAGIC or version != VERSION:
        raise ValueError(f"bad frame {magic!r}/{version}")
    payload = data[FRAME_HEADER:]
    if flags & FLAG_ZLIB:
        payload = zlib.decompress(payload)
    return kind, stream_id, offset, payload


def make_dtype(d):
    return np.dtype({"names": d["names"], "formats": d["formats"], "offsets": d["offsets"], "itemsize": d["itemsize"]})


def rupees(paise):
    return int(paise) / 100.0


def ist(ns):
    return dt.datetime.fromtimestamp(int(ns) / 1e9, IST)


def hms(ns):
    return ist(ns).strftime("%H:%M:%S.%f")[:-3] if ns else ""


def _num(v):
    try:
        return float(v) if v not in (None, "") else 0.0
    except ValueError:
        return 0.0


def _int(v):
    try:
        return int(float(v)) if v not in (None, "") else 0
    except ValueError:
        return 0


def symbol_of(row):
    """'NIFTY' (index), 'NIFTY FUT 2026-10-27', 'NIFTY 2026-10-27 25000 CE'."""
    u, right = row["underlying"], row["right"]
    if right == "SPOT" or not row["expiry"]:
        return u
    if right == "FUT":
        return f"{u} FUT {row['expiry']}"
    return f"{u} {row['expiry']} {row['strike']:g} {right}"


# ---- the day's instruments (master.csv served by the feed) ---------------------------------------------------------
class Instruments:
    def __init__(self, path=None):
        self.rows, self.by_token, self.by_key, self.by_symbol, self.by_nse = [], {}, {}, {}, {}
        if path:
            self.load(path)

    def load(self, path):
        self.rows, self.by_token, self.by_key, self.by_symbol, self.by_nse = [], {}, {}, {}, {}
        with open(path, newline="", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                try:
                    tok = int(r.get("canonical_token") or -1)
                except ValueError:
                    continue
                if tok < 0:
                    continue
                row = {"token": tok, "underlying": (r.get("underlying") or "").strip(), "expiry": (r.get("expiry") or "").strip(),
                       "strike": _num(r.get("strike")), "right": ((r.get("right") or "").strip().upper() or "SPOT"),
                       "lot_size": _int(r.get("lot_size")), "tick_size": _num(r.get("tick_size")),
                       "nse_token": _int(r.get("zerodha_exchange_token"))}
                row["symbol"] = symbol_of(row)
                self.rows.append(row)
                self.by_token[tok] = row
                self.by_key[(row["underlying"], row["expiry"], row["strike"], row["right"])] = tok
                self.by_symbol[row["symbol"]] = tok
                if row["nse_token"]:
                    self.by_nse[row["nse_token"]] = tok
        return len(self.rows)

    def expiries(self, underlying):
        return sorted({r["expiry"] for r in self.rows if r["underlying"] == underlying and r["expiry"] and r["right"] in ("CE", "PE")})

    def token(self, underlying, expiry=None, strike=None, right="SPOT"):
        right = (right or "SPOT").upper()
        if right == "SPOT":
            return self.by_key.get((underlying, "", 0.0, "SPOT"))
        if expiry is None:
            ex = self.expiries(underlying) if right in ("CE", "PE") else sorted({r["expiry"] for r in self.rows if r["underlying"] == underlying and r["right"] == "FUT"})
            expiry = ex[0] if ex else ""
        return self.by_key.get((underlying, str(expiry), float(strike or 0), right))


# ---- the subscriber ------------------------------------------------------------------------------------------------
class AlphabotsFeed:
    def __init__(self, token=None, token_file=None, url=DEFAULT_URL, cafile=None, insecure=False, feed=DEFAULT_FEED,
                 ticks=True, bars=("1m",), underlying=None, right=None, expiry=None, symbols=None, tokens=None,
                 include_stale=False, on_tick=None, on_bar=None, on_session=None, refdata_dir=None, idle_timeout=15.0):
        self.token = (token or "").strip() or (Path(token_file).read_text(encoding="utf-8").strip() if token_file else "") \
            or os.environ.get("FEED_TOKEN", "").strip()
        if not self.token:
            raise ValueError("no token: pass token=, token_file= or set FEED_TOKEN")
        if not url:
            raise ValueError("no feed url: pass url=/--url or set ALPHABOTS_FEED_URL")
        self.url, self.cafile, self.insecure, self.feed = url, cafile, insecure, feed
        self.want_ticks, self.want_bars = bool(ticks), tuple(b for b in (bars or ()) if b in ("1m", "1s"))
        self.f_underlying = {u.strip().upper() for u in (underlying or []) if u.strip()} or None
        self.f_right = {r.strip().upper() for r in (right or []) if r.strip()} or None
        self.f_expiry = {e.strip() for e in (expiry or []) if e.strip()} or None
        self.f_symbols = [s.strip().upper() for s in (symbols or []) if s.strip()] or None
        self.f_tokens = {int(t) for t in (tokens or [])} or None
        self.include_stale = bool(include_stale)
        self.on_tick, self.on_bar, self.on_session = on_tick, on_bar, on_session
        self.refdata_dir = Path(refdata_dir) if refdata_dir else Path(__file__).resolve().parent / "refdata"
        self.idle_timeout = float(idle_timeout)
        self.instruments = Instruments()
        self.latest = {}                 # token -> last tick dict (of this feed, this session)
        self.last_bar = {}               # (token, tf) -> last bar dict
        self.session_date, self.session_open_ns = 0, 0
        self.connected = False
        self.stats = {"ticks": 0, "bars": 0, "frames": 0, "bytes": 0, "newest_recv_ns": 0, "server_dropped": 0, "connects": 0}
        self._rings = {}                 # stream id -> {"name", "tf", "dtype", "n_int"}
        self._seen = {}                  # stream id -> (last seq per slot, last bar_ts per slot)
        self._stop = threading.Event()
        self._thread = self._loop = self._task = None

    # ---- lookups -------------------------------------------------------------------------------------------------
    def token_of(self, symbol_or_token):
        if isinstance(symbol_or_token, int):
            return symbol_or_token
        s = str(symbol_or_token).strip()
        if s.isdigit():
            return int(s)
        return self.instruments.by_symbol.get(s)

    def symbol(self, token):
        r = self.instruments.by_token.get(int(token))
        return r["symbol"] if r else str(token)

    def token(self, underlying, expiry=None, strike=None, right="SPOT"):
        return self.instruments.token(underlying, expiry, strike, right)

    def expiries(self, underlying):
        return self.instruments.expiries(underlying)

    def instruments_list(self, underlying=None, right=None, expiry=None):
        return [r for r in self.instruments.rows if (underlying is None or r["underlying"] == underlying)
                and (right is None or r["right"] == right.upper()) and (expiry is None or r["expiry"] == expiry)]

    def ltp(self, symbol_or_token):
        t = self.latest.get(self.token_of(symbol_or_token))
        return t["ltp"] if t else None

    def tick(self, symbol_or_token):
        return self.latest.get(self.token_of(symbol_or_token))

    def wanted(self, token):
        """Does this instrument pass the filters given at construction?"""
        if self.f_tokens is not None and token not in self.f_tokens:
            return False
        if self.f_underlying is None and self.f_right is None and self.f_expiry is None and self.f_symbols is None:
            return True
        r = self.instruments.by_token.get(token)
        if r is None:
            return self.f_tokens is not None
        if self.f_underlying is not None and r["underlying"].upper() not in self.f_underlying:
            return False
        if self.f_right is not None and r["right"] not in self.f_right:
            return False
        if self.f_expiry is not None and r["expiry"] not in self.f_expiry:
            return False
        if self.f_symbols is not None and not any(s in r["symbol"].upper() for s in self.f_symbols):
            return False
        return True

    # ---- session setup -------------------------------------------------------------------------------------------
    def _on_hello(self, h):
        self.session_date, self.session_open_ns = int(h["session_date"]), int(h["session_open_ns"])
        dtypes = {k: make_dtype(v) for k, v in h["dtypes"].items()}
        want = set()
        if self.want_ticks:
            want.add(f"ab_ticks_{self.feed}")
        for tf in self.want_bars:
            want.add(f"ab_bars_{self.feed}_{tf}")
        self._rings, self._seen = {}, {}
        for info in h["rings"]:
            if info.get("source") != self.feed or info["name"] not in want:
                continue
            n = int(info["n_slots"])
            self._rings[int(info["id"])] = {"name": info["name"], "tf": info["tf"], "dtype": dtypes[info["dtype"]], "n_int": int(info["n_int"])}
            self._seen[int(info["id"])] = (np.zeros(n, dtype=np.uint32), np.zeros(n, dtype=np.int64))
        avail = sorted(r["name"] for r in h["rings"])
        if not self._rings:
            raise RuntimeError(f"feed '{self.feed}' has no rings in this session yet (offered: {avail}); retrying")
        self.latest, self.last_bar = {}, {}
        log.info("session %s%s: receiving %s", self.session_date, f" as client {h['client']}" if h.get("client") else "",
                 ", ".join(sorted(r["name"] for r in self._rings.values())))

    def _master_path(self):
        return self.refdata_dir / str(self.session_date) / "master.csv"

    def _master_missing(self, listed):
        p = self._master_path()
        for f in listed:
            if f["file"] == "master.csv":
                return not (p.exists() and p.stat().st_size == int(f["size"]))
        return False                                         # not served to us: carry on with bare tokens

    def _load_master(self):
        p = self._master_path()
        if p.exists():
            n = self.instruments.load(p)
            log.info("instrument master %s: %d instruments", p, n)
        else:
            log.warning("no instrument master for %s: symbols are shown as tokens", self.session_date)
        if self.on_session:
            try:
                self.on_session(self)
            except Exception as e:                           # noqa: BLE001
                log.warning("on_session callback: %r", e)

    def _subscribe_msg(self):
        return json.dumps({"type": "subscribe", "rings": [r["name"] for r in self._rings.values()], "journals": {}})

    # ---- frames --------------------------------------------------------------------------------------------------
    def _on_ring(self, sid, payload):
        r = self._rings.get(sid)
        if r is None:
            return
        dtype, n_int = r["dtype"], r["n_int"]
        n = len(payload) // (4 + dtype.itemsize)
        if n == 0:
            return
        view = np.frombuffer(payload, dtype=np.dtype([("idx", "<u4"), ("rec", dtype)]), count=n)
        last_seq, last_ts = self._seen[sid]
        now_ns = time.time_ns()
        for k in range(n):
            idx, rec = int(view["idx"][k]), view["rec"][k]
            seq = int(rec["seq"])
            if seq == 0 or idx >= len(last_seq):
                continue
            if r["tf"] == "tick":
                if int(rec["token"]) != idx or int(rec["ltp"]) <= 0 or seq <= int(last_seq[idx]):
                    continue
                last_seq[idx] = seq
                self.stats["newest_recv_ns"] = max(self.stats["newest_recv_ns"], int(rec["recv_ts_ns"]))
                if not self.wanted(idx):
                    continue
                self._emit_tick(idx, rec, now_ns)
            else:
                token, ts = idx // n_int, int(rec["bar_ts_ns"])
                if int(rec["token"]) != token or int(rec["close"]) <= 0:
                    continue
                if ts == int(last_ts[idx]) and seq <= int(last_seq[idx]):
                    continue
                last_seq[idx], last_ts[idx] = seq, ts
                if (int(rec["stale"]) and not self.include_stale) or not self.wanted(token):
                    continue
                self._emit_bar(token, r["tf"], rec, now_ns)

    def _base(self, token):
        row = self.instruments.by_token.get(token)
        if row is None:
            return {"token": token, "symbol": str(token), "underlying": "", "expiry": "", "strike": 0.0, "right": "", "nse_token": 0, "lot_size": 0}
        return {"token": token, "symbol": row["symbol"], "underlying": row["underlying"], "expiry": row["expiry"], "strike": row["strike"],
                "right": row["right"], "nse_token": row["nse_token"], "lot_size": row["lot_size"]}

    def _emit_tick(self, token, s, now_ns):
        d = {"type": "tick", "feed": self.feed, **self._base(token),
             "ltp": rupees(s["ltp"]), "bid": rupees(s["bid"]), "ask": rupees(s["ask"]), "bid_qty": int(s["bid_qty"]), "ask_qty": int(s["ask_qty"]),
             "last_trade_qty": int(s["last_trade_qty"]), "volume": int(s["volume"]), "oi": int(s["oi"]),
             "open": rupees(s["sess_open"]), "high": rupees(s["sess_high"]), "low": rupees(s["sess_low"]), "prev_close": rupees(s["prev_close"]),
             "exch_time": hms(s["exch_ts_ns"]), "recv_time": hms(s["recv_ts_ns"]), "exch_ts_ns": int(s["exch_ts_ns"]), "recv_ts_ns": int(s["recv_ts_ns"])}
        self.latest[token] = d
        self.stats["ticks"] += 1
        if self.on_tick:
            try:
                self.on_tick(d)
            except Exception as e:                           # noqa: BLE001
                log.warning("on_tick callback: %r", e)

    def _emit_bar(self, token, tf, b, now_ns):
        d = {"type": "bar", "feed": self.feed, "tf": tf, **self._base(token),
             "time": ist(b["bar_ts_ns"]).strftime("%Y-%m-%d %H:%M:%S"), "bar_ts_ns": int(b["bar_ts_ns"]),
             "open": rupees(b["open"]), "high": rupees(b["high"]), "low": rupees(b["low"]), "close": rupees(b["close"]),
             "volume": int(b["volume"]), "oi": int(b["oi"]), "last_trade_qty": int(b["last_trade_qty"]), "stale": int(b["stale"])}
        self.last_bar[(token, tf)] = d
        self.stats["bars"] += 1
        if self.on_bar:
            try:
                self.on_bar(d)
            except Exception as e:                           # noqa: BLE001
                log.warning("on_bar callback: %r", e)

    # ---- connection ----------------------------------------------------------------------------------------------
    def _ssl(self):
        if not self.url.startswith("wss://"):
            return None
        if self.insecure:
            ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT); ctx.check_hostname = False; ctx.verify_mode = ssl.CERT_NONE
            return ctx
        if self.cafile:
            return ssl.create_default_context(cafile=str(self.cafile))   # pin: only the feed's own certificate is trusted
        return ssl.create_default_context()

    async def _run_once(self, stats_every):
        from websockets.asyncio.client import connect
        headers = {"Authorization": f"Bearer {self.token}"}
        self._rings, self.connected = {}, False
        file_meta, subscribed, last_stats = None, False, time.monotonic()
        async with connect(self.url, additional_headers=headers, ssl=self._ssl(), max_size=None, compression=None,
                           ping_interval=20, ping_timeout=20, open_timeout=15) as ws:
            self.connected = True
            self.stats["connects"] += 1
            log.info("connected to %s", self.url)
            while not self._stop.is_set():
                try:
                    msg = await asyncio.wait_for(ws.recv(), timeout=self.idle_timeout)
                except asyncio.TimeoutError:
                    raise RuntimeError(f"no frame or heartbeat for {self.idle_timeout:g}s")
                if isinstance(msg, str):
                    m = json.loads(msg)
                    t = m.get("type")
                    if t == "hello":
                        self._on_hello(m)
                        if self._master_missing(m.get("refdata", [])):
                            await ws.send(json.dumps({"type": "refdata", "session_date": self.session_date, "file": "master.csv"}))
                        else:
                            self._load_master()
                            await ws.send(self._subscribe_msg()); subscribed = True
                    elif t == "file":
                        file_meta = m
                    elif t == "heartbeat":
                        st = m.get("stats") or {}
                        if int(st.get("dropped") or 0) > self.stats["server_dropped"]:
                            log.warning("the server discarded %s queued frames for us (we were too slow); a fresh snapshot follows",
                                        int(st["dropped"]) - self.stats["server_dropped"])
                        self.stats["server_dropped"] = int(st.get("dropped") or 0)
                        if int(m.get("session_date") or 0) not in (0, self.session_date):
                            raise RuntimeError("new session on the server: reconnecting")
                    elif t == "snapshot_done":
                        log.info("snapshot received: %d instruments priced", len(self.latest))
                    elif t == "error":
                        log.error("server: %s", m.get("error"))
                        if not subscribed:                   # e.g. master.csv not served: carry on with bare tokens
                            self._load_master()
                            await ws.send(self._subscribe_msg()); subscribed = True
                else:
                    kind, sid, offset, payload = unpack_frame(msg)
                    self.stats["frames"] += 1; self.stats["bytes"] += len(msg)
                    if kind == KIND_RING:
                        self._on_ring(sid, payload)
                    elif kind == KIND_FILE and file_meta is not None:
                        if hashlib.sha256(payload).hexdigest() != file_meta.get("sha256"):
                            log.error("refdata %s: checksum mismatch, ignored", file_meta.get("file"))
                        else:
                            p = self.refdata_dir / str(file_meta.get("session_date") or self.session_date) / file_meta["file"]
                            p.parent.mkdir(parents=True, exist_ok=True)
                            p.write_bytes(payload)
                        file_meta = None
                        if not subscribed:
                            self._load_master()
                            await ws.send(self._subscribe_msg()); subscribed = True
                if stats_every and time.monotonic() - last_stats >= stats_every:
                    last_stats = time.monotonic()
                    log.info(self.summary())
        self.connected = False

    def summary(self):
        lag = (time.time_ns() - self.stats["newest_recv_ns"]) / 1e6 if self.stats["newest_recv_ns"] else None
        return (f"ticks={self.stats['ticks']} bars={self.stats['bars']} priced={len(self.latest)} frames={self.stats['frames']} "
                f"data={self.stats['bytes'] / 1e6:.1f}MB live-lag={f'{lag:.0f}ms' if lag is not None else 'n/a'} "
                f"(lag = this computer's clock minus the newest tick's arrival time on the feed)")

    async def _run(self, stats_every=30.0):
        backoff = 1.0
        while not self._stop.is_set():
            try:
                await self._run_once(stats_every)
                backoff = 1.0
            except asyncio.CancelledError:
                self.connected = False
                raise
            except Exception as e:                           # noqa: BLE001 -- keep reconnecting whatever happened
                self.connected = False
                code = getattr(getattr(e, "rcvd", None), "code", None)
                if code == 4001:
                    log.info("new session on the server: reconnecting"); backoff = 1.0
                else:
                    log.warning("link down: %s -- retry in %.0fs", e, backoff)
            if self._stop.is_set():
                break
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 10.0)

    def run(self, stats_every=30.0):
        """Blocking: receive until Ctrl-C / stop()."""
        self._loop = asyncio.new_event_loop()
        try:
            self._task = self._loop.create_task(self._run(stats_every))
            self._loop.run_until_complete(self._task)
        except (KeyboardInterrupt, asyncio.CancelledError):
            pass
        finally:
            self.connected = False
            self._loop.close(); self._loop = None

    def start(self, stats_every=30.0):
        """Receive in a background thread (daemon); returns at once."""
        self._thread = threading.Thread(target=self.run, kwargs={"stats_every": stats_every}, name="alphabots-feed", daemon=True)
        self._thread.start()
        return self

    def stop(self):
        self._stop.set()
        loop, task = self._loop, self._task
        if loop is not None and task is not None:
            try:
                loop.call_soon_threadsafe(task.cancel)
            except RuntimeError:
                pass
        if self._thread is not None and threading.current_thread() is not self._thread:
            self._thread.join(timeout=5)

    def wait_ready(self, timeout=30.0):
        """Block until the first snapshot has priced something (or the timeout): True when data is flowing."""
        t0 = time.monotonic()
        while time.monotonic() - t0 < timeout:
            if self.latest:
                return True
            time.sleep(0.05)
        return bool(self.latest)


# ---- command line: sinks -------------------------------------------------------------------------------------------
class JsonSink:
    def __init__(self, path=None):
        self.fh = open(path, "a", encoding="utf-8") if path else sys.stdout
        self.n = 0

    def __call__(self, d):
        self.fh.write(json.dumps(d, separators=(",", ":")) + "\n")
        self.n += 1
        self.fh.flush()                                      # every line is readable at once (a tail -f / Get-Content -Wait sees it)


class CsvSink:
    TICK = ("recv_time", "exch_time", "token", "symbol", "underlying", "expiry", "strike", "right", "nse_token", "ltp", "bid", "ask",
            "bid_qty", "ask_qty", "last_trade_qty", "volume", "oi", "open", "high", "low", "prev_close", "exch_ts_ns", "recv_ts_ns")
    BAR = ("time", "tf", "token", "symbol", "underlying", "expiry", "strike", "right", "nse_token", "open", "high", "low", "close", "volume", "oi", "stale", "bar_ts_ns")

    def __init__(self, directory, feed, session=None):
        self.dir, self.feed, self.session = Path(directory), feed, session   # session() -> the feed's session date
        self.dir.mkdir(parents=True, exist_ok=True)
        self.files = {}

    def _date(self, fallback):
        d = self.session() if self.session else 0
        return str(d) if d else fallback

    def _writer(self, kind, date, cols):
        key = (kind, date)
        if key not in self.files:
            p = self.dir / f"{self.feed}_{kind}_{date}.csv"
            new = not p.exists() or p.stat().st_size == 0
            fh = open(p, "a", newline="", encoding="utf-8")
            w = csv.DictWriter(fh, fieldnames=cols, extrasaction="ignore")
            if new:
                w.writeheader()
            self.files[key] = (fh, w)
        return self.files[key]

    def tick(self, d):
        fh, w = self._writer("ticks", self._date(ist(d["recv_ts_ns"]).strftime("%Y%m%d") if d["recv_ts_ns"] else "unknown"), self.TICK)
        w.writerow(d); fh.flush()

    def bar(self, d):
        fh, w = self._writer(f"bars_{d['tf']}", self._date(d["time"][:10].replace("-", "")), self.BAR)
        w.writerow(d); fh.flush()


def _enable_vt():
    if os.name == "nt":
        try:
            import ctypes
            k = ctypes.windll.kernel32
            h = k.GetStdHandle(-11)
            mode = ctypes.c_uint32()
            if k.GetConsoleMode(h, ctypes.byref(mode)):
                k.SetConsoleMode(h, mode.value | 0x0004)
        except Exception:                                    # noqa: BLE001
            pass


def watch(feed, rows=40, every=1.0):
    _enable_vt()
    while not feed._stop.is_set():
        lines = [f"{feed.feed} feed  session {feed.session_date}  {'connected' if feed.connected else 'CONNECTING'}  {time.strftime('%H:%M:%S')}  "
                 f"ticks={feed.stats['ticks']} bars={feed.stats['bars']} priced={len(feed.latest)}",
                 f"{'symbol':32} {'ltp':>10} {'bid':>10} {'ask':>10} {'volume':>12} {'exch time':>12} {'age s':>7}"]
        now = time.time_ns()
        ticks = sorted(feed.latest.values(), key=lambda t: t["symbol"])[:rows]
        for t in ticks:
            lines.append(f"{t['symbol'][:32]:32} {t['ltp']:>10.2f} {t['bid']:>10.2f} {t['ask']:>10.2f} {t['volume']:>12} {t['exch_time']:>12} "
                         f"{(now - t['recv_ts_ns']) / 1e9:>7.1f}")
        sys.stdout.write("\033[2J\033[H" + "\n".join(lines) + "\n"); sys.stdout.flush()
        time.sleep(every)


def main(argv=None):
    ap = argparse.ArgumentParser(description="ALPHABOTS live data subscriber", formatter_class=argparse.RawDescriptionHelpFormatter,
                                 epilog="Output: one JSON line per tick / bar on stdout (see --out, --csv-dir, --watch). Logs go to stderr (see --log-file).")
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("--token"); ap.add_argument("--token-file", help="file holding the token (one line); or set FEED_TOKEN")
    ap.add_argument("--cafile", help="the feed's certificate (default: feed_cert.pem next to this script)")
    ap.add_argument("--insecure", action="store_true", help="no certificate check (tests only)")
    ap.add_argument("--feed", default=DEFAULT_FEED, help="broker feed to receive (default alphabots)")
    ap.add_argument("--no-ticks", action="store_true", help="bars only")
    ap.add_argument("--bars", default="1m", help="'1m', '1m,1s', '1s' or 'none' (default 1m)")
    ap.add_argument("--underlying", default="", help="comma list, e.g. NIFTY,BANKNIFTY (default all)")
    ap.add_argument("--right", default="", help="comma list of CE,PE,FUT (default all)")
    ap.add_argument("--expiry", default="", help="comma list of YYYY-MM-DD (default all)")
    ap.add_argument("--symbols", default="", help="comma list of substrings a symbol must contain, e.g. '25000 CE'")
    ap.add_argument("--tokens", default="", help="comma list of instrument tokens")
    ap.add_argument("--include-stale", action="store_true", help="also emit carry-forward (stale) bars")
    ap.add_argument("--out", help="append JSON lines to this file instead of stdout")
    ap.add_argument("--csv-dir", help="also append CSV files per day into this directory")
    ap.add_argument("--quiet", action="store_true", help="no JSON lines (with --csv-dir, or for the API)")
    ap.add_argument("--watch", action="store_true", help="live table of the latest prices instead of JSON lines")
    ap.add_argument("--watch-rows", type=int, default=40)
    ap.add_argument("--list", action="store_true", help="print the day's instruments (symbol, token, lot size) and exit")
    ap.add_argument("--refdata-dir", help="where the day's instrument master is cached (default ./refdata next to this script)")
    ap.add_argument("--stats-every", type=float, default=30.0, help="seconds between summary log lines (0 = never)")
    ap.add_argument("--idle-timeout", type=float, default=15.0, help="reconnect when nothing arrives for this long")
    ap.add_argument("--log-file", help="write the log here instead of stderr")
    a = ap.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                        **({"filename": a.log_file} if a.log_file else {"stream": sys.stderr}))
    cafile = a.cafile
    if cafile is None and a.url.startswith("wss://") and not a.insecure:
        here = Path(__file__).resolve().parent / "feed_cert.pem"
        if here.exists():
            cafile = str(here)
        else:
            log.error("feed_cert.pem not found next to this script: pass --cafile <path to feed_cert.pem>"); return 2
    split = lambda s: [x for x in s.split(",") if x.strip()]  # noqa: E731
    bars = () if a.bars.strip().lower() in ("", "none", "no") else tuple(split(a.bars))
    try:
        feed = AlphabotsFeed(token=a.token, token_file=a.token_file, url=a.url, cafile=cafile, insecure=a.insecure, feed=a.feed,
                             ticks=not a.no_ticks, bars=bars, underlying=split(a.underlying), right=split(a.right), expiry=split(a.expiry),
                             symbols=split(a.symbols), tokens=split(a.tokens), include_stale=a.include_stale,
                             refdata_dir=a.refdata_dir, idle_timeout=a.idle_timeout)
    except ValueError as e:
        log.error("%s", e); return 2
    sinks_t, sinks_b = [], []
    if a.csv_dir:
        c = CsvSink(a.csv_dir, a.feed, lambda: feed.session_date); sinks_t.append(c.tick); sinks_b.append(c.bar)
    if not a.quiet and not a.watch and not a.list:
        j = JsonSink(a.out); sinks_t.append(j); sinks_b.append(j)
    feed.on_tick = (lambda d: [s(d) for s in sinks_t]) if sinks_t else None
    feed.on_bar = (lambda d: [s(d) for s in sinks_b]) if sinks_b else None
    if a.list:
        feed.start(stats_every=0)
        t0 = time.monotonic()
        while not feed.instruments.rows and time.monotonic() - t0 < 30:
            time.sleep(0.1)
        feed.stop()
        if not feed.instruments.rows:
            log.error("no instrument master received (not connected?)"); return 1
        print(f"{'symbol':34} {'token':>6} {'nse_token':>10} {'lot':>5}  underlying expiry strike right")
        for r in sorted(feed.instruments.rows, key=lambda r: (r["underlying"], r["expiry"], r["right"], r["strike"])):
            print(f"{r['symbol']:34} {r['token']:>6} {r['nse_token']:>10} {r['lot_size']:>5}  {r['underlying']} {r['expiry']} {r['strike']:g} {r['right']}")
        return 0
    try:
        if a.watch:
            feed.start(stats_every=0)
            watch(feed, rows=a.watch_rows)
        else:
            feed.run(stats_every=a.stats_every)
    except KeyboardInterrupt:
        pass
    finally:
        feed.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())

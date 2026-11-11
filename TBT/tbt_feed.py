#!/usr/bin/env python3
"""Capture the in-house TBT snapshot stream (NSE tick-by-tick, colo box) for NIFTY F&O.

Wire contract: ~/Desktop/INTEGRATION.md (WIRE_SPEC v1.2). Every delta ms the box emits one
WINDOW with a 64-byte record per instrument that changed; KEYFRAMEs carry day values every
30 s; HEARTBEATs fill silence. Records carry the NSE token and `exch_last_ns`, the exchange
timestamp (UTC ns) of the last message applied, which makes this the reference clock for
"when did it happen at the exchange" when ranking Quantick.

Two ways in:

  --hub tcp://HOST:4421     subscribe to the team's ZeroMQ hub (safe: any number of subscribers)
  --upstream HOST:4420      connect to the publisher directly. The publisher serves ONE client
                            and a new one replaces the old, so this would knock a running hub
                            off. Refused unless --allow-direct is given.

Output uses the Arrow/Quantick schema and canonical keys (TBT/tbt_live_ticks_<date>.csv,
TBT/tbt_live_1s_<date>.csv) plus a frame log (TBT/tbt_frames_<date>.csv) for window-level
latency, missed windows and outages. listed_at_ist = exch_last_ns.
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import os
import signal
import socket
import struct
import sys
import time
import urllib.request
from pathlib import Path
from typing import Any, Iterator

import pandas as pd
import zstandard

BASE_DIR = Path(__file__).resolve().parent
ROOT_DIR = BASE_DIR.parent
sys.path.insert(0, str(ROOT_DIR / "Arrow"))
import arrow_websocket as arrow  # noqa: E402  shared schema, canonical keys, TickStorage

IST = arrow.IST
now_ist = arrow.now_ist

# ------------------------------------------------------------------ wire (spec section 1 / 4)
FRAME_HDR = struct.Struct("<IHHI")              # len, type, version, raw_len
BODY_HDR = struct.Struct("<QQQIIIIII")          # window_id, t_k_ns, emit_ns, delta_ms, nstreams, nrecords, flags, late_count, depth_levels
RECORD = struct.Struct("<IIqIIIIIIIIIIII")      # token, flags, exch_last_ns, bid_px, bid_qty, ask_px, ask_qty,
                                                # open, high, low, close, vol_at_ask, vol_at_bid, trades, day_volume
WINDOW, KEYFRAME, HEARTBEAT = 1, 2, 3
F_TRADED, F_BOOK, F_STALE, F_NOBOOK, F_DAY, F_CASH = 1, 2, 4, 8, 16, 32
_dctx = zstandard.ZstdDecompressor()


def decode(frame: bytes) -> dict[str, Any]:
    _ln, ftype, version, raw_len = FRAME_HDR.unpack_from(frame, 0)
    if ftype == HEARTBEAT:
        (emit_ns,) = struct.unpack_from("<Q", frame, 12)
        return {"type": ftype, "version": version, "emit_ns": emit_ns, "records": []}
    body = _dctx.decompress(frame[12:], max_output_size=raw_len)
    window_id, t_k_ns, emit_ns, delta_ms, nstreams, nrecords, flags, late_count, depth_levels = BODY_HDR.unpack_from(body, 0)
    stale = body[48 + 4 * nstreams: 48 + 5 * nstreams]
    off = (48 + 5 * nstreams + 7) & ~7
    records = [RECORD.unpack_from(body, off + 64 * i) for i in range(nrecords)]
    return {"type": ftype, "version": version, "window_id": window_id, "t_k_ns": t_k_ns, "emit_ns": emit_ns,
            "delta_ms": delta_ms, "nrecords": nrecords, "flags": flags, "late_count": late_count,
            "depth_levels": depth_levels, "stale_streams": sum(1 for b in stale if b), "records": records}


def split_frames(buf: bytearray) -> Iterator[bytes]:
    """Incremental splitter: yields complete frames and leaves the remainder in buf."""
    while len(buf) >= 4:
        (ln,) = struct.unpack_from("<I", buf, 0)
        if len(buf) < 4 + ln:
            return
        frame = bytes(buf[:4 + ln])
        del buf[:4 + ln]
        yield frame


# ------------------------------------------------------------------ token -> canonical key


def nifty_token_map(refresh: bool = False) -> dict[int, dict[str, Any]]:
    """NSE F&O token -> contract metadata, from Arrow's public instrument master (no login)."""
    master = arrow.prepare_instrument_master(arrow.ensure_instrument_master(arrow.INSTRUMENT_MASTER_FILE, refresh=refresh))
    raw = pd.read_csv(arrow.INSTRUMENT_MASTER_FILE, low_memory=False)
    fo = raw[(raw["ExchSeg"] == "NSEFO") & (raw["Underlying"].astype(str).str.upper() == "NIFTY")]
    out: dict[int, dict[str, Any]] = {}
    for r in fo.itertuples(index=False):
        try:
            expiry = pd.to_datetime(r.Expiry).date()
        except (ValueError, TypeError):
            continue
        opt = str(r.OptionType or "").upper()
        # Arrow marks futures with OptionType 'XX' (TradingSymbol like NIFTY27OCT26F).
        itype = opt if opt in {"CE", "PE"} else ("FUT" if opt == "XX" else None)
        if itype is None:
            continue
        token = int(getattr(r, "ExchangeID", None) or r.Token)
        strike = None if itype == "FUT" else float(r.StrikePrice)
        out[token] = {
            "token": token, "native_symbol": str(r.TradingSymbol), "symbol": str(r.TradingSymbol),
            "canonical_symbol": arrow.canonical_symbol("NIFTY", expiry, strike, itype, str(r.TradingSymbol)),
            "underlying": "NIFTY", "expiry": expiry.isoformat(), "strike": arrow._strike_text(strike),
            "instrument_type": itype, "exchange_segment": "NFO_FUT" if itype == "FUT" else "NFO_OPT",
        }
    del master
    return out


# ------------------------------------------------------------------ sources


class DirectSource:
    """TCP to the publisher. First connect: no REPLAY60 (fresh KEYFRAME); reconnects send REPLAY60."""

    def __init__(self, upstream: str, silence_s: float = 2.0):
        host, _, port = upstream.rpartition(":")
        self.addr = (host, int(port))
        self.silence_s = silence_s
        self.connects = 0

    def frames(self, should_stop) -> Iterator[tuple[int, str, bytes]]:
        while not should_stop():
            try:
                sock = socket.create_connection(self.addr, timeout=5)
            except OSError as exc:
                yield time.time_ns(), "status", json.dumps({"event": "connect_failed", "error": str(exc)}).encode()
                time.sleep(1)
                continue
            if self.connects:
                sock.sendall(b"REPLAY60")
            self.connects += 1
            yield time.time_ns(), "status", json.dumps({"event": "upstream_connected", "replay": self.connects > 1}).encode()
            sock.settimeout(self.silence_s)
            buf = bytearray()
            try:
                while not should_stop():
                    chunk = sock.recv(1 << 20)
                    recv_ns = time.time_ns()  # first thing after the read: stamp before parsing
                    if not chunk:
                        raise ConnectionError("closed by far end")
                    buf += chunk
                    for frame in split_frames(buf):
                        yield recv_ns, "box", frame
            except (OSError, ConnectionError) as exc:
                yield time.time_ns(), "status", json.dumps({"event": "upstream_lost", "error": str(exc)}).encode()
            finally:
                sock.close()
            time.sleep(1)


class HubSource:
    """ZeroMQ subscriber per the brief: SUB tbt.fo.snapshot first, then a private tbt.kf.<id>."""

    def __init__(self, hub: str, topic: str = "tbt.fo.snapshot"):
        import zmq

        self.zmq = zmq
        self.ctx = zmq.Context.instance()
        self.sock = self.ctx.socket(zmq.SUB)
        self.sock.setsockopt(zmq.RCVHWM, 0)
        self.sock.connect(hub)
        self.topic = topic.encode()
        self.private = f"tbt.kf.{os.getpid()}{int(time.time())}".encode()
        self.sock.setsockopt(zmq.SUBSCRIBE, self.topic)
        self.sock.setsockopt(zmq.SUBSCRIBE, self.private)

    def frames(self, should_stop) -> Iterator[tuple[int, str, bytes]]:
        poller = self.zmq.Poller()
        poller.register(self.sock, self.zmq.POLLIN)
        while not should_stop():
            if not dict(poller.poll(500)):
                continue
            parts = self.sock.recv_multipart()
            recv_ns = time.time_ns()
            kind = parts[1].decode() if len(parts) > 1 else "?"
            payload = parts[2] if len(parts) > 2 else b""
            yield recv_ns, kind, payload


# ------------------------------------------------------------------ storage

FRAME_COLUMNS = ["received_at_ist", "recv_ns", "kind", "type", "window_id", "t_k_ns", "emit_ns", "delta_ms",
                 "nrecords", "nifty_records", "late_count", "flags", "stale_streams", "missed_windows", "info"]


def ns_to_ist(ns: int) -> dt.datetime:
    return dt.datetime.fromtimestamp(ns / 1e9, tz=dt.timezone.utc).astimezone(IST)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    src = ap.add_mutually_exclusive_group(required=True)
    src.add_argument("--hub", help="ZeroMQ hub publish address, e.g. tcp://HUB_HOST:4421")
    src.add_argument("--upstream", help="publisher HOST:PORT, e.g. PUBLISHER_HOST:4420 (one client only!)")
    ap.add_argument("--allow-direct", action="store_true", help="really connect to the publisher directly")
    ap.add_argument("--out", default=str(BASE_DIR))
    ap.add_argument("--until", default="15:32", help="stop time IST HH:MM")
    ap.add_argument("--all-nifty", action="store_true", help="write every NIFTY F&O token, not just today's Quantick universe")
    ap.add_argument("--refresh-master", action="store_true")
    ap.add_argument("--no-duckdb", action="store_true")
    ap.add_argument("--probe", type=float, default=0, help="print frames for N seconds, write nothing")
    args = ap.parse_args()

    if args.upstream and not args.allow_direct:
        print("Refusing --upstream without --allow-direct: the publisher serves one client and a new connection "
              "knocks the current one (the team hub) off. Use --hub, or pass --allow-direct if nothing else is connected.")
        return 2

    tokens = nifty_token_map(args.refresh_master)
    wanted = None
    if not args.all_nifty:
        # Same contracts as today's Quantick capture, so the comparison is like for like.
        uj = ROOT_DIR / "Quantick" / f"quantick_universe_{now_ist().date().isoformat()}.json"
        if uj.exists():
            wanted = set(json.loads(uj.read_text()).get("canonical_by_symbol", {}).values())
    keep = {t: m for t, m in tokens.items() if wanted is None or m["canonical_symbol"] in wanted}
    print("=" * 72)
    print(f"  TBT snapshot capture | {now_ist():%Y-%m-%d %H:%M:%S %Z} | source={'hub ' + args.hub if args.hub else 'DIRECT ' + args.upstream}")
    print(f"  NIFTY F&O tokens known: {len(tokens)}  writing: {len(keep)}"
          f"{' (all NIFTY)' if wanted is None else ' (today''s Quantick universe)'}")
    print("=" * 72, flush=True)

    source = HubSource(args.hub) if args.hub else DirectSource(args.upstream)
    state = {"stop": False}
    signal.signal(signal.SIGINT, lambda *_: state.update(stop=True))
    signal.signal(signal.SIGTERM, lambda *_: state.update(stop=True))
    hh, mm = map(int, args.until.split(":"))
    day = now_ist().date()
    stop_at = dt.datetime.combine(day, dt.time(hh, mm), tzinfo=IST)
    probe_until = time.time() + args.probe if args.probe else None
    should_stop = lambda: state["stop"] or now_ist() >= stop_at or (probe_until is not None and time.time() > probe_until)

    storage = frame_log = frame_writer = None
    if not args.probe:
        out = Path(args.out)
        out.mkdir(parents=True, exist_ok=True)
        tag = day.isoformat()
        storage = arrow.TickStorage(out / f"tbt_live_ticks_{tag}.csv", out / f"tbt_live_ticks_{tag}.duckdb",
                                    out / f"tbt_live_1s_{tag}.csv", "1s", duckdb_enabled=not args.no_duckdb)
        fpath = out / f"tbt_frames_{tag}.csv"
        new = not fpath.exists()
        frame_log = fpath.open("a", newline="")
        frame_writer = csv.writer(frame_log)
        if new:
            frame_writer.writerow(FRAME_COLUMNS)

    last_tk = None
    last_ltp: dict[int, float] = {}
    day_vol: dict[int, list[int]] = {}  # token -> [last_raw_u32, u64_total]
    stats = {"frames": 0, "records": 0, "missed": 0, "outages": 0, "lat": []}
    last_report = time.time()
    try:
        for recv_ns, kind, payload in source.frames(should_stop):
            received_at = ns_to_ist(recv_ns)
            if kind in ("status", "snap_end") or not payload or kind == "hub_hb" and len(payload) < 12:
                info = payload.decode(errors="replace") if kind in ("status", "snap_end") else ""
                if kind == "status":
                    print(f"  [{received_at:%H:%M:%S}] status {info}", flush=True)
                    if "source_down" in info or "upstream_lost" in info:
                        stats["outages"] += 1
                if frame_writer:
                    frame_writer.writerow([received_at.isoformat(), recv_ns, kind] + [None] * 11 + [info])
                continue
            try:
                f = decode(payload)
            except Exception as exc:
                print(f"  decode error ({kind}): {exc}", flush=True)
                continue
            stats["frames"] += 1
            missed = 0
            if f["type"] == WINDOW and kind in ("box", "replay"):
                if last_tk is not None:
                    if f["t_k_ns"] <= last_tk:
                        continue  # duplicate from REPLAY60
                    missed = max(0, round((f["t_k_ns"] - last_tk) / (f["delta_ms"] * 1e6)) - 1)
                    stats["missed"] += missed
                last_tk = f["t_k_ns"]
                if kind == "box":
                    stats["lat"].append((recv_ns - f["t_k_ns"]) / 1e6)
            if args.probe:
                ftype = {1: "WINDOW", 2: "KEYFRAME", 3: "HEARTBEAT"}.get(f["type"], f["type"])
                print(f"  {kind:<7} {ftype:<9} window={f.get('window_id')} nrec={len(f['records'])} "
                      f"emit_age_ms={(recv_ns - f['emit_ns']) / 1e6:.1f}", flush=True)
                continue
            n_nifty = 0
            for rec in f["records"]:
                token, flags, exch_ns, bid, bid_q, ask, ask_q, o, h, l, c, v_ask, v_bid, trades, dvol = rec
                if flags & F_CASH or token not in keep:
                    continue
                meta = keep[token]
                n_nifty += 1
                prev = day_vol.get(token)
                if prev is None:
                    day_vol[token] = [dvol, dvol]
                else:
                    prev[1] += (dvol - prev[0]) & 0xFFFFFFFF
                    prev[0] = dvol
                if c:
                    last_ltp[token] = c / 100
                listed = ns_to_ist(exch_ns) if exch_ns > 0 else received_at
                storage.persist({
                    **meta,
                    "date": listed.date().isoformat(), "datetime": listed.replace(microsecond=0).isoformat(),
                    "listed_at_ist": listed.isoformat(), "received_at_ist": received_at.isoformat(),
                    "ltp": last_ltp.get(token), "ltq": None, "avg_price": None,
                    "volume": day_vol[token][1], "total_sell_quantity": None, "total_buy_quantity": None,
                    "open_interest": None, "oi_day_high": None, "oi_day_low": None,
                    "open_price": o / 100 if o else None, "high_price": h / 100 if h else None,
                    "low_price": l / 100 if l else None, "close_price": c / 100 if c else None,
                    "bid_price": bid / 100 if bid else None, "ask_price": ask / 100 if ask else None,
                    "bid_qty": bid_q, "ask_qty": ask_q, "bid_orders": None, "ask_orders": None,
                    "message_type": "keyframe" if f["type"] == KEYFRAME else ("window" if kind == "box" else kind),
                    "raw_json": json.dumps({"flags": flags, "trades": trades, "vol_at_ask": v_ask, "vol_at_bid": v_bid,
                                            "window_id": f.get("window_id"), "t_k_ns": f.get("t_k_ns"),
                                            "emit_ns": f.get("emit_ns"), "exch_last_ns": exch_ns, "kind": kind},
                                           separators=(",", ":")),
                })
            stats["records"] += n_nifty
            frame_writer.writerow([received_at.isoformat(), recv_ns, kind, f["type"], f.get("window_id"), f.get("t_k_ns"),
                                   f.get("emit_ns"), f.get("delta_ms"), len(f["records"]), n_nifty, f.get("late_count"),
                                   f.get("flags"), f.get("stale_streams"), missed, None])
            if time.time() - last_report >= 60:
                last_report = time.time()
                lat = sorted(stats["lat"]); stats["lat"].clear()
                p50 = lat[len(lat) // 2] if lat else float("nan")
                print(f"  [{now_ist():%H:%M:%S}] frames={stats['frames']} nifty_records={stats['records']} "
                      f"missed_windows={stats['missed']} outages={stats['outages']} recv-t_k p50={p50:.1f}ms", flush=True)
                frame_log.flush()
    finally:
        if storage:
            storage.close()
        if frame_log:
            frame_log.close()
    print(f"  done: frames={stats['frames']} nifty_records={stats['records']} missed_windows={stats['missed']}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())

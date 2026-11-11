#!/usr/bin/env python3
"""Capture the ALPHABOTS NSE tick-by-tick feed (peers' service) into the bench format.

Uses the peers' client `alphabots_feed.py` unchanged (TLS websocket to the abmd feed, pinned
`feed_cert.pem`, token in `feed.token`). Every tick carries `exch_ts_ns` (exchange time, ns),
cumulative `volume`, L1 and day OHLC; `recv_ts_ns` in the tick is the FEED SERVER's arrival
time, so this wrapper stamps THIS Mac's arrival itself (first statement of the callback).

    python Alphabots/alphabots_capture.py --until 15:35

Writes, in the Arrow/Quantick schema and canonical keys:
  Alphabots/alphabots_live_ticks_<date>.csv   one row per tick (listed_at_ist = exch_ts_ns)
  Alphabots/alphabots_live_1s_<date>.csv      1-second rollup
  Alphabots/alphabots_bars_1m_<date>.csv      the feed's own finished 1-minute bars + Mac arrival
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import signal
import sys
import threading
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(ROOT / "Arrow"))
import arrow_websocket as arrow  # noqa: E402
from dotenv import load_dotenv  # noqa: E402

load_dotenv(ROOT / ".env")  # ALPHABOTS_FEED_URL must be set before alphabots_feed reads it
from alphabots_feed import AlphabotsFeed  # noqa: E402

IST = arrow.IST
INDEX_NAMES = {"NIFTY": "INDEX:NIFTY 50", "BANKNIFTY": "INDEX:NIFTY BANK", "SENSEX": "INDEX:SENSEX"}
BAR_COLUMNS = ["received_at_ist", "recv_ns", "bar_close_ist", "arrival_after_close_ms", "canonical_symbol", "symbol",
               "nse_token", "tf", "bar_start_ist", "open", "high", "low", "close", "volume", "oi", "stale"]


def ns_ist(ns: int) -> dt.datetime:
    return dt.datetime.fromtimestamp(int(ns) / 1e9, tz=dt.timezone.utc).astimezone(IST)


def canonical(d: dict) -> str:
    u, right = d.get("underlying") or "", (d.get("right") or "").upper()
    if right in ("CE", "PE") and d.get("expiry"):
        return arrow.canonical_symbol(u, dt.date.fromisoformat(d["expiry"]), d.get("strike"), right, d.get("symbol") or "")
    if right == "FUT" and d.get("expiry"):
        return arrow._canon_future(u, dt.date.fromisoformat(d["expiry"]))
    return INDEX_NAMES.get(u, f"INDEX:{u}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--token-file", default=str(HERE / "feed.token"))
    ap.add_argument("--out", default=str(HERE))
    ap.add_argument("--until", default="15:35", help="stop time IST HH:MM")
    ap.add_argument("--underlying", default="", help="comma list (default: everything the feed carries)")
    ap.add_argument("--duckdb", action="store_true", help="also write a duckdb sidecar (CSV is the source of truth)")
    args = ap.parse_args()

    out = Path(args.out); out.mkdir(parents=True, exist_ok=True)
    day = arrow.now_ist().date().isoformat()
    storage = arrow.TickStorage(out / f"alphabots_live_ticks_{day}.csv", out / f"alphabots_live_ticks_{day}.duckdb",
                                out / f"alphabots_live_1s_{day}.csv", "1s", duckdb_enabled=args.duckdb)
    bar_path = out / f"alphabots_bars_1m_{day}.csv"
    new = not bar_path.exists()
    bar_fh = bar_path.open("a", newline="")
    bar_w = csv.writer(bar_fh)
    if new:
        bar_w.writerow(BAR_COLUMNS)
    lock = threading.Lock()
    stats = {"ticks": 0, "bars": 0, "lag_mac": [], "lag_feed": []}

    def on_tick(d: dict) -> None:
        recv_ns = time.time_ns()  # first statement: this Mac's arrival
        received = ns_ist(recv_ns)
        listed = ns_ist(d["exch_ts_ns"]) if d.get("exch_ts_ns") else received
        right = (d.get("right") or "").upper()
        row = {
            "date": listed.date().isoformat(), "datetime": listed.replace(microsecond=0).isoformat(),
            "listed_at_ist": listed.isoformat(), "received_at_ist": received.isoformat(),
            "token": d.get("nse_token") or d.get("token"), "exchange_segment": "NSE_INDEX" if right in ("", "SPOT") else ("NFO_FUT" if right == "FUT" else "NFO_OPT"),
            "symbol": d.get("symbol"), "native_symbol": d.get("symbol"), "canonical_symbol": canonical(d),
            "underlying": d.get("underlying"), "expiry": d.get("expiry") or None, "strike": arrow._strike_text(d.get("strike")) if right in ("CE", "PE") else None,
            "instrument_type": right if right in ("CE", "PE", "FUT") else "INDEX",
            "ltp": d.get("ltp"), "ltq": d.get("last_trade_qty"), "avg_price": None, "volume": d.get("volume"),
            "total_sell_quantity": None, "total_buy_quantity": None, "open_interest": d.get("oi"), "oi_day_high": None, "oi_day_low": None,
            "open_price": d.get("open"), "high_price": d.get("high"), "low_price": d.get("low"),
            "close_price": d.get("prev_close"),  # previous close, as in Quantick's ohlc.close
            "bid_price": d.get("bid") or None, "ask_price": d.get("ask") or None, "bid_qty": d.get("bid_qty"), "ask_qty": d.get("ask_qty"),
            "bid_orders": None, "ask_orders": None, "message_type": "tick",
            "raw_json": json.dumps({"token": d.get("token"), "nse_token": d.get("nse_token"), "exch_ts_ns": d.get("exch_ts_ns"),
                                    "feed_recv_ts_ns": d.get("recv_ts_ns"), "mac_recv_ns": recv_ns}, separators=(",", ":")),
        }
        storage.persist(row)
        with lock:
            stats["ticks"] += 1
            if d.get("exch_ts_ns"):
                stats["lag_mac"].append((recv_ns - d["exch_ts_ns"]) / 1e6)
                if d.get("recv_ts_ns"):
                    stats["lag_feed"].append((d["recv_ts_ns"] - d["exch_ts_ns"]) / 1e6)

    def on_bar(b: dict) -> None:
        recv_ns = time.time_ns()
        start = ns_ist(b["bar_ts_ns"])
        close_at = start + dt.timedelta(minutes=1 if b.get("tf") == "1m" else 0, seconds=1 if b.get("tf") == "1s" else 0)
        with lock:
            bar_w.writerow([ns_ist(recv_ns).isoformat(), recv_ns, close_at.isoformat(), round((recv_ns / 1e9 - close_at.timestamp()) * 1000, 1),
                            canonical(b), b.get("symbol"), b.get("nse_token"), b.get("tf"), start.isoformat(),
                            b.get("open"), b.get("high"), b.get("low"), b.get("close"), b.get("volume"), b.get("oi"), b.get("stale")])
            bar_fh.flush()
            stats["bars"] += 1

    feed = AlphabotsFeed(token_file=args.token_file, cafile=str(HERE / "feed_cert.pem"), bars=("1m",),
                         underlying=[u for u in args.underlying.split(",") if u.strip()], on_tick=on_tick, on_bar=on_bar,
                         refdata_dir=str(HERE / "refdata"))
    stop = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    hh, mm = map(int, args.until.split(":"))
    stop_at = dt.datetime.combine(arrow.now_ist().date(), dt.time(hh, mm), tzinfo=IST)
    print(f"  ALPHABOTS capture {arrow.now_ist():%Y-%m-%d %H:%M:%S} until {args.until} -> {out}", flush=True)
    feed.start(stats_every=0)
    last = time.time()
    try:
        while not stop.is_set() and arrow.now_ist() < stop_at:
            time.sleep(1)
            if time.time() - last >= 60:
                last = time.time()
                with lock:
                    lm, lf = sorted(stats["lag_mac"]), sorted(stats["lag_feed"])
                    stats["lag_mac"].clear(); stats["lag_feed"].clear()
                p = lambda v, q: v[int(len(v) * q)] if v else float("nan")
                print(f"  [{arrow.now_ist():%H:%M:%S}] connected={feed.connected} ticks={stats['ticks']} bars={stats['bars']} "
                      f"mac-exch p50={p(lm, .5):.0f}ms p95={p(lm, .95):.0f}ms | feed-server-exch p50={p(lf, .5):.0f}ms "
                      f"server_dropped={feed.stats['server_dropped']}", flush=True)
    finally:
        feed.stop()
        storage.close()
        bar_fh.close()
    print(f"  done: ticks={stats['ticks']} bars={stats['bars']}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""Offline test: a spec-following fake publisher on localhost, the collector against it.

    python TBT/test_tbt_feed.py

Checks: frames decode, records map to canonical keys, prices are paise/100, day_volume is
accumulated, the frame log has no missed windows, and a forced disconnect is followed by a
REPLAY60 reconnect that refills the gap.
"""
from __future__ import annotations

import socket
import struct
import subprocess
import sys
import threading
import time
from pathlib import Path

import pandas as pd
import zstandard

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
PORT = 44200
DELTA_MS = 100
TOKENS = [48704, 47126, 47127]  # NIFTY Oct future + two options from Arrow's master
cctx = zstandard.ZstdCompressor(level=3)


def body(ftype: int, k: int, t_k_ns: int, records: list[tuple]) -> bytes:
    hdr = struct.pack("<QQQIIIIII", k, t_k_ns, time.time_ns(), DELTA_MS, 23, len(records), 0, 0, 0)
    hdr += struct.pack("<23I", *range(23)) + bytes(23)
    hdr += bytes((-len(hdr)) % 8)
    return hdr + b"".join(struct.pack("<IIqIIIIIIIIIIII", *r) for r in records)


def frame(ftype: int, payload: bytes) -> bytes:
    comp = cctx.compress(payload)
    return struct.pack("<IHHI", 8 + len(comp), ftype, 1, len(payload)) + comp


def generator(stop: threading.Event, history: list, lock: threading.Lock) -> None:
    """Like the real box: a WINDOW every delta on the grid, connected or not."""
    vol = {t: 0 for t in TOKENS}
    while not stop.is_set():
        k = time.time_ns() // (DELTA_MS * 1_000_000) + 1
        time.sleep(max(0, (k * DELTA_MS * 1_000_000 - time.time_ns()) / 1e9) + 0.005)
        for t in TOKENS:
            vol[t] += 65
        recs = [(t, 1 | 2, time.time_ns() - 3_000_000, 10000 + k % 7, 65, 10050, 130,
                 10010, 10030, 10000, 10020 + k % 5, 65, 0, 1, vol[t] & 0xFFFFFFFF) for t in TOKENS]
        with lock:
            history.append(frame(1, body(1, k, k * DELTA_MS * 1_000_000, recs)))


def publisher(stop: threading.Event, history: list) -> None:
    lock = threading.Lock()
    threading.Thread(target=generator, args=(stop, history, lock), daemon=True).start()
    srv = socket.socket(); srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", PORT)); srv.listen(1)
    connections = 0
    while not stop.is_set():
        srv.settimeout(0.5)
        try:
            conn, _ = srv.accept()
        except socket.timeout:
            continue
        connections += 1
        conn.settimeout(0.2)
        try:
            replay = conn.recv(8) == b"REPLAY60"
        except socket.timeout:
            replay = False
        with lock:
            idx = len(history)
            if replay:
                for fr in history[-600:]:
                    conn.sendall(fr)
            else:
                k = time.time_ns() // (DELTA_MS * 1_000_000)
                recs = [(t, 16, time.time_ns(), 10000, 65, 10050, 130, 9000, 11000, 8000, 10025, 0, 0, 5, 0) for t in TOKENS]
                conn.sendall(frame(2, body(2, k, k * DELTA_MS * 1_000_000, recs)))
        sent = 0
        while not stop.is_set():
            time.sleep(0.02)
            with lock:
                new, idx = history[idx:], len(history)
            try:
                for fr in new:
                    conn.sendall(fr)
                    sent += 1
            except OSError:
                break
            if connections == 1 and sent >= 30:   # force one disconnect; next connect must REPLAY60
                break
        conn.close()
    srv.close()


def main() -> int:
    out = HERE / "_test_out"
    out.mkdir(exist_ok=True)
    for f in out.glob("*"):
        f.unlink()
    stop, history = threading.Event(), []
    th = threading.Thread(target=publisher, args=(stop, history), daemon=True)
    th.start()
    time.sleep(0.3)
    until = time.strftime("%H:%M", time.localtime(time.time() + 120))
    proc = subprocess.Popen([sys.executable, str(HERE / "tbt_feed.py"), "--upstream", f"127.0.0.1:{PORT}", "--allow-direct",
                             "--all-nifty", "--no-duckdb", "--out", str(out), "--until", until],
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    time.sleep(10)
    proc.terminate()
    log = proc.communicate(timeout=20)[0]
    stop.set()
    day = time.strftime("%Y-%m-%d")
    frames = pd.read_csv(out / f"tbt_frames_{day}.csv")
    ticks = pd.read_csv(out / f"tbt_live_ticks_{day}.csv")
    win = frames[frames["type"] == 1]
    checks = {
        "frames decoded": len(frames[frames["type"].notna()]) > 50,
        "keyframe first": int(frames[frames["type"].notna()].iloc[0]["type"]) == 2,
        "reconnected with REPLAY60": '"replay": true' in log,
        "no missed windows after replay": int(win["missed_windows"].fillna(0).sum()) == 0,
        "window ids contiguous": bool((pd.Series(sorted(win["t_k_ns"].astype("int64"))).diff().dropna() == DELTA_MS * 1_000_000).all()),
        "canonical keys": set(ticks["canonical_symbol"]) >= {"NIFTY26OCTFUT"},
        "price paise->rupees": abs(ticks[ticks["message_type"] == "window"]["bid_price"].iloc[0] - 100.0) < 0.1,
        "day_volume increasing": bool(ticks[ticks["canonical_symbol"] == "NIFTY26OCTFUT"]["volume"].is_monotonic_increasing),
    }
    for name, ok in checks.items():
        print(f"  {'PASS' if ok else 'FAIL'}  {name}")
    print("\ncollector log tail:\n" + "\n".join(log.strip().splitlines()[-6:]))
    return 0 if all(checks.values()) else 1


if __name__ == "__main__":
    sys.exit(main())

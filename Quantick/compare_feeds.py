#!/usr/bin/env python3
"""Compare Quantick against the other feeds and against the official EOD files.

    python Quantick/compare_feeds.py live --date 2026-10-16
    python Quantick/compare_feeds.py eod  --date 2026-10-16
    python Quantick/compare_feeds.py all  --date 2026-10-16

live  Quantick's 1-second capture vs every other broker captured that day
      (Arrow, Arrow HFT, Dhan, FYERS, Nubra, XTS -- whichever files exist), on the
      contracts they share (NIFTY options + futures). Per broker:
        cadence      updates per symbol-minute, raw inter-arrival p50
        delay        receive time - exchange time (p50/p95)
        agreement    same-second LTP equality on a forward-filled 1s grid
        lead/lag     for each Quantick price change, when the other feed showed
                     the same price (positive = Quantick later)
        range        1-minute high-low vs the other feed (shrinkage = coalescing)

eod   Quantick's historical candles vs:
        official     NSE F&O / NSE cash / NSE indices / BSE F&O / BSE cash bhavcopy
                     (day OHLC, close/last/settle, volume, OI)
        own live     its own live capture: day OHLC from the last tick, and
                     minute candles rebuilt from the 1s capture
        other feeds  minute candles rebuilt from every other broker's 1s capture

Reports land in Quantick/reports/<date>/ (summary.md + one CSV per table).
"""
from __future__ import annotations

import argparse
import datetime as dt
import io
import json
import sys
import time
import urllib.request
import zipfile
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from dotenv import dotenv_values

BASE_DIR = Path(__file__).resolve().parent
ROOT_DIR = BASE_DIR.parent
sys.path.insert(0, str(ROOT_DIR))
sys.path.insert(0, str(BASE_DIR))

IST = dt.timezone(dt.timedelta(hours=5, minutes=30))
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36"
SESSION = (dt.time(9, 15), dt.time(15, 30))
MCX_END = dt.time(23, 30)

# broker -> (folder, 1s rollup pattern, raw ticks pattern or None, Server.py loader name, loader kwargs)
FEEDS: dict[str, tuple[str, str, str | None, str, dict]] = {
    "quantick": ("Quantick", "quantick_live_1s_{d}.csv", "quantick_live_ticks_{d}.csv", "_load_arrow_rows", {"broker": "quantick"}),
    "arrow": ("Arrow", "arrow_live_1s_{d}.csv", "arrow_live_ticks_{d}.csv", "_load_arrow_rows", {}),
    "arrow_hft": ("Arrow", "arrow_hft_live_1s_{d}.csv", "arrow_hft_live_ticks_{d}.csv", "_load_arrow_hft_rows", {}),
    "dhan": ("Dhan", "dhan_live_1s_{d}.csv", "dhan_live_ticks_{d}.csv", "_load_dhan_rows", {}),
    "fyers": ("FYERS", "fyers_live_1s_{d}.csv", "fyers_live_ticks_{d}.csv", "_load_fyers_rows", {}),
    "nubra": ("Nubra", "nubra_live_1s_{d}.csv", None, "_load_nubra_rows", {}),
    "xts": ("XTS", "xts_live_1s_{d}.csv", "xts_live_ticks_{d}.csv", "_load_xts_rows", {}),
}


def _pct(x: float | None) -> str:
    return "-" if x is None or pd.isna(x) else f"{x:.1f}%"


def _num(x: Any, nd: int = 1) -> str:
    return "-" if x is None or pd.isna(x) else f"{x:,.{nd}f}"


def md_table(df: pd.DataFrame) -> str:
    if df is None or df.empty:
        return "_no rows_\n"
    cols = list(df.columns)
    lines = ["| " + " | ".join(map(str, cols)) + " |", "|" + "|".join("---" for _ in cols) + "|"]
    for _, row in df.iterrows():
        lines.append("| " + " | ".join("" if pd.isna(v) else str(v) for v in row.values) + " |")
    return "\n".join(lines) + "\n"


# ======================================================================= live


def load_feed(broker: str, day: str, end: dt.time = SESSION[1]) -> pd.DataFrame:
    import Server  # the dashboard's loaders: one uniform frame per broker, keyed by canonical_symbol

    folder, pattern, _, loader, kwargs = FEEDS[broker]
    path = ROOT_DIR / folder / pattern.format(d=day)
    if not path.exists():
        return pd.DataFrame()
    df = getattr(Server, loader)(str(path), "", **kwargs)
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.copy()
    df["broker"] = broker
    for col in ("timestamp_ist", "listed_at_ist", "received_at_ist"):
        df[col] = pd.to_datetime(df[col], errors="coerce", format="ISO8601")
    start = pd.Timestamp.combine(pd.Timestamp(day).date(), SESSION[0]).tz_localize(IST)
    end = pd.Timestamp.combine(pd.Timestamp(day).date(), end).tz_localize(IST)
    ts = df["timestamp_ist"]
    if ts.dt.tz is None:
        ts = ts.dt.tz_localize(IST)
        df["timestamp_ist"] = ts
    return df[(ts >= start) & (ts <= end)]


def load_change_events(broker: str, day: str, symbols: set[str], loaded: pd.DataFrame | None = None) -> pd.DataFrame:
    """(canonical_symbol, recv, ltp) at every price change.

    Best source first: raw ticks, then the 1s rollup's first receive time, then the
    dashboard loader's frame (Nubra writes no canonical key; the loader rebuilds it).
    """
    folder, pattern, raw_pattern, _, _ = FEEDS[broker]
    raw = ROOT_DIR / folder / raw_pattern.format(d=day) if raw_pattern else None
    if raw is not None and raw.exists():
        df = pd.read_csv(raw, usecols=lambda c: c in {"canonical_symbol", "received_at_ist", "ltp"})
        df = df.rename(columns={"received_at_ist": "recv"})
    else:
        roll = ROOT_DIR / folder / pattern.format(d=day)
        if not roll.exists():
            return pd.DataFrame()
        cols = pd.read_csv(roll, nrows=0).columns
        recv_col = "first_received_at_ist" if "first_received_at_ist" in cols else ("received_at_ist" if "received_at_ist" in cols else None)
        if recv_col and {"canonical_symbol", "ltp"} <= set(cols):
            df = pd.read_csv(roll, usecols=["canonical_symbol", recv_col, "ltp"]).rename(columns={recv_col: "recv"})
        elif loaded is not None and not loaded.empty:
            df = loaded[["canonical_symbol", "received_at_ist", "ltp"]].rename(columns={"received_at_ist": "recv"})
        else:
            return pd.DataFrame(columns=["canonical_symbol", "recv", "ltp"])
    df = df[df["canonical_symbol"].isin(symbols)].dropna(subset=["ltp", "recv"])
    df["recv"] = pd.to_datetime(df["recv"], errors="coerce", utc=True, format="ISO8601")
    df = df.dropna(subset=["recv"]).sort_values(["canonical_symbol", "recv"])
    changed = df["ltp"].ne(df.groupby("canonical_symbol")["ltp"].shift())
    return df[changed]


def lead_lag(q_events: pd.DataFrame, b_events: pd.DataFrame, window_s: float = 5.0) -> pd.Series:
    """For each Quantick change to price P, nearest time the other feed changed to P (within +/- window)."""
    out = []
    for sym, qg in q_events.groupby("canonical_symbol"):
        bg = b_events[b_events["canonical_symbol"] == sym]
        if bg.empty:
            continue
        by_price = {p: g["recv"].values for p, g in bg.groupby("ltp")}
        for t, p in zip(qg["recv"].values, qg["ltp"].values):
            times = by_price.get(p)
            if times is None:
                continue
            d = (t - times) / np.timedelta64(1, "ms")
            d = d[np.abs(d) <= window_s * 1000]
            if len(d):
                out.append(d[np.argmin(np.abs(d))])
    return pd.Series(out, dtype=float)


def raw_inter_arrival(broker: str, day: str, symbols: set[str]) -> float | None:
    folder, _, raw_pattern, _, _ = FEEDS[broker]
    raw = ROOT_DIR / folder / raw_pattern.format(d=day) if raw_pattern else None
    if raw is None or not raw.exists():
        return None
    df = pd.read_csv(raw, usecols=lambda c: c in {"canonical_symbol", "received_at_ist"})
    df = df[df["canonical_symbol"].isin(symbols)]
    df["recv"] = pd.to_datetime(df["received_at_ist"], errors="coerce", utc=True, format="ISO8601")
    gaps = df.sort_values("recv").groupby("canonical_symbol")["recv"].diff().dt.total_seconds() * 1000
    return float(gaps.median()) if gaps.notna().any() else None


def grid_ltp(df: pd.DataFrame, clock: str = "received_at_ist") -> pd.DataFrame:
    """canonical x second grid of last LTP, forward-filled.

    clock="received_at_ist": what each feed was showing at that wall-clock second.
    clock="timestamp_ist": the price each feed attributes to that exchange second
    (correctness, independent of how late it arrived).
    """
    when = pd.to_datetime(df[clock], errors="coerce", utc=True, format="ISO8601")
    g = df.assign(sec=when.dt.floor("s")).dropna(subset=["sec"]).groupby(["canonical_symbol", "sec"])["ltp"].last()
    frames = []
    for sym, s in g.groupby(level=0):
        s = s.droplevel(0)
        idx = pd.date_range(s.index.min(), s.index.max(), freq="s")
        frames.append(s.reindex(idx).ffill().rename_axis("sec").reset_index().assign(canonical_symbol=sym))
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=["sec", "ltp", "canonical_symbol"])


def minute_bars(df: pd.DataFrame) -> pd.DataFrame:
    m = df.assign(minute=df["timestamp_ist"].dt.floor("min"))
    hi = m[["h", "ltp"]].max(axis=1)
    lo = m[["l", "ltp"]].min(axis=1)
    m = m.assign(hi=hi, lo=lo)
    return m.groupby(["canonical_symbol", "minute"]).agg(open=("ltp", "first"), high=("hi", "max"), low=("lo", "min"), close=("ltp", "last")).reset_index()


def run_live(day: str, out_dir: Path) -> str:
    feeds = {b: load_feed(b, day) for b in FEEDS}
    feeds = {b: df for b, df in feeds.items() if not df.empty}
    q = feeds.get("quantick")
    lines = [f"## Live feed comparison - {day}\n"]
    present = ", ".join(f"{b} ({df['canonical_symbol'].nunique()} symbols)" for b, df in feeds.items())
    lines.append(f"Feeds with a 1s capture for {day}: {present or 'none'}\n")
    if q is None:
        lines.append("No Quantick capture for this date.\n")
        return "\n".join(lines)

    # Quantick on its own, every segment it captured.
    q_raw_path = ROOT_DIR / "Quantick" / f"quantick_live_ticks_{day}.csv"
    if q_raw_path.exists():
        raw = pd.read_csv(q_raw_path, usecols=["exchange_segment", "canonical_symbol", "received_at_ist", "listed_at_ist"])
        raw["recv"] = pd.to_datetime(raw["received_at_ist"], utc=True, errors="coerce", format="ISO8601")
        raw["listed"] = pd.to_datetime(raw["listed_at_ist"], utc=True, errors="coerce", format="ISO8601")
        raw = raw.sort_values("recv")
        raw["gap_ms"] = raw.groupby("canonical_symbol")["recv"].diff().dt.total_seconds() * 1000
        raw["delay_ms"] = (raw["recv"] - raw["listed"]).dt.total_seconds() * 1000
        seg = raw.groupby("exchange_segment").agg(
            symbols=("canonical_symbol", "nunique"), ticks=("canonical_symbol", "size"),
            gap_p50_ms=("gap_ms", "median"), gap_p90_ms=("gap_ms", lambda s: s.quantile(0.9)),
            delay_p50_ms=("delay_ms", "median"), delay_p95_ms=("delay_ms", lambda s: s.quantile(0.95))).reset_index()
        seg.to_csv(out_dir / "live_quantick_by_segment.csv", index=False)
        lines.append("### Quantick by segment (raw ticks)\n")
        lines.append("`gap` = time between updates of the same contract. A p50 pinned at ~1000 ms means a 1-second snapshot feed, not tick-by-tick. "
                     "`delay` = receive time minus exchange timestamp (1-second resolution, so ~500 ms of it is rounding).\n")
        lines.append(md_table(seg.round(0)))

    # Arrival on this Mac minus last-traded time (LTT), on the first arrival of each new trade.
    # A new trade = cumulative volume went up for that contract; ticks without a new trade keep
    # the old LTT and would otherwise count quiet time as "delay".
    ltt_rows, ltt_frames, lines_excluded = [], [], []
    for name, path, cols in (
            ("quantick", ROOT_DIR / "Quantick" / f"quantick_live_ticks_{day}.csv",
             {"sym": "canonical_symbol", "ltt": "listed_at_ist", "recv": "received_at_ist", "vol": "volume", "type": "instrument_type"}),
            ("nubra", ROOT_DIR / "Nubra" / f"nubra_live_ticks_{day}.csv",
             {"sym": "symbol", "ltt": "datetime_IST", "recv": "received_at_IST", "vol": "cumulative_volume", "type": "instrument_type"})):
        if not path.exists():
            continue
        t = pd.read_csv(path, usecols=list(cols.values())).rename(columns={v: k for k, v in cols.items()})
        if name == "quantick":
            und = pd.read_csv(path, usecols=["underlying", "exchange_segment"])
            t = t[(und["underlying"] == "NIFTY").values & und["exchange_segment"].astype(str).str.startswith("NFO").values]
        t["ltt"] = pd.to_datetime(t["ltt"], format="ISO8601")
        t["recv"] = pd.to_datetime(t["recv"], format="ISO8601")
        t = t.dropna(subset=["vol"]).sort_values(["sym", "recv"])
        g = t.groupby("sym")
        prev_vol, prev_ltt, first = g["vol"].shift(), g["ltt"].shift(), g.cumcount() == 0
        new_trade = (t["vol"] > prev_vol) & ~first
        # Does this feed's timestamp behave like an LTT? (it should not move without a trade)
        moved_without_trade = ((t["ltt"] != prev_ltt) & ~new_trade & ~first & (t["vol"] == prev_vol)).sum()
        same_vol = ((t["vol"] == prev_vol) & ~first).sum()
        tr = t[new_trade]
        tr = tr[(tr["ltt"].dt.time >= SESSION[0]) & (tr["ltt"].dt.time < SESSION[1])].copy()
        tr["delay_s"] = (tr["recv"] - tr["ltt"]).dt.total_seconds()
        tr["kind"] = np.where(tr["type"].astype(str).eq("FUT"), "NIFTY futures", "NIFTY options")
        tr["feed"] = name
        # Drop 15-minute blocks where OUR capture path was behind (median delay > 10 s):
        # a tunnel drop or collector backlog, not the vendor. Report which blocks were dropped.
        block = tr["recv"].dt.floor("15min")
        bad = tr.groupby(block)["delay_s"].median()
        bad = bad[bad > 10].index
        if len(bad):
            lines_excluded.append(f"{name}: " + ", ".join(b.strftime("%H:%M") for b in bad))
        tr = tr[~block.isin(bad)]
        ltt_frames.append(tr)
        for kind, k in tr.groupby("kind"):
            d = k["delay_s"]
            ltt_rows.append({"feed": name, "contracts": f"{kind} ({k['sym'].nunique()})", "new trades": f"{len(k):,}",
                             "p50 s": round(d.median(), 2), "p90 s": round(d.quantile(0.9), 2), "p95 s": round(d.quantile(0.95), 2),
                             "p99 s": round(d.quantile(0.99), 2), "max s": round(d.max(), 1),
                             "<=1 s": _pct(100 * (d <= 1).mean()), "<=2 s": _pct(100 * (d <= 2).mean()),
                             "timestamp moved without a trade": _pct(100 * moved_without_trade / max(same_vol, 1))})
    if ltt_rows:
        ltt_table = pd.DataFrame(ltt_rows)
        ltt_table.to_csv(out_dir / "live_arrival_minus_ltt.csv", index=False)
        pd.concat(ltt_frames)[["feed", "sym", "kind", "ltt", "recv", "delay_s"]].to_csv(out_dir / "live_arrival_minus_ltt_detail.csv", index=False)
        lines.append("### Arrival on this Mac minus last-traded time (LTT)\n")
        lines.append("Per new trade (cumulative volume rose), the first time it reached this machine minus the trade's LTT. "
                     "Quantick's LTT has 1-second resolution (true delay is 0-1 s lower). `timestamp moved without a trade` "
                     "near 0% means the feed's timestamp really is an LTT; a high value means it is packet time, and the "
                     "delay then understates time-since-trade.\n")
        lines.append(md_table(ltt_table))
        if lines_excluded:
            lines.append("Excluded 15-minute blocks (our capture path > 10 s behind): " + "; ".join(lines_excluded) + "\n")

    # Lag through the day per raw feed: a transport incident (tunnel drop, collector
    # backlog) shows up here as a block of 15-minute buckets with minutes of lag.
    lag_frames = []
    for name, path, exch_col, recv_col in (
            ("quantick", ROOT_DIR / "Quantick" / f"quantick_live_ticks_{day}.csv", "listed_at_ist", "received_at_ist"),
            ("nubra", ROOT_DIR / "Nubra" / f"nubra_live_ticks_{day}.csv", "datetime_IST", "received_at_IST")):
        if not path.exists():
            continue
        raw = pd.read_csv(path, usecols=[exch_col, recv_col])
        recv = pd.to_datetime(raw[recv_col], format="ISO8601")
        lag = (recv - pd.to_datetime(raw[exch_col], format="ISO8601")).dt.total_seconds()
        in_session = (recv.dt.time >= SESSION[0]) & (recv.dt.time <= SESSION[1])
        g = lag[in_session].groupby(recv[in_session].dt.floor("15min"))
        lag_frames.append(pd.DataFrame({f"{name} ticks": g.size(), f"{name} lag p50 s": g.median().round(2),
                                        f"{name} lag p95 s": g.quantile(0.95).round(2)}))
    if lag_frames:
        lag_table = pd.concat(lag_frames, axis=1)
        lag_table.index = lag_table.index.strftime("%H:%M")
        lag_table = lag_table.rename_axis("15 min from").reset_index()
        lag_table.to_csv(out_dir / "live_lag_by_time.csv", index=False)
        lines.append("### Receive minus exchange time, through the day\n")
        lines.append("Steady ~1 s is normal for a 1-second snapshot feed. Blocks of minutes mean our capture path fell behind "
                     "(tunnel drop or collector backlog), not the vendor; exclude them when quoting latency.\n")
        lines.append(md_table(lag_table))

    rows = []
    q_syms = set(q["canonical_symbol"])
    for broker, df in feeds.items():
        if broker == "quantick":
            continue
        common = q_syms & set(df["canonical_symbol"])
        if not common:
            rows.append({"broker": broker, "common_symbols": 0})
            continue
        qa, ba = q[q["canonical_symbol"].isin(common)], df[df["canonical_symbol"].isin(common)]
        j = grid_ltp(qa).merge(grid_ltp(ba), on=["canonical_symbol", "sec"], suffixes=("_q", "_b")).dropna()
        diff = (j["ltp_q"] - j["ltp_b"]).abs()
        jx = grid_ltp(qa, "timestamp_ist").merge(grid_ltp(ba, "timestamp_ist"), on=["canonical_symbol", "sec"], suffixes=("_q", "_b")).dropna()
        diff_x = (jx["ltp_q"] - jx["ltp_b"]).abs()
        backlog = lambda d: (pd.to_datetime(d["received_at_ist"], utc=True, format="ISO8601") - pd.to_datetime(d["timestamp_ist"], utc=True, format="ISO8601")).dt.total_seconds()
        qb, bb = minute_bars(qa), minute_bars(ba)
        mb = qb.merge(bb, on=["canonical_symbol", "minute"], suffixes=("_q", "_b"))
        rq, rb = mb["high_q"] - mb["low_q"], mb["high_b"] - mb["low_b"]
        valid = rb > 0
        qe, be = load_change_events("quantick", day, common, qa), load_change_events(broker, day, common, ba)
        ll = lead_lag(qe, be)
        minutes = lambda d: d.groupby("canonical_symbol")["timestamp_ist"].agg(lambda s: (s.max() - s.min()).total_seconds() / 60).sum()
        rows.append({
            "broker": broker,
            "common_symbols": len(common),
            "seconds with an update / sym-min: Quantick": round(len(qa) / max(minutes(qa), 1), 1),
            "seconds with an update / sym-min: other": round(len(ba) / max(minutes(ba), 1), 1),
            "raw inter-arrival p50 ms: Quantick": _num(raw_inter_arrival("quantick", day, common), 0),
            "raw inter-arrival p50 ms: other": _num(raw_inter_arrival(broker, day, common), 0),
            # Not comparable across feeds: some stamp packet time, others last-trade time.
            "recv - feed timestamp p50 ms: Quantick": _num(qa["receive_delay_ms"].median(), 0),
            "recv - feed timestamp p50 ms: other": _num(ba["receive_delay_ms"].median(), 0),
            # Backlog: a collector that cannot keep up shows minutes here even for liquid contracts.
            "recv - feed timestamp p10 s (backlog floor): Quantick": _num(backlog(qa).quantile(0.1), 1),
            "recv - feed timestamp p10 s (backlog floor): other": _num(backlog(ba).quantile(0.1), 1),
            "LTP equal, same exchange second": _pct(100 * (diff_x < 1e-9).mean()),
            "LTP within 1 tick, same exchange second": _pct(100 * (diff_x <= 0.05 + 1e-9).mean()),
            "LTP equal, same receive second": _pct(100 * (diff < 1e-9).mean()),
            "LTP within 1 tick, same receive second": _pct(100 * (diff <= 0.05 + 1e-9).mean()),
            "median abs LTP diff (receive second)": _num(diff.median(), 2),
            "lead/lag matches": len(ll),
            "quantick later by p50 ms": _num(ll.median(), 0),
            "p25..p75 ms": f"{_num(ll.quantile(0.25), 0)}..{_num(ll.quantile(0.75), 0)}" if len(ll) else "-",
            "quantick first": _pct(100 * (ll < 0).mean()) if len(ll) else "-",
            "1m range quantick/other p50": _num((rq[valid] / rb[valid]).median(), 2),
        })
    comp = pd.DataFrame(rows)
    comp.to_csv(out_dir / "live_vs_brokers.csv", index=False)
    lines.append("### Quantick vs each broker (shared NIFTY contracts)\n")
    lines.append("`recv - feed timestamp` is NOT a cross-feed latency: Quantick stamps packet time, Arrow/Dhan stamp last-trade time. "
                 "Use lead/lag for who-is-faster.\n")
    lines.append("Lead/lag: for each Quantick price change, the nearest time (+/-5 s) the other feed changed to the same price. "
                 "Positive = Quantick delivered it later. `1m range` < 1 means Quantick's 1-minute high-low is narrower, i.e. it misses intra-second extremes.\n")
    lines.append(md_table(comp.T.reset_index().rename(columns={"index": "metric"}) if not comp.empty else comp))
    return "\n".join(lines)


# ======================================================================== eod


def _get(url: str, referer: str | None = None) -> bytes | None:
    req = urllib.request.Request(url, headers={"User-Agent": UA, **({"Referer": referer} if referer else {})})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.read()
    except Exception:
        return None


def load_bhavcopies(day: dt.date, cache: Path) -> dict[str, pd.DataFrame]:
    cache.mkdir(parents=True, exist_ok=True)
    ymd, dmy = day.strftime("%Y%m%d"), day.strftime("%d%m%Y")
    sources = {
        "nse_fo": (f"https://nsearchives.nseindia.com/content/fo/BhavCopy_NSE_FO_0_0_0_{ymd}_F_0000.csv.zip", None),
        "nse_cm": (f"https://nsearchives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_{ymd}_F_0000.csv.zip", None),
        "nse_idx": (f"https://nsearchives.nseindia.com/content/indices/ind_close_all_{dmy}.csv", None),
        "bse_fo": (f"https://www.bseindia.com/download/BhavCopy/Derivative/BhavCopy_BSE_FO_0_0_0_{ymd}_F_0000.CSV", "https://www.bseindia.com/"),
        "bse_cm": (f"https://www.bseindia.com/download/BhavCopy/Equity/BhavCopy_BSE_CM_0_0_0_{ymd}_F_0000.CSV", "https://www.bseindia.com/"),
    }
    out = {}
    for key, (url, ref) in sources.items():
        path = cache / f"{key}_{ymd}.csv"
        if not path.exists():
            blob = _get(url, ref)
            if not blob or blob[:1] == b"<":
                continue
            if url.endswith(".zip"):
                with zipfile.ZipFile(io.BytesIO(blob)) as z:
                    blob = z.read(z.namelist()[0])
            path.write_bytes(blob)
        out[key] = pd.read_csv(path)
        out[key].columns = [c.strip() for c in out[key].columns]
    return out


def quantick_client():
    from qtickclient import QTickClient
    from quantick_websocket import SshTunnel, load_credentials

    creds = load_credentials(ROOT_DIR)
    base = str(creds["base_url"]).rstrip("/")
    tunnel = None
    if creds["ssh_host"]:
        port = int(creds["tunnel_port"] or 18080)
        host, _, rport = base.split("://", 1)[1].split("/", 1)[0].partition(":")
        tunnel = SshTunnel(str(creds["ssh_host"]), port, host, int(rport or 80))
        tunnel.start()
        base = f"http://127.0.0.1:{port}"
    client = QTickClient(base)
    client.login(str(creds["user"]), str(creds["password"]))
    return client, tunnel


def universe_for(day: str) -> tuple[list[str], dict[str, str], dict[str, str]]:
    path = BASE_DIR / f"quantick_universe_{day}.json"
    if not path.exists():
        raise SystemExit(f"{path.name} not found: run the live capture for {day} first (it records the exact contracts).")
    u = json.loads(path.read_text())
    return u["symbols"], u["root_by_symbol"], u.get("canonical_by_symbol") or {}


def bhav_row(sym: str, root: str, meta: dict[str, Any], bhav: dict[str, pd.DataFrame]) -> tuple[str, pd.Series | None]:
    exch = sym.split(":", 1)[0] if ":" in sym else "NFO"
    itype = str(meta.get("instrument_type") or "").upper()
    expiry = str(meta.get("expiry") or "")[:10]
    if sym.endswith("-IN"):
        name = {"NIFTY": "Nifty 50", "BANKNIFTY": "Nifty Bank"}.get(root)
        if name and "nse_idx" in bhav:
            df = bhav["nse_idx"]
            hit = df[df["Index Name"].str.strip().str.lower() == name.lower()]
            if not hit.empty:
                r = hit.iloc[0]
                return "nse_idx", pd.Series({"OpnPric": r["Open Index Value"], "HghPric": r["High Index Value"],
                                             "LwPric": r["Low Index Value"], "ClsPric": r["Closing Index Value"]})
        if root == "SENSEX" and "bse_fo" in bhav:
            df = bhav["bse_fo"]
            hit = df[df["TckrSymb"] == "SENSEX"]["UndrlygPric"].dropna()
            if not hit.empty:
                return "bse_fo(underlying)", pd.Series({"ClsPric": float(hit.iloc[0])})
        return "", None
    key = {"NFO": "nse_fo", "BFO": "bse_fo", "NSE": "nse_cm", "BSE": "bse_cm"}.get(exch)
    if not key or key not in bhav:
        return "", None
    df = bhav[key]
    if itype == "EQ":
        hit = df[df["TckrSymb"] == root]
        if key == "nse_cm":
            hit = hit[hit["SctySrs"] == "EQ"]
    elif itype in {"CE", "PE"}:
        hit = df[(df["TckrSymb"] == root) & (df["XpryDt"] == expiry) & (df["OptnTp"] == itype)
                 & (pd.to_numeric(df["StrkPric"], errors="coerce").round(2) == round(float(meta.get("strike") or 0), 2))]
    elif itype == "FUT":
        hit = df[(df["TckrSymb"] == root) & (df["XpryDt"] == expiry) & (df["FinInstrmTp"].isin(["IDF", "STF"]))]
    else:
        return key, None
    return key, (hit.iloc[0] if not hit.empty else None)


def key_is_nse_fo(source: str) -> bool:
    return source == "nse_fo"


def run_eod(day: str, out_dir: Path) -> str:
    symbols, root_by_symbol, canonical_by_symbol = universe_for(day)
    d = dt.date.fromisoformat(day)
    bhav = load_bhavcopies(d, BASE_DIR / "bhavcopy_cache")
    lines = [f"## EOD comparison - {day}\n", f"Official files found: {', '.join(sorted(bhav)) or 'none (published ~18:00-20:00 IST)'}\n"]

    client, tunnel = quantick_client()
    try:
        meta_by_symbol: dict[str, dict] = {}
        day_rows, minute_frames = [], []
        for sym in symbols:
            try:
                dres = client.historical(symbol=sym, from_date=day, to_date=day, interval="day", oi=True)
                mres = client.historical(symbol=sym, from_date=f"{day} 09:15:00", to_date=f"{day} 23:59:00", interval="minute")
            except Exception as exc:
                day_rows.append({"symbol": sym, "error": f"{type(exc).__name__}: {str(exc)[:120]}"})
                continue
            meta = {"instrument_type": None, "expiry": None, "strike": None}
            ts = str(dres.get("tradingsymbol") or "")
            seg = str(dres.get("segment") or "")
            meta_by_symbol[sym] = meta
            canon = canonical_by_symbol.get(sym, sym)
            # Reconstruct type/expiry/strike from the canonical key the live capture wrote.
            if seg.endswith("_OPT") and canon[-2:] in {"CE", "PE"}:
                root = root_by_symbol.get(sym, "")
                body = canon[len(root):]
                meta.update(instrument_type=canon[-2:], expiry=f"{body[:4]}-{body[4:6]}-{body[6:8]}", strike=float(body[8:-2]))
            elif seg.endswith("_FUT"):
                meta["instrument_type"] = "FUT"
            elif seg.endswith("_EQ"):
                meta["instrument_type"] = "EQ"
            candles = dres.get("candles") or []
            c = candles[-1] if candles else {}
            row = {"symbol": sym, "canonical": canon, "segment": seg, "tradingsymbol": ts,
                   "q_open": c.get("open"), "q_high": c.get("high"), "q_low": c.get("low"), "q_close": c.get("close"),
                   "q_volume": c.get("volume"), "q_oi": c.get("oi")}
            day_rows.append(row)
            mc = pd.DataFrame(mres.get("candles") or [])
            if not mc.empty:
                mc["minute"] = pd.to_datetime(mc["date"], format="ISO8601").dt.tz_localize(IST)
                mc["canonical_symbol"] = canon
                minute_frames.append(mc[["canonical_symbol", "minute", "open", "high", "low", "close", "volume"]])
            time.sleep(0.05)
    finally:
        client.close()
        if tunnel:
            tunnel.stop()

    qday = pd.DataFrame(day_rows)
    # Futures expiry for the bhavcopy lookup comes from the tradingsymbol (e.g. NIFTY26OCTFUT) + the FO file.
    out = []
    for _, r in qday.iterrows():
        if "error" in r and isinstance(r.get("error"), str):
            out.append({"symbol": r["symbol"], "source": "", "note": r["error"]})
            continue
        sym = r["symbol"]
        meta = dict(meta_by_symbol.get(sym, {}))
        root = root_by_symbol.get(sym, "")
        if meta.get("instrument_type") == "FUT":
            key = "nse_fo" if sym.startswith("NFO:") else "bse_fo"
            if key in bhav:
                hit = bhav[key][bhav[key]["FinInstrmNm"] == r["tradingsymbol"]]
                meta["expiry"] = hit["XpryDt"].iloc[0] if not hit.empty else None
        src, b = bhav_row(sym, root, meta, bhav)
        rec = {"symbol": sym, "canonical": r["canonical"], "source": src or "-"}
        for k in ("open", "high", "low", "close"):
            rec[f"q_{k}"] = r[f"q_{k}"]
        if b is None:
            rec["note"] = "no official row (MCX has no free bhavcopy)" if sym.startswith("MCX:") else "no official row"
            out.append(rec)
            continue
        f = lambda k: pd.to_numeric(b.get(k), errors="coerce")
        rec.update(x_open=f("OpnPric"), x_high=f("HghPric"), x_low=f("LwPric"), x_close=f("ClsPric"), x_last=f("LastPric"), x_settle=f("SttlmPric"))
        for k in ("open", "high", "low"):
            # None = the official file has no value to check against (e.g. SENSEX close only).
            rec[f"{k}_ok"] = None if pd.isna(rec[f"x_{k}"]) else bool(abs(float(rec[f"q_{k}"] or 0) - rec[f"x_{k}"]) < 0.0051)
        qc = float(rec["q_close"] or 0)
        rec["close_matches"] = ",".join(n for n in ("close", "last", "settle") if pd.notna(rec[f"x_{n}"]) and abs(qc - rec[f"x_{n}"]) < 0.0051) or "none"
        vol, lot = f("TtlTradgVol"), f("NewBrdLotQty")
        # NSE F&O reports volume in contracts, BSE F&O and both cash files in shares/units.
        rec["x_volume_qty"] = vol * lot if key_is_nse_fo(src) and pd.notna(vol) and pd.notna(lot) and lot > 0 else vol
        rec["x_oi"] = f("OpnIntrst")
        qv = pd.to_numeric(r["q_volume"], errors="coerce")
        if pd.notna(rec["x_volume_qty"]) and rec["x_volume_qty"] > 0 and pd.notna(qv):
            rec["volume_ratio"] = round(qv / rec["x_volume_qty"], 4)
        qoi, xoi = pd.to_numeric(r["q_oi"], errors="coerce"), f("OpnIntrst")
        if pd.notna(qoi) and pd.notna(xoi) and xoi > 0:
            rec["oi_ratio"] = round(qoi / xoi, 4)
        out.append(rec)
    official = pd.DataFrame(out)
    official.to_csv(out_dir / "eod_quantick_vs_official.csv", index=False)

    lines.append("### Quantick daily candle vs official bhavcopy\n")
    have = official[official.get("source", pd.Series(dtype=str)).fillna("-").ne("-")] if not official.empty else official
    if not have.empty and "open_ok" in have:
        summ = []
        for src, g in have.groupby("source"):
            ok = lambda col: _pct(100 * g[col].dropna().astype(float).mean()) if col in g and g[col].notna().any() else "-"
            summ.append({
                "source": src, "contracts": len(g),
                "open match": ok("open_ok"), "high match": ok("high_ok"), "low match": ok("low_ok"),
                "close = ": g["close_matches"].value_counts().head(3).to_dict() if "close_matches" in g else "-",
                "volume ratio p50": _num(g["volume_ratio"].median(), 4) if "volume_ratio" in g else "-",
                "OI ratio p50": _num(g["oi_ratio"].median(), 4) if "oi_ratio" in g else "-",
            })
        lines.append(md_table(pd.DataFrame(summ)))
        bad = have[have[["open_ok", "high_ok", "low_ok"]].eq(False).any(axis=1)]
        if not bad.empty:
            lines.append(f"\n{len(bad)} contracts with an O/H/L mismatch (first 15):\n")
            cols = [c for c in ["canonical", "q_open", "x_open", "q_high", "x_high", "q_low", "x_low", "q_close", "x_close", "x_last"] if c in bad]
            lines.append(md_table(bad[cols].head(15)))
    else:
        lines.append("No official rows matched.\n")
    missing = official[official.get("note").notna()] if "note" in official else pd.DataFrame()
    if not missing.empty:
        lines.append(f"\nNot checked against an official file: {len(missing)} ({missing['note'].value_counts().to_dict()})\n")

    # ---- historical minute candles vs live captures (Quantick's own + every other broker)
    qmin = pd.concat(minute_frames, ignore_index=True) if minute_frames else pd.DataFrame()
    if not qmin.empty:
        qmin.to_csv(out_dir / "eod_quantick_minute_candles.csv", index=False)
        seg_of = dict(zip(qday.get("canonical", []), qday.get("segment", [])))
        cov = qmin.assign(segment=qmin["canonical_symbol"].map(seg_of)).groupby("segment").agg(
            contracts=("canonical_symbol", "nunique"),
            first_minute=("minute", lambda s: s.min().strftime("%H:%M")),
            last_minute=("minute", lambda s: s.max().strftime("%H:%M")),
            minutes_per_contract=("minute", lambda s: round(len(s) / max(1, 1), 0)),
        ).reset_index()
        cov["minutes_per_contract"] = (qmin.assign(segment=qmin["canonical_symbol"].map(seg_of)).groupby("segment").size()
                                       / cov.set_index("segment")["contracts"]).round(0).values
        cov.to_csv(out_dir / "eod_minute_coverage.csv", index=False)
        lines.append("\n### Quantick historical minute-candle coverage\n")
        lines.append("NSE/BSE trade 09:15-15:30 (last candle 15:29); MCX 09:00-23:30. A last candle before 15:29 means the "
                     "day candle is missing the close; one after 15:29 means post-close packets were folded into the history.\n")
        lines.append(md_table(cov))
    rows = []
    for broker in FEEDS:
        live = load_feed(broker, day, end=MCX_END)
        if live.empty or qmin.empty:
            continue
        live = live[live["canonical_symbol"].isin(set(qmin["canonical_symbol"]))]
        if live.empty:
            continue
        lb = minute_bars(live)
        # Ignore the first/last partial minute of each capture.
        per_sym = lb.groupby("canonical_symbol")["minute"]
        lb = lb[lb["minute"].ne(per_sym.transform("min")) & lb["minute"].ne(per_sym.transform("max"))]
        j = qmin.merge(lb, on=["canonical_symbol", "minute"], suffixes=("_hist", "_live"))
        if j.empty:
            continue
        tick = 0.05 + 1e-9
        for label, g in [("all", j)] + ([("NIFTY F&O", j[j["canonical_symbol"].str.startswith("NIFTY2")])] if broker == "quantick" else []):
            if g.empty:
                continue
            rows.append({
                "live feed": broker, "subset": label, "symbols": g["canonical_symbol"].nunique(), "minutes": len(g),
                "close equal": _pct(100 * ((g["close_hist"] - g["close_live"]).abs() < 1e-9).mean()),
                "close within 1 tick": _pct(100 * ((g["close_hist"] - g["close_live"]).abs() <= tick).mean()),
                "high equal": _pct(100 * ((g["high_hist"] - g["high_live"]).abs() < 1e-9).mean()),
                "low equal": _pct(100 * ((g["low_hist"] - g["low_live"]).abs() < 1e-9).mean()),
                "hist range wider than live": _pct(100 * (((g["high_hist"] - g["low_hist"]) - (g["high_live"] - g["low_live"])) > tick).mean()),
                "median abs close diff": _num((g["close_hist"] - g["close_live"]).abs().median(), 2),
            })
    mins = pd.DataFrame(rows)
    mins.to_csv(out_dir / "eod_minute_hist_vs_live.csv", index=False)
    lines.append("\n### Quantick historical minute candles vs live captures\n")
    lines.append("`quantick` row = Quantick's historical API vs its own live feed (internal consistency). Other rows = Quantick's history vs that broker's live feed. "
                 "`hist range wider than live` is high when the live feed is a coalesced snapshot that misses intra-minute extremes.\n")
    lines.append(md_table(mins))

    # ---- day OHLC on the last live tick vs the historical day candle (all segments incl. MCX)
    raw_path = BASE_DIR / f"quantick_live_ticks_{day}.csv"
    if raw_path.exists() and not qday.empty and "q_open" in qday:
        raw = pd.read_csv(raw_path, usecols=["canonical_symbol", "exchange_segment", "received_at_ist", "ltp", "open_price", "high_price", "low_price", "volume", "open_interest"])
        last = raw.sort_values("received_at_ist").groupby("canonical_symbol").last().reset_index()
        j = qday.merge(last, left_on="canonical", right_on="canonical_symbol", how="inner")
        res = []
        for seg, g in j.groupby("exchange_segment"):
            res.append({"segment": seg, "contracts": len(g),
                        "open equal": _pct(100 * ((g["q_open"] - g["open_price"]).abs() < 1e-9).mean()),
                        "high equal": _pct(100 * ((g["q_high"] - g["high_price"]).abs() < 1e-9).mean()),
                        "low equal": _pct(100 * ((g["q_low"] - g["low_price"]).abs() < 1e-9).mean()),
                        "close = last live LTP": _pct(100 * ((g["q_close"] - g["ltp"]).abs() < 1e-9).mean())})
        lines.append("\n### Quantick day candle vs its own last live tick (day OHLC)\n")
        lines.append("MCX trades until 23:30; if the capture stopped at 15:32, MCX highs/lows after that are expected to differ.\n")
        lines.append(md_table(pd.DataFrame(res)))

        # ---- the LIVE feed's final tick vs the official file: covers every exchange, not just NIFTY
        if not official.empty and "x_close" in official:
            jo = last.merge(official, left_on="canonical_symbol", right_on="canonical", how="inner")
            jo = jo[jo["source"].fillna("-").ne("-")]
            res = []
            eq = lambda a, b: _pct(100 * ((jo_g[a] - jo_g[b]).abs() < 0.0051)[jo_g[b].notna()].mean()) if jo_g[b].notna().any() else "-"
            for seg, jo_g in jo.groupby("exchange_segment"):
                vol_x = pd.to_numeric(jo_g.get("x_volume_qty"), errors="coerce")
                res.append({"segment": seg, "contracts": len(jo_g),
                            "day open": eq("open_price", "x_open"), "day high": eq("high_price", "x_high"), "day low": eq("low_price", "x_low"),
                            "LTP = official last": eq("ltp", "x_last"), "LTP = official close": eq("ltp", "x_close"),
                            "volume ratio p50": _num((jo_g["volume"] / vol_x).median(), 4) if vol_x.notna().any() else "-",
                            "OI ratio p50": _num((jo_g["open_interest"] / jo_g["x_oi"]).median(), 4) if "x_oi" in jo_g and jo_g["x_oi"].notna().any() else "-"})
            lines.append("\n### Quantick LIVE feed (final captured tick) vs official bhavcopy\n")
            lines.append("Checks the live stream itself on every exchange. Day open/high/low come from the tick's day OHLC. "
                         "If the capture stopped before the close, the last LTP/volume are not final; run it to 16:05 for cash.\n")
            lines.append(md_table(pd.DataFrame(res)))
    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("mode", choices=["live", "eod", "all"])
    ap.add_argument("--date", default=dt.datetime.now(IST).date().isoformat())
    args = ap.parse_args()
    out_dir = BASE_DIR / "reports" / args.date
    out_dir.mkdir(parents=True, exist_ok=True)
    parts = [f"# Quantick feed check - {args.date}\n"]
    if args.mode in {"live", "all"}:
        parts.append(run_live(args.date, out_dir))
    if args.mode in {"eod", "all"}:
        parts.append(run_eod(args.date, out_dir))
    report = "\n\n".join(parts)
    name = "summary.md" if args.mode == "all" else f"summary_{args.mode}.md"
    (out_dir / name).write_text(report)
    print(report)
    print(f"\nWritten: {out_dir / name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

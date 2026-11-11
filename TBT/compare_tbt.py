#!/usr/bin/env python3
"""Quantick vs a reference feed with exchange timestamps (TBT stream or Alphabots feed), NIFTY options + futures, live ticks only.

    python TBT/compare_tbt.py --date 2026-10-21 --ref alphabots

TBT records carry exch_last_ns (exchange time, UTC ns, PTP-disciplined box), so it is the
reference for "when did it happen". Sections, written to Quantick/reports/<date>/tbt_*.csv
and summary_tbt.md:

  1. Same trade, both feeds: match (contract, cumulative volume). Arrival of each feed minus
     the exchange time TBT gives for that volume; and Quantick minus TBT arrival.
  2. Candle close vs receive (ms): trades whose exchange time is in the candle's last second,
     arrival after the :00 close, per feed, on the same matched trades.
  3. Feed's own arrival minus its own timestamp on new trades (Quantick LTT, TBT exch_last_ns).
  4. Price agreement on a 1-second receive grid; update cadence.
  5. EOD vs live (NSE bhavcopy), if published: day open/high/low/last/volume per feed.

Arrivals are compared on one clock only if both collectors ran on the same machine; the
frame log's host is noted in the report. Otherwise compare each feed against exchange time.
"""
from __future__ import annotations

import argparse
import datetime as dt
import sys
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
sys.path.insert(0, str(ROOT / "Quantick"))
from compare_feeds import md_table, load_bhavcopies  # noqa: E402

IST = "Asia/Kolkata"
SESSION = ("09:15", "15:30")


def _t(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, format="ISO8601", utc=True).dt.tz_convert(IST)


def in_session(ts: pd.Series) -> pd.Series:
    hm = ts.dt.strftime("%H:%M")
    return (hm >= SESSION[0]) & (hm < SESSION[1])


def pct(x) -> str:
    return "-" if x is None or pd.isna(x) else f"{x:.1f}%"


def dist(v: pd.Series, unit: float = 1.0, nd: int = 0) -> dict:
    v = v.dropna() * unit
    if v.empty:
        return {}
    r = lambda x: round(float(x), nd) if nd else int(round(float(x)))
    return {"p50": r(v.median()), "p75": r(v.quantile(.75)), "p90": r(v.quantile(.9)),
            "p95": r(v.quantile(.95)), "p99": r(v.quantile(.99))}


REFS = {"tbt": ("TBT", HERE / "tbt_live_ticks_{d}.csv", ["window", "replay"]),
        "alphabots": ("Alphabots", ROOT / "Alphabots" / "alphabots_live_ticks_{d}.csv", ["tick"])}
REF_LABEL = "TBT"


def load(day: str, ref: str = "tbt"):
    """Chunked, cached loader (feedlib): a full Alphabots day is ~10 GB of CSV on an 8 GB Mac."""
    import feedlib as fl
    q = fl.load_ticks("quantick", day).rename(columns={"ts": "ltt"}).dropna(subset=["volume"])
    b = fl.load_ticks(ref, day).rename(columns={"ts": "exch"}).dropna(subset=["volume"])
    for d in (q, b):
        d["canonical_symbol"] = d["canonical_symbol"].astype(str)
    return q, b


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--date", required=True)
    ap.add_argument("--ref", default="tbt", choices=sorted(REFS), help="reference feed with exchange-time stamps")
    a = ap.parse_args()
    day, ref = a.date, a.ref
    global REF_LABEL
    REF_LABEL = REFS[ref][0]
    out = ROOT / "Quantick" / "reports" / day
    out.mkdir(parents=True, exist_ok=True)
    sys.path.insert(0, str(HERE))
    import feedlib as fl
    q, b = load(day, ref)
    off_ms = fl.clock_offset_ms(day) or 0.0
    OFF = pd.Timedelta(milliseconds=off_ms)
    common = set(q.canonical_symbol) & set(b.canonical_symbol)
    q, b = q[q.canonical_symbol.isin(common)], b[b.canonical_symbol.isin(common)]
    lines = [f"# Quantick vs {REF_LABEL} - {day}\n", f"Common NIFTY F&O contracts: {len(common)}\n",
             f"Clock: this Mac ran {off_ms:+.0f} ms behind NTP (median of logs/clock-offset-{day}.log); every "
             "'arrival - exchange/own time' number below includes that correction. Quantick-minus-reference numbers need none.\n"]
    kind = lambda s: np.where(s.str.endswith("FUT"), "futures", "options")

    # ---- 0. align cumulative volume: a feed can start its day count later (Alphabots' NIFTY future ran
    # ~28,400 below Quantick all day on 2026-10-21), which would pair different trades. Per contract,
    # the offset is the most common (Quantick - reference) volume at the same moment.
    qs = q.sort_values("recv")[["canonical_symbol", "recv", "volume"]]
    bs = b.sort_values("recv")[["canonical_symbol", "recv", "volume"]]
    near = pd.merge_asof(qs, bs, on="recv", by="canonical_symbol", suffixes=("_q", "_b"), tolerance=pd.Timedelta("200ms"), direction="nearest").dropna()
    vdiff = (near.volume_q - near.volume_b).groupby(near.canonical_symbol)
    offsets = vdiff.agg(lambda v: v.mode().iloc[0] if len(v) else 0)
    # An offset that changes over the day means one feed keeps missing (or adding) volume: matching
    # by volume would pair different trades, so those contracts sit out the same-trade sections.
    def _drift(v):  # median gap in the last fifth of the day minus the first fifth; jitter averages out
        n = max(1, len(v) // 5)
        return abs(v.iloc[-n:].median() - v.iloc[:n].median())
    drift = (near.assign(d=near.volume_q - near.volume_b)).groupby("canonical_symbol").d.agg(_drift)
    drifting = sorted(drift[drift > 1000].index)
    q = q.assign(volume=q.volume - q.canonical_symbol.map(offsets).fillna(0))
    shifted = offsets[(offsets != 0) & ~offsets.index.isin(drifting)]
    if drifting:
        d0 = near[near.canonical_symbol.isin(drifting)]
        lines.append(f"Volume drift: {len(drifting)} contract(s) whose cumulative volume diverges between the feeds during the day "
                     f"({', '.join(drifting[:5])}; Quantick - {REF_LABEL} went from {int((d0.volume_q - d0.volume_b).iloc[:50].median()):+,} "
                     f"to {int((d0.volume_q - d0.volume_b).iloc[-50:].median()):+,}). Excluded from sections 1-2 (see section 3 and EOD).\n")
    lines.append(f"Volume alignment: {len(shifted)} contract(s) had a constant cumulative-volume offset vs the reference, removed before matching"
                 + (f" (e.g. {', '.join(f'{k} {int(v):+,}' for k, v in shifted.head(3).items())})" if len(shifted) else "") + ".\n")

    # ---- 0b. incident minutes: a connection outage makes a feed deliver a backlog late; that is not its
    # normal speed. Minutes where either feed's median new-trade lag exceeds 3 s are excluded and listed.
    def trade_lag(d, tcol):
        d = d.sort_values(["canonical_symbol", "recv"])
        nt = d[d.volume > d.groupby("canonical_symbol").volume.shift()]
        return ((nt.recv + OFF - nt[tcol]).dt.total_seconds()).groupby(nt.recv.dt.floor("min")).median()
    lag_q, lag_b = trade_lag(q, "ltt"), trade_lag(b, "exch")
    bad_q, bad_b = set(lag_q[lag_q > 3].index), set(lag_b[lag_b > 3].index)
    bad = bad_q | bad_b
    fmt = lambda ms: ", ".join(sorted(t.strftime("%H:%M") for t in ms)) or "none"
    lines.append(f"Incident minutes excluded (median lag > 3 s): Quantick: {fmt(bad_q)}; {REF_LABEL}: {fmt(bad_b)}.\n")
    q = q[~q.recv.dt.floor("min").isin(bad)]
    b = b[~b.recv.dt.floor("min").isin(bad)]

    # ---- 1. same trade, matched by (contract, cumulative volume)
    qf = q.sort_values("recv").groupby(["canonical_symbol", "volume"]).agg(q_recv=("recv", "first"), q_ltt=("ltt", "first"))
    bf = b.sort_values("recv").groupby(["canonical_symbol", "volume"]).agg(b_recv=("recv", "first"), exch=("exch", "first"))
    j = qf.join(bf, how="inner").reset_index()
    j = j[~j.canonical_symbol.isin(drifting)]
    j = j[in_session(j.exch) & ((j.q_recv - j.b_recv).dt.total_seconds().abs() < 60)]
    j["kind"] = kind(j.canonical_symbol)
    rows = []
    for k, g in j.groupby("kind"):
        for label, v in ((f"{REF_LABEL} arrival - exchange time", g.b_recv + OFF - g.exch), ("Quantick arrival - exchange time", g.q_recv + OFF - g.exch),
                         (f"Quantick arrival - {REF_LABEL} arrival", g.q_recv - g.b_recv)):
            rows.append({"contracts": k, "measure (ms)": label, "same trades": f"{len(g):,}", **dist(v.dt.total_seconds(), 1000)})
        rows.append({"contracts": k, "measure (ms)": f"{REF_LABEL} first", "same trades": pct(100 * (g.b_recv < g.q_recv).mean())})
    t1 = pd.DataFrame(rows)
    t1.to_csv(out / f"{ref}_same_trade_ms.csv", index=False)
    lines += ["## 1. Same trade in both feeds (ms)\n",
              "Matched by contract + cumulative volume. Exchange time = the reference feed's exchange timestamp (ns).\n", md_table(t1)]

    # ---- 2. candle close vs receive, trades in the candle's last second (exchange time)
    last = j[j.exch.dt.second == 59].copy()
    close_at = last.exch.dt.floor("min") + pd.Timedelta(minutes=1)
    rows = []
    for k, g in last.groupby("kind"):
        ca = close_at[g.index]
        for feed, col in ((REF_LABEL, "b_recv"), ("Quantick", "q_recv")):
            v = (g[col] + OFF - ca).dt.total_seconds() * 1000
            rows.append({"contracts": k, "feed": feed, "trades at :59": f"{len(g):,}", **dist(v),
                         "within 500 ms": pct(100 * (v <= 500).mean()), "within 1000 ms": pct(100 * (v <= 1000).mean())})
    t2 = pd.DataFrame(rows)
    t2.to_csv(out / f"{ref}_candle_close_ms.csv", index=False)
    lines += ["\n## 2. Candle close vs received (ms after the 1-minute close)\n",
              "Same matched trades, exchange time in the candle's final second (:59).\n", md_table(t2)]

    # ---- 3. own arrival minus own timestamp on new trades
    rows = []
    for feed, d, tcol in (("Quantick", q, "ltt"), (REF_LABEL, b, "exch")):
        d = d.sort_values(["canonical_symbol", "recv"])
        nt = d[d.volume > d.groupby("canonical_symbol").volume.shift()]
        nt = nt[in_session(nt[tcol])]
        for k, g in nt.groupby(kind(nt.canonical_symbol)):
            rows.append({"feed": feed, "contracts": k, "new trades": f"{len(g):,}", **dist((g.recv + OFF - g[tcol]).dt.total_seconds(), 1000)})
    t3 = pd.DataFrame(rows)
    t3.to_csv(out / f"{ref}_own_timestamp_ms.csv", index=False)
    lines += ["\n## 3. Each feed's arrival minus its own timestamp, new trades (ms)\n",
              "Quantick's LTT has 1-second resolution (true delay 0-1000 ms lower); the reference feed's is ns.\n", md_table(t3)]

    # ---- 4. price agreement + cadence
    grid = lambda d: d.assign(sec=d.recv.dt.floor("s")).groupby(["canonical_symbol", "sec"]).ltp.last()
    g = pd.concat([grid(q).rename("q"), grid(b).rename("b")], axis=1).groupby(level=0).ffill().dropna()
    diff = (g.q - g.b).abs()
    mins = lambda d: d.groupby("canonical_symbol").recv.agg(lambda s: (s.max() - s.min()).total_seconds() / 60).sum()
    t4 = pd.DataFrame([{"LTP equal (same receive second)": pct(100 * (diff < 1e-9).mean()),
                        "within 1 tick": pct(100 * (diff <= 0.0501).mean()), "median abs diff": round(diff.median(), 2),
                        "updates/contract-min Quantick": round(len(q) / max(mins(q), 1), 1),
                        f"updates/contract-min {REF_LABEL}": round(len(b) / max(mins(b), 1), 1)}])
    t4.to_csv(out / f"{ref}_agreement.csv", index=False)
    lines += ["\n## 4. Price agreement and cadence\n", md_table(t4)]

    # ---- 4b. the reference feed's own finished 1-minute bars: arrival after the close
    bars = ROOT / "Alphabots" / f"alphabots_bars_1m_{day}.csv"
    if ref == "alphabots" and bars.exists():
        bb = pd.read_csv(bars)
        bb = bb[bb.canonical_symbol.isin(common) & (pd.to_datetime(bb.bar_close_ist, format="ISO8601").dt.strftime("%H:%M") <= "15:30")]
        # first arrival of each bar; leave out the incident minutes (reliability is reported on its own)
        bb = bb.sort_values("recv_ns").drop_duplicates(["canonical_symbol", "bar_start_ist"])
        arr = pd.to_datetime(bb.received_at_ist, format="ISO8601", utc=True).dt.tz_convert(IST).dt.floor("min")
        bb = bb[~arr.isin(bad)]
        bb = bb.assign(arrival_after_close_ms=bb.arrival_after_close_ms + off_ms)
        rows = [{"contracts": k, "bars": f"{len(g):,}", **dist(g.arrival_after_close_ms), "within 500 ms": pct(100 * (g.arrival_after_close_ms <= 500).mean()),
                 "within 1000 ms": pct(100 * (g.arrival_after_close_ms <= 1000).mean())}
                for k, g in bb.groupby(kind(bb.canonical_symbol))]
        t4b = pd.DataFrame(rows)
        t4b.to_csv(out / "alphabots_bar_arrival_ms.csv", index=False)
        lines += ["\n## 4b. Alphabots' own finished 1-minute bars: arrival on this Mac after the candle close (ms; clock-corrected, incident minutes excluded)\n", md_table(t4b)]

    # ---- 5. EOD vs live
    bh = load_bhavcopies(dt.date.fromisoformat(day), ROOT / "Quantick" / "bhavcopy_cache")
    if "nse_fo" in bh:
        import subprocess
        res = subprocess.run([sys.executable, str(ROOT / "Quantick" / "eod_live_vs_official.py"), "--date", day], capture_output=True, text=True)
        lines.append("\n## 5. EOD vs live (NSE bhavcopy)\n\n```\n" + res.stdout.strip() + "\n```\n")
    else:
        lines.append("\n## 5. EOD vs live\n\nNSE F&O bhavcopy not published yet; re-run later.\n")
    (out / f"summary_{ref}.md").write_text("\n".join(lines))
    print("\n".join(lines))
    return 0


if __name__ == "__main__":
    sys.exit(main())

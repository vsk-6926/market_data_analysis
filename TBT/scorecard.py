#!/usr/bin/env python3
"""Who is better and faster on this Mac: Quantick vs Alphabots, one table, with a previous day alongside.

    python TBT/scorecard.py --date 2026-10-22 --vs 2026-10-21

Reads the CSVs the day's reports already wrote (compare_tbt.py --ref alphabots, candle_reconcile.py,
eod_live_vs_official.py) from Quantick/reports/<date>/ and writes scorecard.md there.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "Quantick"))
from compare_feeds import md_table  # noqa: E402


def rd(day: str, name: str) -> pd.DataFrame | None:
    p = ROOT / "Quantick" / "reports" / day / name
    return pd.read_csv(p) if p.exists() else None


def pick(df, **eq):
    if df is None:
        return None
    for k, v in eq.items():
        df = df[df[k] == v]
    return df.iloc[0] if len(df) else None


def metrics(day: str) -> dict:
    m: dict = {}
    st = rd(day, "alphabots_same_trade_ms.csv")
    if st is not None:
        col = "measure (ms)"
        a = pick(st, contracts="options", **{col: "Alphabots arrival - exchange time"})
        q = pick(st, contracts="options", **{col: "Quantick arrival - exchange time"})
        d = pick(st, contracts="options", **{col: "Quantick arrival - Alphabots arrival"})
        f = pick(st, contracts="options", **{col: "Alphabots first"})
        if a is not None:
            m["Same trade: arrival after exchange time, p50 (ms)"] = (a.p50, q.p50, "lower")
            m["Same trade: arrival after exchange time, p95 (ms)"] = (a.p95, q.p95, "lower")
            m["Same trade: Quantick later than Alphabots, p50 / p95 (ms)"] = (f"{d.p50:+.0f} / {d.p95:+.0f}", "", "info")
            m["Same trade: delivered first (% of trades)"] = (f["same trades"], f"{100 - float(str(f['same trades']).rstrip('%')):.1f}%", "higher%")
            m["Same trades matched"] = (a["same trades"], "", "info")
    cc = rd(day, "alphabots_candle_close_ms.csv")
    if cc is not None:
        a, q = pick(cc, contracts="options", feed="Alphabots"), pick(cc, contracts="options", feed="Quantick")
        if a is not None and q is not None:
            m["Candle: minute's last trades in, after close, p50 (ms)"] = (a.p50, q.p50, "lower")
    bar = rd(day, "alphabots_bar_arrival_ms.csv")
    if bar is not None:
        b = pick(bar, contracts="options")
        qc = rd(day, "quantick_candle_changes.csv")
        if b is not None and qc is not None and len(qc):
            m["Candle: vendor's own 1-min candle final after close, p50 (ms)"] = (b.p50, round(qc.final_s.median() * 1000), "lower")
            m["Candle: vendor's own 1-min candle final after close, p95 (ms)"] = (b.p95, round(qc.final_s.quantile(.95) * 1000), "lower")
            m["Candle: Quantick API candle changed after first returned"] = ("never (pushed when final)", f"{100 * qc.changed_after_first_post_close.mean():.0f}% of minutes", "info")
    cl = rd(day, "candles_vs_actual.csv")
    if cl is not None:
        for src, label in (("alphabots_ticks", "Alphabots"), ("quantick_live", "Quantick")):
            r = pick(cl, contracts="options", **{"source vs actual": src})
            if r is not None:
                m.setdefault("Accuracy: candle from received ticks, close = actual", ["", "", "higher%"])
                m.setdefault("Accuracy: candle from received ticks, volume = actual", ["", "", "higher%"])
                i = 0 if label == "Alphabots" else 1
                m["Accuracy: candle from received ticks, close = actual"][i] = r["close equal"]
                m["Accuracy: candle from received ticks, volume = actual"][i] = r["volume equal"]
        h = pick(cl, contracts="options", **{"source vs actual": "quantick_hist"})
        if h is not None:
            m["Accuracy: Quantick historical candle, close / volume = actual"] = ("-", f"{h['close equal']} / {h['volume equal']}", "info")
    ag = rd(day, "alphabots_agreement.csv")
    if ag is not None:
        r = ag.iloc[0]
        m["Completeness: updates per contract per minute"] = (r["updates/contract-min Alphabots"], r["updates/contract-min Quantick"], "higher")
    eod = rd(day, "eod_live_vs_official.csv")
    if eod is not None:
        for kind in ("futures", "options"):
            a = eod[(eod.feed == "Alphabots") & eod.contracts.str.startswith(kind)]
            q = eod[(eod.feed == "Quantick") & eod.contracts.str.startswith(kind)]
            if len(a) and len(q):
                m[f"EOD: {kind} volume vs NSE official (closer to 1 = better)"] = (a.iloc[0]["volume live/official p50"], q.iloc[0]["volume live/official p50"], "closer1")
    summ = ROOT / "Quantick" / "reports" / day / "summary_alphabots.md"
    if summ.exists():
        line = next((l for l in summ.read_text().splitlines() if l.startswith("Incident minutes excluded")), "")
        if line:
            parts = dict(seg.split(":", 1) for seg in line.split(": ", 1)[1].rstrip(".").split("; ") if ":" in seg)
            cnt = lambda v: 0 if v.strip() == "none" else len([x for x in v.split(",") if x.strip()])
            m["Reliability: minutes lost to connection incidents (lag > 3 s)"] = (cnt(parts.get("Alphabots", "none")), cnt(parts.get("Quantick", "none")), "lower")
    for k in list(m):
        m[k] = tuple(m[k])
    return m


def winner(a, q, rule) -> str:
    try:
        fa, fq = float(str(a).rstrip("%")), float(str(q).rstrip("%"))
    except (TypeError, ValueError):
        return ""
    if rule == "lower":
        return "Alphabots" if fa < fq else "Quantick" if fq < fa else "tie"
    if rule in ("higher", "higher%"):
        return "Alphabots" if fa > fq else "Quantick" if fq > fa else "tie"
    if rule == "closer1":
        if abs(abs(fa - 1) - abs(fq - 1)) < 0.001:
            return "tie"
        return "Alphabots" if abs(fa - 1) < abs(fq - 1) else "Quantick"
    return ""


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True)
    ap.add_argument("--vs", default=None)
    a = ap.parse_args()
    cur, prev = metrics(a.date), metrics(a.vs) if a.vs else {}
    rows = []
    for k, (av, qv, rule) in cur.items():
        row = {"measure (NIFTY options)": k, "Alphabots": av, "Quantick": qv, "better": winner(av, qv, rule)}
        if a.vs:
            pv = prev.get(k)
            row[f"{a.vs}: Alphabots / Quantick"] = f"{pv[0]} / {pv[1]}" if pv else "-"
        rows.append(row)
    t = pd.DataFrame(rows)
    wins = t.better.value_counts()
    head = (f"# Who is better and faster on this Mac: Quantick vs Alphabots, {a.date}\n\n"
            f"Measures won: Alphabots {int(wins.get('Alphabots', 0))}, Quantick {int(wins.get('Quantick', 0))}"
            + (f", tie {int(wins.get('tie', 0))}" if wins.get("tie") else "") + "\n")
    text = head + "\n" + md_table(t)
    (ROOT / "Quantick" / "reports" / a.date / "scorecard.md").write_text(text)
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

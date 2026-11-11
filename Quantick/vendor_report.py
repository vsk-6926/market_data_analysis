#!/usr/bin/env python3
"""Quantick-only findings to send to Quantick (no data from any other feed).

    python Quantick/vendor_report.py --date 2026-10-21

Sources, all Quantick's own:
  quantick_candle_latency_<date>.csv  every historical(interval="minute") response we got while polling
                                      each minute's candle around the close (and the 2026-10-16 run)
  reports/<date>/candles_all_sources.csv  rows quantick_hist (historical API) and quantick_live (candle
                                      built from the ticks Quantick's websocket sent us)

Writes reports/<date>/quantick_vendor_report.html and quantick_vendor_message.txt.
"""
from __future__ import annotations

import argparse
import html
from pathlib import Path

import pandas as pd

BASE = Path(__file__).resolve().parent
IST = "Asia/Kolkata"
VALS = ["open", "high", "low", "close", "volume"]


def polls(day: str) -> pd.DataFrame:
    p = BASE / f"quantick_candle_latency_{day}.csv"
    d = pd.read_csv(p)
    if "source" not in d.columns:
        d.insert(0, "source", "quantick"); d.insert(1, "role", d["symbol"])
    d = d[d.source == "quantick"].copy()
    for c in ("sent_at", "received_at"):
        d[c] = pd.to_datetime(d[c], format="ISO8601", utc=True).dt.tz_convert(IST)
    d["minute"] = pd.to_datetime(d.minute, format="ISO8601").dt.tz_localize(IST)
    d = d[(d.minute.dt.strftime("%H:%M") >= "09:15") & (d.minute.dt.strftime("%H:%M") <= "15:29")]
    return d.sort_values("received_at")


def changes(d: pd.DataFrame) -> pd.DataFrame:
    """Per symbol-minute: was it served while forming, did it change after the close, when final."""
    out = []
    for (sym, minute), g in d.groupby(["symbol", "minute"]):
        close_at = minute + pd.Timedelta(minutes=1)
        after = g[g.sent_at >= close_at]
        if after.empty or (after.sent_at.iloc[0] - close_at).total_seconds() > 1.0:
            continue  # we were not polling at this boundary
        final = g.iloc[-1]
        same = (g[VALS].values == final[VALS].values).all(axis=1)
        diffs = [i for i, ok in enumerate(same) if not ok]
        settled = g.iloc[diffs[-1] + 1] if diffs else after.iloc[0]
        if settled.received_at < after.received_at.iloc[0]:
            settled = after.iloc[0]
        first = after.iloc[0]
        versions = g[g.sent_at >= close_at].drop_duplicates(subset=VALS)
        out.append({"symbol": sym, "minute": minute,
                    "served_while_forming": bool((g.sent_at < close_at).any()),
                    "changed_after_first_post_close": bool((after[VALS].nunique() > 1).any()),
                    "versions_after_close": len(versions),
                    "first_s": (first.received_at - close_at).total_seconds(),
                    "final_s": (settled.received_at - close_at).total_seconds(),
                    "close_first": first.close, "close_final": final.close,
                    "vol_first": first.volume, "vol_final": final.volume})
    return pd.DataFrame(out)


def table(df: pd.DataFrame) -> str:
    head = "".join(f"<th>{html.escape(str(c))}</th>" for c in df.columns)
    body = "".join("<tr>" + "".join(f"<td>{html.escape(str(v))}</td>" for v in r) + "</tr>" for r in df.itertuples(index=False))
    return f"<table><tr>{head}</tr>{body}</table>"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True)
    day = ap.parse_args().date
    out = BASE / "reports" / day
    d = polls(day)
    ch = changes(d)
    ch.to_csv(out / "quantick_candle_changes.csv", index=False)
    per = []
    for sym, g in ch.groupby("symbol"):
        per.append({"symbol": sym, "minutes checked": len(g),
                    "candle changed after it was first returned post-close": f"{100 * g.changed_after_first_post_close.mean():.0f}%",
                    "first returned (s after close, p50)": round(g.first_s.median(), 2),
                    "stopped changing (s after close, p50 / p95 / max)": f"{g.final_s.median():.2f} / {g.final_s.quantile(.95):.2f} / {g.final_s.max():.2f}",
                    "current minute returned while still forming": f"{100 * g.served_while_forming.mean():.0f}%"})
    per = pd.DataFrame(per)
    allm = ch
    headline = (f"{len(allm)} one-minute candles polled every 0.5 s after the close: the candle changed after it was first returned in "
                f"{100 * allm.changed_after_first_post_close.mean():.0f}% of minutes; it stopped changing {allm.final_s.median():.1f} s after "
                f"the close at the median (p95 {allm.final_s.quantile(.95):.1f} s, max {allm.final_s.max():.1f} s).")

    # earlier evidence: the 2026-10-16 run (98 minutes x 5 symbols)
    prev = []
    for pd_day in ("2026-10-21", "2026-10-16"):
        if pd_day == day or not (BASE / f"quantick_candle_latency_{pd_day}.csv").exists():
            continue
        pc = changes(polls(pd_day))
        if not pc.empty:
            prev.append(f"Same check on {pd_day} ({len(pc)} candles): changed after first returned in "
                        f"{100 * pc.changed_after_first_post_close.mean():.0f}% of minutes; final {pc.final_s.median():.1f} s after the close "
                        f"(p95 {pc.final_s.quantile(.95):.1f} s, max {pc.final_s.max():.1f} s).")
    prev_line = " ".join(prev)

    # one example: the candle with the most versions after the close
    traded = allm[~allm.symbol.str.endswith("-IN") & (allm.vol_final > 0)]
    pool = (traded if not traded.empty else allm).copy()
    # clearest evidence: the biggest change between the first post-close version and the final one
    pool["chg"] = (pool.close_final - pool.close_first).abs() / pool.close_final.abs().clip(lower=1e-9) + \
                  (pool.vol_final - pool.vol_first).abs() / pool.vol_final.abs().clip(lower=1)
    ex = pool.sort_values(["chg", "versions_after_close"], ascending=False).iloc[0]
    g = d[(d.symbol == ex.symbol) & (d.minute == ex.minute)]
    close_at = ex.minute + pd.Timedelta(minutes=1)
    timeline = g.drop_duplicates(subset=VALS).assign(
        **{"returned at (s after the minute closed)": lambda x: ((x.received_at - close_at).dt.total_seconds()).round(2)})
    timeline = timeline[["returned at (s after the minute closed)"] + VALS]

    # historical candle vs a candle built from Quantick's own websocket ticks (same vendor)
    cs = out / "candles_all_sources.csv"
    cons, cons_ex = pd.DataFrame(), pd.DataFrame()
    if cs.exists():
        c = pd.read_csv(cs)
        h = c[c.source == "quantick_hist"].set_index(["canonical_symbol", "minute"])
        l = c[c.source == "quantick_live"].set_index(["canonical_symbol", "minute"])
        j = h.join(l, lsuffix="_hist", rsuffix="_ws", how="inner")
        if not j.empty:
            cons = pd.DataFrame([{"minutes compared": f"{len(j):,}",
                                  **{f"{f} equal": f"{100 * ((j[f + '_hist'] - j[f + '_ws']).abs() < 0.0051).mean():.1f}%" for f in ("open", "high", "low", "close")},
                                  "volume equal": f"{100 * (j.volume_hist == j.volume_ws).mean():.1f}%"}])
            e = j.reset_index()
            e = e[e.minute.str.contains(" 11:00:")].head(3)
            cons_ex = pd.DataFrame([{"contract": r.canonical_symbol, "minute": r.minute[11:16], "version": v,
                                     **{f: getattr(r, f"{f}_{k}") for f in VALS}}
                                    for r in e.itertuples() for v, k in (("historical API", "hist"), ("from websocket ticks", "ws"))])

    # ---- summary table in the same shape as the internal comparison, Quantick column only.
    # Rows 'vs reference' use NSE tick-by-tick exchange data as the reference (named generically on purpose).
    def pct_row(csv_name, source, kind="options"):
        f = out / csv_name
        if not f.exists():
            return None
        t = pd.read_csv(f)
        t = t[(t.iloc[:, 0] == source) & (t.contracts == kind)]
        return t.iloc[0] if not t.empty else None
    hist, live = pct_row("candles_vs_actual.csv", "quantick_hist"), pct_row("candles_vs_actual.csv", "quantick_live")
    tick = None
    cc = out / "alphabots_candle_close_ms.csv"
    if cc.exists():
        t = pd.read_csv(cc)
        t = t[(t.feed == "Quantick") & (t.contracts == "options")]
        tick = t.iloc[0] if not t.empty else None
    fs = allm.final_s
    m = lambda r: (f"close {r['close equal']}, open {r['open equal']}, high {r['high equal']}, low {r['low equal']}, volume {r['volume equal']}"
                   if r is not None else "-")
    summary_rows = [
        ("A. Quantick 1-minute candle (historical API)", ""),
        ("How it is obtained", "REST request historical(interval='minute'); no candle on the websocket"),
        ("First returned after the minute closes (p50)", f"{allm.first_s.median():.2f} s, but not final"),
        ("Final, stops changing (p50)", f"{fs.median():.2f} s"),
        ("Final, slow end (p95 / max)", f"{fs.quantile(.95):.2f} s / {fs.max():.2f} s"),
        ("Changed after it was first returned post-close", f"{100 * allm.changed_after_first_post_close.mean():.0f}% of minutes"),
        ("Matches the NSE tick-by-tick reference candle", m(hist)),
        ("B. Candle built from Quantick websocket ticks", ""),
        ("Minute's last trades reach us after the close (p50 / p95)",
         f"{tick['p50'] / 1000:+.2f} s / {tick['p95'] / 1000:+.2f} s" if tick is not None else "-"),
        ("Matches the NSE tick-by-tick reference candle", m(live)),
    ]
    summary = pd.DataFrame(summary_rows, columns=["", "Quantick"])

    questions = [
        "Is it expected that a 1-minute candle returned by historical(interval='minute') keeps changing for several seconds after the minute has closed? What is the guaranteed time after which a minute candle is final?",
        "The latest candle in a response is returned while that minute is still forming. Can the API mark whether a candle is final (e.g. a flag), or return only closed minutes?",
        "Why does the historical candle differ from the ticks your websocket delivered for the same minute (open, high/low, volume)? Is the historical candle built from a different/complete trade source?",
        "Do you offer (or plan) a finished 1-minute candle on the websocket, pushed when the minute closes?",
    ]
    style = """<style>:root{--bg:#fff;--fg:#1d1d1f;--mut:#6e6e73;--line:#d9d9de}@media(prefers-color-scheme:dark){:root{--bg:#1c1c1e;--fg:#f2f2f7;--mut:#a1a1a6;--line:#3a3a3c}}
body{background:var(--bg);color:var(--fg);font:14px -apple-system,Segoe UI,sans-serif;margin:24px;max-width:1050px}table{border-collapse:collapse;margin:8px 0 18px}
th,td{border-bottom:1px solid var(--line);padding:6px 10px;text-align:left}th{color:var(--mut)}p,li{line-height:1.5}</style>"""
    page = (f"<!doctype html><html><head><meta charset='utf-8'><title>Quantick candle check {day}</title>{style}</head><body>"
            f"<h2>Quantick 1-minute candles: observations, {day}</h2><p>NIFTY options and futures, qtickclient 3.3.0. Times are arrival on our machine, clock-corrected. "
            f"The reference candle is built from NSE tick-by-tick exchange data.</p><h3>Summary</h3>{table(summary)}"
            f"<h3>1. Historical minute candle keeps changing after the minute closes</h3><p>{html.escape(headline)}</p><p>{html.escape(prev_line)}</p>{table(per)}"
            f"<p>Example: {html.escape(str(ex.symbol))}, minute {ex.minute:%H:%M}. Each distinct version returned by the API after the minute closed:</p>{table(timeline)}"
            + (f"<h3>2. Historical candle vs the ticks your websocket sent for the same minute</h3>{table(cons)}{table(cons_ex)}" if not cons.empty else "")
            + "<h3>Questions</h3><ol>" + "".join(f"<li>{html.escape(q)}</li>" for q in questions) + "</ol></body></html>")
    (out / "quantick_vendor_report.html").write_text(page)

    msg = [f"Hi Quantick team, observations from {day} (NIFTY options/futures, qtickclient 3.3.0; times measured on our machine, clock-corrected):", ""]
    for k, v in summary_rows:
        msg.append(f"{k}" if not v else f"  - {k}: {v}")
    msg += ["", "Details:",
           f"1. Minute candles change after the minute closes. {headline}" + (f" {prev_line}" if prev_line else ""),
           f"   Example {ex.symbol} {ex.minute:%H:%M}: close {ex.close_first} when first returned after the close, final {ex.close_final}; volume {int(ex.vol_first)} -> {int(ex.vol_final)}.",
           "2. The current minute is returned while still forming (it is the last candle in every response)."]
    if not cons.empty:
        r = cons.iloc[0]
        msg.append(f"3. Historical vs your websocket ticks for the same minute ({r['minutes compared']} minutes): open equal {r['open equal']}, "
                   f"high {r['high equal']}, low {r['low equal']}, close {r['close equal']}, volume {r['volume equal']}.")
    msg += ["", "Could you confirm whether this is expected:"] + [f"- {q}" for q in questions] + ["", "Details attached. Thanks."]
    (out / "quantick_vendor_message.txt").write_text("\n".join(msg))
    print("\n".join(msg))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

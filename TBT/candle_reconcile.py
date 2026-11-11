#!/usr/bin/env python3
"""1-minute candle reconciliation and tick-level field check: Quantick vs Alphabots vs "actual".

    python TBT/candle_reconcile.py --date 2026-10-21 [--no-history]

For every shared NIFTY contract and minute (e.g. 11:00:00-11:00:59) four candles:

  actual          the Alphabots feed's own finished 1-minute bar, built on its server from the full NSE
                  tick-by-tick feed (on 2026-10-21 its highs/lows were wider than a candle built from the
                  ticks it streams 17-19% of the time and narrower 0.2%: the streamed ticks skip trades)
  alphabots_ticks built from the ticks Alphabots streams to this Mac
  quantick_live built from the Quantick ticks received on this Mac (minute by Quantick's LTT)
  quantick_hist Quantick's historical 1-minute candle (fetched once after the close)

Each is compared with `actual`: open/high/low/close equal (to the paisa) and per-minute volume
equal. A tick-level check matches the same trade in both feeds (contract + cumulative volume)
and compares the price, the timestamp second (Quantick LTT vs exchange time) and OI.

Writes Quantick/reports/<date>/candles_*.csv, ticks_field_check.csv and summary_candles.md.
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(ROOT / "Quantick"))
import feedlib as fl  # noqa: E402
from compare_feeds import md_table  # noqa: E402

IST = fl.IST
SRC_ORDER = ["actual", "alphabots_ticks", "quantick_live", "quantick_hist"]


def candles_from_trades(tr: pd.DataFrame, label: str) -> pd.DataFrame:
    """OHLC from trades by the feed's own timestamp minute; per-minute volume from cumulative volume."""
    t = tr[(tr.ts.dt.strftime("%H:%M") >= "09:15") & (tr.ts.dt.strftime("%H:%M") <= "15:29")].copy()
    t["minute"] = t.ts.dt.floor("min")
    t = t.sort_values(["canonical_symbol", "ts", "recv"], kind="stable")
    g = t.groupby(["canonical_symbol", "minute"], observed=True)
    c = g.agg(open=("ltp", "first"), high=("ltp", "max"), low=("ltp", "min"), close=("ltp", "last"), vol_end=("volume", "max")).reset_index()
    c["volume"] = c.groupby("canonical_symbol", observed=True).vol_end.diff()
    c["source"] = label
    return c.drop(columns="vol_end")


def alphabots_bars(day: str, keep: set) -> pd.DataFrame:
    p = ROOT / "Alphabots" / f"alphabots_bars_1m_{day}.csv"
    if not p.exists():
        return pd.DataFrame()
    b = pd.read_csv(p)
    b = b[b.canonical_symbol.isin(keep) & (b.tf == "1m")]
    b["minute"] = pd.to_datetime(b.bar_start_ist, format="ISO8601", utc=True).dt.tz_convert(IST)
    b = b.sort_values(["canonical_symbol", "minute", "received_at_ist"]).drop_duplicates(["canonical_symbol", "minute"], keep="last")
    b["volume"] = b.groupby("canonical_symbol").volume.diff()  # the bar's volume is cumulative for the day
    b["source"] = "actual"
    return b[["canonical_symbol", "minute", "open", "high", "low", "close", "volume", "source"]]


def quantick_symbol(canon: str, fut_months: list[str]) -> str:
    if canon.endswith("FUT"):
        return {0: "NFO:NIFTY-I", 1: "NFO:NIFTY-II", 2: "NFO:NIFTY-III"}.get(fut_months.index(canon), "")
    body = canon[len("NIFTY"):]                          # 2026102723000CE
    return f"NIFTY{body[2:8]}{body[8:-2]}{body[-2:]}-O"   # NIFTY26102723000CE-O


def quantick_history(day: str, keep: set) -> pd.DataFrame:
    cache = ROOT / "Quantick" / f"quantick_hist_1m_{day}.csv"
    if cache.exists():
        h = pd.read_csv(cache)
    else:
        from compare_feeds import quantick_client
        futs = sorted([c for c in keep if c.endswith("FUT")], key=lambda c: pd.to_datetime(c[5:10], format="%y%b"))
        client, tunnel = quantick_client()
        rows = []
        try:
            for canon in sorted(keep):
                sym = quantick_symbol(canon, futs)
                if not sym:
                    continue
                try:
                    r = client.historical(symbol=sym, from_date=f"{day} 09:15:00", to_date=f"{day} 15:29:00", interval="minute")
                except Exception as exc:
                    print(f"  history {sym}: {type(exc).__name__}: {str(exc)[:100]}")
                    continue
                for c in r.get("candles") or []:
                    rows.append({"canonical_symbol": canon, "minute": c["date"], "open": c["open"], "high": c["high"],
                                 "low": c["low"], "close": c["close"], "volume": c["volume"]})
                time.sleep(0.4)  # ~2.5 req/s, far below what got the account blocked on 2026-10-16
        finally:
            client.close()
            if tunnel:
                tunnel.stop()
        h = pd.DataFrame(rows)
        h.to_csv(cache, index=False)
    if h.empty:
        return h
    h["minute"] = pd.to_datetime(h.minute, format="ISO8601").dt.tz_localize(IST)
    h["source"] = "quantick_hist"
    return h


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--date", required=True)
    ap.add_argument("--no-history", action="store_true", help="skip Quantick's historical candles")
    ap.add_argument("--examples", default="NIFTY26OCTFUT", help="comma list of contracts for the example table")
    ap.add_argument("--example-minute", default="11:00")
    a = ap.parse_args()
    day = a.date
    out = ROOT / "Quantick" / "reports" / day
    out.mkdir(parents=True, exist_ok=True)

    q, ab = fl.load_ticks("quantick", day), fl.load_ticks("alphabots", day)
    keep = set(q.canonical_symbol.astype(str)) & set(ab.canonical_symbol.astype(str))
    q, ab = q[q.canonical_symbol.isin(keep)], ab[ab.canonical_symbol.isin(keep)]
    qt, at = fl.new_trades(q), fl.new_trades(ab)
    frames = [alphabots_bars(day, keep), candles_from_trades(at, "alphabots_ticks"), candles_from_trades(qt, "quantick_live")]
    if not a.no_history:
        frames.append(quantick_history(day, keep))
    allc = pd.concat([f for f in frames if not f.empty], ignore_index=True)
    allc["canonical_symbol"] = allc["canonical_symbol"].astype(str)
    allc.to_csv(out / "candles_all_sources.csv", index=False)

    act = allc[allc.source == "actual"].set_index(["canonical_symbol", "minute"])
    rows, worst = [], []
    kind = lambda s: np.where(pd.Series(s).str.endswith("FUT"), "futures", "options")
    for src in SRC_ORDER[1:]:
        s = allc[allc.source == src].set_index(["canonical_symbol", "minute"])
        j = s.join(act, how="inner", lsuffix="_s", rsuffix="_a")
        if j.empty:
            continue
        j["kind"] = kind(j.index.get_level_values(0))
        for k, g in j.groupby("kind"):
            r = {"source vs actual": src, "contracts": k, "minutes": f"{len(g):,}"}
            for f in ("open", "high", "low", "close"):
                d = (g[f"{f}_s"] - g[f"{f}_a"]).abs()
                r[f"{f} equal"] = f"{100 * (d < 0.0051).mean():.1f}%"
            vol = g.dropna(subset=["volume_s", "volume_a"])
            r["volume equal"] = f"{100 * (vol.volume_s == vol.volume_a).mean():.1f}%" if len(vol) else "-"
            r["median |close diff| when unequal"] = round(float((g.close_s - g.close_a).abs()[(g.close_s - g.close_a).abs() >= 0.0051].median() or 0), 2)
            rows.append(r)
        w = j.assign(close_diff=(j.close_s - j.close_a).abs()).sort_values("close_diff", ascending=False).head(5).reset_index()
        w["source"] = src
        worst.append(w[["source", "canonical_symbol", "minute", "open_s", "open_a", "high_s", "high_a", "low_s", "low_a", "close_s", "close_a"]])
    summary = pd.DataFrame(rows)
    summary.to_csv(out / "candles_vs_actual.csv", index=False)

    # Example minute side by side, e.g. 11:00:00-11:00:59.
    ex_min = pd.Timestamp(f"{day} {a.example_minute}", tz=IST)
    atm = None
    if not at.empty:
        fut_close = act.reset_index()
        fut_close = fut_close[(fut_close.canonical_symbol.str.endswith("FUT")) & (fut_close.minute == ex_min)]
        if not fut_close.empty:
            atm = int(round(float(fut_close.close.iloc[0]) / 100.0) * 100)
    ex_syms = [s for s in a.examples.split(",") if s]
    if atm:
        opt = sorted(c for c in keep if c.endswith(f"{atm}CE"))
        ex_syms += opt[:1]
    ex = allc[allc.canonical_symbol.isin(ex_syms) & (allc.minute == ex_min)].copy()
    ex["source"] = pd.Categorical(ex.source, SRC_ORDER, ordered=True)
    ex = ex.sort_values(["canonical_symbol", "source"])[["canonical_symbol", "source", "open", "high", "low", "close", "volume"]]
    ex.to_csv(out / "candles_example.csv", index=False)

    # Tick-level: the same trade in both feeds.
    qf = qt.sort_values("recv").groupby(["canonical_symbol", "volume"], observed=True).agg(q_ltp=("ltp", "first"), q_ts=("ts", "first"), q_oi=("oi", "first"))
    af = at.sort_values("recv").groupby(["canonical_symbol", "volume"], observed=True).agg(a_ltp=("ltp", "first"), a_ts=("ts", "first"), a_oi=("oi", "first"))
    m = qf.join(af, how="inner").dropna(subset=["q_ltp", "a_ltp"]).reset_index()
    m = m[(m.a_ts.dt.strftime("%H:%M") >= "09:15") & (m.a_ts.dt.strftime("%H:%M") < "15:30")]
    sec_diff = (m.q_ts.dt.floor("s") - m.a_ts.dt.floor("s")).dt.total_seconds()
    oi_both = m.dropna(subset=["q_oi", "a_oi"])
    field = pd.DataFrame([{
        "same trades matched": f"{len(m):,}",
        "price equal": f"{100 * ((m.q_ltp - m.a_ltp).abs() < 0.0051).mean():.2f}%",
        "Quantick LTT second = exchange second": f"{100 * (sec_diff == 0).mean():.1f}%",
        "LTT 1 s later": f"{100 * (sec_diff == 1).mean():.1f}%",
        "LTT earlier": f"{100 * (sec_diff < 0).mean():.1f}%",
        "LTT >= 2 s later": f"{100 * (sec_diff >= 2).mean():.1f}%",
        "OI": "Alphabots sends no OI (always 0); Quantick does" if (m.a_oi.fillna(0) == 0).all() else
              f"{100 * (oi_both.q_oi == oi_both.a_oi).mean():.1f}% equal",
    }])
    field.to_csv(out / "ticks_field_check.csv", index=False)

    lines = [f"# 1-minute candles and tick fields: Quantick vs Alphabots vs actual - {day}\n",
             f"Shared NIFTY contracts: {len(keep)}. `actual` = the Alphabots feed's own 1-minute bar, built on its server from the full "
             "NSE tick-by-tick feed. `alphabots_ticks` / `quantick_live` = candles built from the ticks each feed delivered to this Mac.\n",
             "## Each source vs the actual candle\n", md_table(summary),
             f"\n## Example: {a.example_minute}:00-{a.example_minute}:59\n", md_table(ex),
             "\n## Biggest close mismatches per source\n", md_table(pd.concat(worst) if worst else pd.DataFrame()),
             "\n## Tick level: the same trade in both feeds\n",
             "Matched by contract + cumulative volume. Quantick's LTT has whole seconds; the exchange time is ns.\n", md_table(field)]
    (out / "summary_candles.md").write_text("\n".join(lines))
    print("\n".join(lines))
    return 0


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""Each feed's LIVE end-of-day values vs NSE's official F&O bhavcopy, NIFTY options + futures.

    python Quantick/eod_live_vs_official.py --date 2026-10-19

Quantick: last captured tick per contract (its ticks carry the exchange's day open/high/low).
TBT: its last KEYFRAME per contract (day values from the box).
Nubra: built from its orderbook ticks (first/max/min/last trade price, last cumulative volume;
the orderbook stream carries no OI). If the capture stopped before 15:30, last price and
volume are as of the stop, not the close.
"""
from __future__ import annotations

import argparse
import datetime as dt
import sys
from pathlib import Path

import numpy as np
import pandas as pd

BASE = Path(__file__).resolve().parent
ROOT = BASE.parent
sys.path.insert(0, str(BASE))
from compare_feeds import load_bhavcopies  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True)
    day = ap.parse_args().date
    b = load_bhavcopies(dt.date.fromisoformat(day), BASE / "bhavcopy_cache")
    if "nse_fo" not in b:
        print("NSE F&O bhavcopy not published yet")
        return 2
    fo = b["nse_fo"]
    fo = fo[(fo.TckrSymb == "NIFTY") & fo.FinInstrmTp.isin(["IDO", "IDF"])].copy()
    fo["key"] = np.where(fo.OptnTp.isin(["CE", "PE"]),
                         "NIFTY" + fo.XpryDt.str.replace("-", "") + pd.to_numeric(fo.StrkPric).fillna(0).astype(int).astype(str) + fo.OptnTp.fillna(""),
                         "NIFTY" + pd.to_datetime(fo.XpryDt).dt.strftime("%y%b").str.upper() + "FUT")
    fo = fo.set_index("key")
    fo["vol_qty"] = fo.TtlTradgVol * fo.NewBrdLotQty

    q = pd.read_csv(BASE / f"quantick_live_ticks_{day}.csv",
                    usecols=["canonical_symbol", "instrument_type", "received_at_ist", "ltp", "open_price", "high_price", "low_price", "volume", "open_interest"])
    q = q[q.instrument_type.isin(["CE", "PE", "FUT"])].sort_values("received_at_ist")
    stop = q.received_at_ist.max()[11:19]
    q = q.groupby("canonical_symbol").last().rename(columns={"open_price": "open", "high_price": "high", "low_price": "low", "ltp": "last", "open_interest": "oi"})

    feeds = [("Quantick", q)]
    nubra_path = ROOT / "Nubra" / f"nubra_live_ticks_{day}.csv"
    if nubra_path.exists():  # Nubra is not captured every day
        n = pd.read_csv(nubra_path, usecols=["symbol", "instrument_type", "expiry", "strike", "datetime_IST", "received_at_IST", "ltp",
                                             "cumulative_volume"]).dropna(subset=["ltp"])
        n["key"] = np.where(n.instrument_type.isin(["CE", "PE"]),
                            "NIFTY" + n.expiry.astype(str).str[:8] + pd.to_numeric(n.strike, errors="coerce").fillna(0).astype(int).astype(str) + n.instrument_type,
                            n.symbol.str.upper())
        n = n[n.datetime_IST.str[11:16] >= "09:15"].sort_values("received_at_IST")
        feeds.append(("Nubra", n.groupby("key").agg(open=("ltp", "first"), high=("ltp", "max"), low=("ltp", "min"), last=("ltp", "last"),
                                                    volume=("cumulative_volume", "last"))))
    tbt_path = ROOT / "TBT" / f"tbt_live_ticks_{day}.csv"
    if tbt_path.exists():
        # TBT KEYFRAMEs (every 30 s) carry the exchange's DAY open/high/low, last price and day volume.
        t = pd.read_csv(tbt_path, usecols=["canonical_symbol", "received_at_ist", "message_type", "open_price", "high_price",
                                           "low_price", "close_price", "volume"])
        t = t[t.message_type == "keyframe"].sort_values("received_at_ist").groupby("canonical_symbol").last()
        t = t.rename(columns={"open_price": "open", "high_price": "high", "low_price": "low", "close_price": "last"})
        feeds.append(("TBT", t))
    ab_path = ROOT / "Alphabots" / f"alphabots_live_ticks_{day}.csv"
    if ab_path.exists():
        # Alphabots ticks carry the exchange's day open/high/low (sess_*), LTP, day volume and OI.
        # ~10 GB for a full day: keep only the last tick per contract, chunk by chunk (file is in arrival order).
        last = {}
        for ch in pd.read_csv(ab_path, chunksize=2_000_000, usecols=["canonical_symbol", "instrument_type", "received_at_ist", "ltp", "open_price",
                                                                    "high_price", "low_price", "volume", "open_interest"]):
            ch = ch[ch.instrument_type.isin(["CE", "PE", "FUT"])]
            for sym, row in ch.groupby("canonical_symbol").tail(1).set_index("canonical_symbol").iterrows():
                last[sym] = row
        a = pd.DataFrame.from_dict(last, orient="index")
        feeds.append(("Alphabots", a.rename(columns={"open_price": "open", "high_price": "high", "low_price": "low", "ltp": "last", "open_interest": "oi"})))
    rows, detail = [], []
    for name, live in feeds:
        j = live.join(fo, how="inner")
        for kind, g in (("options", j[~j.index.str.endswith("FUT")]), ("futures", j[j.index.str.endswith("FUT")])):
            if g.empty:
                continue
            eq = lambda a, o: f"{100 * ((g[a] - g[o]).abs() < 0.0051).mean():.0f}%"
            rows.append({"feed": name, "contracts": f"{kind} ({len(g)})",
                         "day open = official": eq("open", "OpnPric"), "day high = official": eq("high", "HghPric"),
                         "day low = official": eq("low", "LwPric"),
                         "last price = official last": eq("last", "LastPric"),
                         "median |last - official close|": round((g["last"] - g.ClsPric).abs().median(), 2),
                         "volume live/official p50": round((g.volume / g.vol_qty).median(), 4),
                         "OI live/official p50": round((g.oi / g.OpnIntrst).median(), 4) if "oi" in g else "-"})
            detail.append(g.assign(feed=name)[["feed", "open", "OpnPric", "high", "HghPric", "low", "LwPric", "last", "LastPric", "ClsPric", "volume", "vol_qty"]])
    out = BASE / "reports" / day
    out.mkdir(parents=True, exist_ok=True)
    res = pd.DataFrame(rows)
    res.to_csv(out / "eod_live_vs_official.csv", index=False)
    pd.concat(detail).to_csv(out / "eod_live_vs_official_detail.csv")
    print(f"capture stopped at {stop}; last price and volume are as of then\n")
    print(res.to_string(index=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())

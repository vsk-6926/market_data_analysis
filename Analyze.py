"""
Offline analysis of captured feeds.

    python analyze.py captures/2026-07-16 --ref fyers

Runs the three tests that actually separate a stream from a snapshot:

  1. Inter-arrival distribution   — a spike at a fixed cadence means the feed
                                    is polling on your behalf, not streaming.
  2. Tick capture ratio           — events seen vs the reference feed.
  3. 1-min range shrinkage        — the cheapest throttle detector there is.
                                    A coalescing feed misses spikes, so its
                                    bars are systematically NARROWER. If a
                                    provider's high-low ranges are tighter
                                    than the reference, it is eating your data.

Lead-lag joins on cumulative volume, not price: volume is monotonic and
uniquely identifies a trade, so it's the only reliable way to say two feeds
are reporting the same event.

Caveat worth keeping in mind: this measures SDK-callback-to-us, not
wire-arrival-to-us, because each collector goes through its vendor's SDK and
those differ in parse quality. Good enough to rank capture and cadence. For
true lead-lag you eventually want raw sockets on all four.
"""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import pandas as pd


def load(path: Path) -> pd.DataFrame:
    rows = []
    for line in path.open():
        try:
            o = json.loads(line)
        except json.JSONDecodeError:
            continue
        d = o.get("d")
        if not isinstance(d, dict):
            continue
        rows.append({"recv_ns": o["t"], **_flat(d)})
    df = pd.DataFrame(rows)
    if not df.empty:
        df["provider"] = path.stem
        df["ts"] = pd.to_datetime(df["recv_ns"], unit="ns", utc=True)
    return df


def _flat(d: dict) -> dict:
    """Pull the few fields we compare across providers, whatever they're called."""
    def pick(*names):
        for n in names:
            if n in d and d[n] is not None:
                return d[n]
        return None

    depth = d.get("depth")
    bid = ask = None
    if isinstance(depth, list) and depth and isinstance(depth[0], dict):
        bid, ask = depth[0].get("bid_price"), depth[0].get("ask_price")

    bids, asks = d.get("bids"), d.get("asks")
    if isinstance(bids, list) and bids and isinstance(bids[0], dict):
        bid = bids[0].get("price")
    if isinstance(asks, list) and asks and isinstance(asks[0], dict):
        ask = asks[0].get("price")

    return {
        "id": str(pick("symbol", "security_id", "ref_id", "token", "ticker") or ""),
        "ltp": _f(pick("ltp", "LTP", "last_traded_price")),
        "vol": _f(pick("vol_traded_today", "volume")),
        "bid": _f(pick("bid_price", "bid") or bid),
        "ask": _f(pick("ask_price", "ask") or ask),
        "open": _f(pick("open", "open_price", "o")),
        "high": _f(pick("high", "high_price", "h")),
        "low": _f(pick("low", "low_price", "l")),
        "close": _f(pick("close", "close_price", "c")),
        "levels": len(bids) if isinstance(bids, list)
                  else (len(depth) if isinstance(depth, list) else None),
    }


def _f(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _parse_bound(v: str | None):
    if not v:
        return None
    s = str(v).strip()
    if not s:
        return None
    ts = pd.to_datetime(s)
    if getattr(ts, "tzinfo", None) is None:
        ts = ts.tz_localize("Asia/Kolkata")
    return ts.tz_convert("UTC")


def apply_time_range(df: pd.DataFrame, start: str | None, end: str | None):
    if df.empty or "ts" not in df.columns:
        return df
    out = df.copy()
    start_ts = _parse_bound(start)
    end_ts = _parse_bound(end)
    if start_ts is not None:
        out = out[out["ts"] >= start_ts]
    if end_ts is not None:
        out = out[out["ts"] <= end_ts]
    return out


def report_window(df: pd.DataFrame):
    print("\n── TIME WINDOW " + "─" * 56)
    if df.empty or "ts" not in df.columns:
        print("  no rows in selected range")
        return
    lo = df["ts"].min()
    hi = df["ts"].max()
    print(f"  from  {lo}")
    print(f"  to    {hi}")
    print(f"  rows  {len(df):,}")


def interarrival(df: pd.DataFrame):
    print("\n── INTER-ARRIVAL (ms, per instrument) " + "─" * 38)
    out = []
    for (prov, iid), g in df.groupby(["provider", "id"]):
        # Low threshold on purpose: a feed sparse enough to fall below this
        # is exactly the one you want to see in the table, not filter out.
        if len(g) < 5:
            continue
        d = g["recv_ns"].sort_values().diff().dropna() / 1e6
        out.append({
            "provider": prov, "id": iid, "n": len(g),
            "p50": round(d.median(), 1), "p90": round(d.quantile(.9), 1),
            "p99": round(d.quantile(.99), 1),
        })
    r = pd.DataFrame(out)
    if r.empty:
        print("  not enough data")
        return
    agg = r.groupby("provider")[["p50", "p90", "p99"]].median()
    agg["msgs"] = r.groupby("provider")["n"].sum()
    print(agg.to_string())
    print("\n  A p50 pinned near a round number (250/500/1000ms) = snapshots.")
    print("  A p50 that varies with activity = a real stream.")


def per_second_density(df: pd.DataFrame):
    print("\n── DATAPOINTS PER SECOND " + "─" * 47)
    d = df.copy()
    if "ts" not in d.columns:
        print("  missing timestamp column")
        return
    d["sec"] = d["ts"].dt.floor("1s")

    per_sec = d.groupby(["provider", "sec"]).size().rename("points").reset_index()
    if per_sec.empty:
        print("  no timestamped data")
        return

    rows = []
    for prov, g in per_sec.groupby("provider"):
        peak = g.loc[g["points"].idxmax()]
        rows.append({
            "provider": prov,
            "active_secs": int(len(g)),
            "avg_pts/sec": round(g["points"].mean(), 2),
            "median": round(g["points"].median(), 2),
            "p90": round(g["points"].quantile(0.9), 2),
            "max": int(g["points"].max()),
            "peak_second": str(peak["sec"]),
        })
    print(pd.DataFrame(rows).set_index("provider").to_string())
    print("\n  This counts how many packets each broker gives you inside one second.")
    print("  For sparse feeds, 'median' can still be high on active seconds, so read it together with active_secs.")


def per_minute_second_coverage(df: pd.DataFrame):
    print("\n── SECONDS PRESENT PER MINUTE " + "─" * 40)
    d = df.copy()
    if "ts" not in d.columns:
        print("  missing timestamp column")
        return
    d["min"] = d["ts"].dt.floor("1min")
    d["sec"] = d["ts"].dt.floor("1s")

    cov = d.groupby(["provider", "id", "min"])["sec"].nunique().rename("seconds_present").reset_index()
    if cov.empty:
        print("  no timestamped data")
        return
    cov["seconds_missing"] = 60 - cov["seconds_present"]

    rows = []
    for prov, g in cov.groupby("provider"):
        rows.append({
            "provider": prov,
            "mins": int(len(g)),
            "avg_secs_present": round(g["seconds_present"].mean(), 2),
            "median_secs_present": round(g["seconds_present"].median(), 2),
            "avg_secs_missing": round(g["seconds_missing"].mean(), 2),
            "median_secs_missing": round(g["seconds_missing"].median(), 2),
            "full_60s_mins": int((g["seconds_present"] == 60).sum()),
        })
    print(pd.DataFrame(rows).set_index("provider").to_string())
    print("\n  'seconds_present' means how many distinct second buckets had at least one datapoint in that minute.")
    print("  A true tick stream tends to fill many seconds. A 1-minute snapshot feed will sit near 1 present / 59 missing.")


def second_price_comparison(df: pd.DataFrame, ref: str):
    print("\n── PER-SECOND LTP DIFFERENCE vs " + ref.upper() + " " + "─" * 30)
    d = df.dropna(subset=["ltp"]).copy()
    if d.empty:
        print("  no LTP data")
        return
    d["sec"] = d["ts"].dt.floor("1s")
    sec_last = d.groupby(["provider", "id", "sec"])["ltp"].last().reset_index()

    if ref not in set(sec_last["provider"]):
        print(f"  reference '{ref}' not in capture")
        return

    ref_df = sec_last[sec_last["provider"] == ref][["id", "sec", "ltp"]].rename(
        columns={"ltp": "ltp_ref"}
    )

    rows = []
    for prov in sorted(set(sec_last["provider"])):
        if prov == ref:
            continue
        prov_df = sec_last[sec_last["provider"] == prov][["id", "sec", "ltp"]].rename(
            columns={"ltp": "ltp_cmp"}
        )
        common_ids = sorted(set(ref_df["id"]) & set(prov_df["id"]))
        if not common_ids:
            rows.append({
                "provider": prov,
                "common_ids": 0,
                "secs_both": 0,
                "secs_same": 0,
                "secs_diff": 0,
                "secs_missing": 0,
                "median_diff": None,
                "median_abs_diff": None,
            })
            continue

        a = ref_df[ref_df["id"].isin(common_ids)]
        b = prov_df[prov_df["id"].isin(common_ids)]
        j = a.merge(b, on=["id", "sec"], how="outer")
        both = j.dropna(subset=["ltp_ref", "ltp_cmp"]).copy()
        missing = int(j["ltp_ref"].isna().sum() + j["ltp_cmp"].isna().sum())
        if both.empty:
            rows.append({
                "provider": prov,
                "common_ids": len(common_ids),
                "secs_both": 0,
                "secs_same": 0,
                "secs_diff": 0,
                "secs_missing": missing,
                "median_diff": None,
                "median_abs_diff": None,
            })
            continue

        both["diff"] = both["ltp_cmp"] - both["ltp_ref"]
        same = both["diff"].round(6).eq(0)
        rows.append({
            "provider": prov,
            "common_ids": len(common_ids),
            "secs_both": int(len(both)),
            "secs_same": int(same.sum()),
            "secs_diff": int((~same).sum()),
            "secs_missing": missing,
            "median_diff": round(both["diff"].median(), 4),
            "median_abs_diff": round(both["diff"].abs().median(), 4),
        })

    out = pd.DataFrame(rows)
    if out.empty:
        print("  nothing to compare")
        return
    print(out.set_index("provider").to_string())
    print("\n  'secs_same' means both brokers had an LTP in that second and the last LTP in that second matched exactly.")
    print("  'secs_missing' counts aligned seconds where one side had no LTP at all.")


def capture_ratio(df: pd.DataFrame, ref: str):
    print("\n── TICK CAPTURE vs " + ref.upper() + " " + "─" * 46)
    counts = defaultdict(dict)
    for (prov, iid), g in df.groupby(["provider", "id"]):
        counts[iid][prov] = g["ltp"].dropna().diff().ne(0).sum()

    rows = []
    sparse_ref = []
    for iid, per in counts.items():
        base = per.get(ref)
        if not base:
            continue
        row = {"instrument": iid, ref: int(base)}
        if base < 10:
            sparse_ref.append((iid, int(base)))
        for prov, n in per.items():
            if prov != ref:
                row[prov] = f"{int(n)}  ({n / base * 100:.0f}%)"
        rows.append(row)
    if rows:
        print(pd.DataFrame(rows).to_string(index=False))
    else:
        print(f"  no overlapping instruments with reference '{ref}'")
    if sparse_ref:
        joined = ", ".join(f"{iid}={base}" for iid, base in sparse_ref[:5])
        print(f"\n  Warning: the reference is too sparse for percentage interpretation ({joined}).")
        print("  Example: ref=1 and cmp=5992 becomes 599200%, which is mathematically true but analytically useless.")
    print("\n  Instrument IDs differ per broker — map them first or this is noise.")


def range_shrinkage(df: pd.DataFrame, ref: str):
    print("\n── 1-MIN HIGH-LOW RANGE vs " + ref.upper() + " " + "─" * 38)
    d = df.copy()
    if "ts" not in d.columns:
        print("  missing timestamp column")
        return
    d["min"] = d["ts"].dt.floor("1min")

    pieces = []
    for (prov, iid), g in d.groupby(["provider", "id"]):
        has_hl = "high" in g.columns and "low" in g.columns and g[["high", "low"]].notna().any().any()
        if has_hl:
            b = g.dropna(subset=["high", "low"]).groupby("min").agg(
                high=("high", "max"),
                low=("low", "min"),
            )
            if b.empty:
                continue
            b["range"] = b["high"] - b["low"]
        else:
            b = g.dropna(subset=["ltp"]).groupby("min")["ltp"].agg(["max", "min"]).rename(
                columns={"max": "ltp_max", "min": "ltp_min"}
            )
            if b.empty:
                continue
            b["range"] = b["ltp_max"] - b["ltp_min"]
        b["provider"] = prov
        b["id"] = iid
        pieces.append(b.reset_index()[["provider", "id", "min", "range"]])

    if not pieces:
        print("  no LTP/OHLC data")
        return
    bars = pd.concat(pieces, ignore_index=True)

    avg = bars.groupby("provider")["range"].mean()
    if ref not in avg:
        print(f"  reference '{ref}' not in capture")
        return
    print(pd.DataFrame({
        "mean 1m range": avg.round(4),
        "vs ref": (avg / avg[ref] * 100).round(1).astype(str) + "%",
    }).to_string())
    print("\n  Below 100% means that feed is missing spikes the reference saw.")
    print("  That's coalescing. It is invisible in an LTP display and fatal to TCA.")


def depth_report(df: pd.DataFrame):
    print("\n── DEPTH LEVELS DELIVERED " + "─" * 47)
    if "levels" not in df.columns:
        print("  no depth data captured")
        return
    lv = df.dropna(subset=["levels"]).groupby("provider")["levels"].agg(["max", "median"])
    print(lv.to_string() if not lv.empty else "  no depth data captured")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("dir", nargs="?", help="capture dir, e.g. captures/2026-07-16")
    ap.add_argument("--fyers-csv", help="Fyers tick CSV export")
    ap.add_argument("--dhan-csv", help="Dhan tick CSV export")
    ap.add_argument("--nubra-parquet", help="Nubra parquet export (1m bars)")
    ap.add_argument("--symbol", "--nubra-symbol", dest="symbol",
                    default="24400PE",
                    help="canonical contract symbol, e.g. 24400PE")
    ap.add_argument("--from-ts", help="inclusive start bound, e.g. 2026-07-21 09:15:00")
    ap.add_argument("--to-ts", help="inclusive end bound, e.g. 2026-07-21 09:20:00")
    ap.add_argument("--ref", default="fyers", help="reference provider")
    a = ap.parse_args()

    frames = []

    if a.nubra_parquet and (a.fyers_csv or a.dhan_csv):
        if a.fyers_csv:
            fyers = pd.read_csv(a.fyers_csv)
            need = {"datetime", "symbol"}
            if not need.issubset(set(fyers.columns)):
                raise SystemExit(f"fyers csv missing columns: {sorted(need - set(fyers.columns))}")
            fy = fyers.copy()
            fy["base_symbol"] = fy["symbol"].astype(str).str.replace(r"^[A-Z]+:", "", regex=True)
            fy = fy[fy["base_symbol"] == a.symbol]
            if fy.empty:
                syms = fyers["symbol"].astype(str).dropna().unique()[:10]
                raise SystemExit(f"no FYERS rows for {a.symbol}. FYERS sample: {syms}")
            fy["ts"] = pd.to_datetime(fy["datetime"]).dt.tz_localize("Asia/Kolkata").dt.tz_convert("UTC")
            fy["recv_ns"] = fy["ts"].astype("int64")
            fy["provider"] = "fyers"
            fy["id"] = a.symbol
            fy["ltp"] = pd.to_numeric(fy.get("ltp"), errors="coerce")
            fy["high"] = pd.to_numeric(fy.get("high_price"), errors="coerce")
            fy["low"] = pd.to_numeric(fy.get("low_price"), errors="coerce")
            frames.append(fy[["provider", "id", "recv_ns", "ts", "ltp", "high", "low"]])

        if a.dhan_csv:
            dhan = pd.read_csv(a.dhan_csv)
            if "timestamp" not in dhan.columns:
                raise SystemExit("dhan csv missing 'timestamp' column")
            dhan["ts"] = pd.to_datetime(dhan["timestamp"], utc=True)
            dhan["recv_ns"] = (dhan["ts"].astype("int64")).astype("int64")
            dhan["provider"] = "dhan"
            dhan["id"] = a.symbol
            dhan["ltp"] = pd.to_numeric(dhan.get("ltp"), errors="coerce")
            frames.append(dhan[["provider", "id", "recv_ns", "ts", "ltp"]])

        nubra = pd.read_parquet(a.nubra_parquet)
        need = {"datetime_IST", "symbol", "instrument_type"}
        if not need.issubset(set(nubra.columns)):
            raise SystemExit(f"nubra parquet missing columns: {sorted(need - set(nubra.columns))}")
        n = nubra[nubra["symbol"].astype(str) == a.symbol].copy()
        if n.empty:
            syms = nubra[nubra["instrument_type"] == "FUT"]["symbol"].astype(str).unique()[:10]
            raise SystemExit(f"no Nubra rows for {a.symbol}. Nubra sample: {syms}")
        n["ts"] = pd.to_datetime(n["datetime_IST"], utc=True)
        n["recv_ns"] = (n["ts"].astype("int64")).astype("int64")
        n["provider"] = "nubra"
        n["id"] = a.symbol
        n["ltp"] = pd.to_numeric(n.get("close"), errors="coerce")
        n["high"] = pd.to_numeric(n.get("high"), errors="coerce")
        n["low"] = pd.to_numeric(n.get("low"), errors="coerce")
        frames.append(n[["provider", "id", "recv_ns", "ts", "ltp", "high", "low"]])

    else:
        if not a.dir:
            raise SystemExit("pass either a capture dir or --nubra-parquet plus --fyers-csv / --dhan-csv")
        files = sorted(Path(a.dir).glob("*.jsonl"))
        if not files:
            raise SystemExit(f"no .jsonl captures in {a.dir}")

        for f in files:
            df = load(f)
            print(f"  loaded {f.name:14s} {len(df):>8,} rows")
            if not df.empty:
                frames.append(df)
    if not frames:
        raise SystemExit("nothing parseable")

    df = pd.concat(frames, ignore_index=True)
    df = apply_time_range(df, a.from_ts, a.to_ts)
    if df.empty:
        raise SystemExit("no rows in selected time range")
    report_window(df)
    per_second_density(df)
    per_minute_second_coverage(df)
    second_price_comparison(df, a.ref)
    interarrival(df)
    depth_report(df)
    capture_ratio(df, a.ref)
    range_shrinkage(df, a.ref)
    print()


if __name__ == "__main__":
    main()

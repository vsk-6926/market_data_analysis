"""Memory-safe tick loading for the Quantick vs reference-feed reports.

A full Alphabots day is ~10 GB of CSV and the Mac has 8 GB of RAM, so ticks are read in
chunks, filtered to NIFTY F&O, reduced to the columns the reports use and cached as parquet
next to the CSV (`<file>.nifty.parquet`). The cache is rebuilt when the CSV has grown.

Every feed file uses the bench schema: listed_at_ist = the feed's own timestamp
(Quantick: LTT, 1-second resolution; Alphabots/TBT: exchange time, ns),
received_at_ist = arrival on this Mac.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
IST = "Asia/Kolkata"
FILES = {
    "quantick": ROOT / "Quantick" / "quantick_live_ticks_{d}.csv",
    "alphabots": ROOT / "Alphabots" / "alphabots_live_ticks_{d}.csv",
    "tbt": ROOT / "TBT" / "tbt_live_ticks_{d}.csv",
}
COLS = ["canonical_symbol", "instrument_type", "listed_at_ist", "received_at_ist", "ltp", "volume", "open_interest", "message_type"]


def _is_nifty_fo(s: pd.Series) -> pd.Series:
    return s.str.startswith("NIFTY2") | (s.str.startswith("NIFTY") & s.str.endswith("FUT"))


def load_ticks(feed: str, day: str, chunksize: int = 2_000_000) -> pd.DataFrame:
    """NIFTY F&O ticks: canonical_symbol, instrument_type, ts (feed timestamp), recv (Mac), ltp, volume, oi."""
    src = Path(str(FILES[feed]).format(d=day))
    cache = src.with_suffix(".nifty.parquet")
    if cache.exists() and cache.stat().st_mtime >= src.stat().st_mtime:
        return pd.read_parquet(cache)
    header = pd.read_csv(src, nrows=0).columns
    usecols = [c for c in COLS if c in header]
    parts = []
    for chunk in pd.read_csv(src, usecols=usecols, chunksize=chunksize, dtype={"canonical_symbol": str}):
        chunk = chunk[_is_nifty_fo(chunk["canonical_symbol"].astype(str))]
        if "message_type" in chunk:
            chunk = chunk[~chunk["message_type"].isin(["keyframe", "status"])]
        if chunk.empty:
            continue
        out = pd.DataFrame({
            "canonical_symbol": chunk["canonical_symbol"].values,
            "instrument_type": chunk["instrument_type"].astype(str).values,
            "ts": pd.to_datetime(chunk["listed_at_ist"], format="ISO8601", utc=True).values,
            "recv": pd.to_datetime(chunk["received_at_ist"], format="ISO8601", utc=True).values,
            "ltp": pd.to_numeric(chunk["ltp"], errors="coerce").astype("float64").values,
            "volume": pd.to_numeric(chunk["volume"], errors="coerce").values,
            "oi": pd.to_numeric(chunk.get("open_interest"), errors="coerce").values if "open_interest" in chunk else None,
        })
        parts.append(out)
    df = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(columns=["canonical_symbol", "instrument_type", "ts", "recv", "ltp", "volume", "oi"])
    for c in ("ts", "recv"):
        df[c] = pd.to_datetime(df[c], utc=True).dt.tz_convert(IST)
    df["canonical_symbol"] = df["canonical_symbol"].astype("category")
    df.to_parquet(cache, index=False)
    return df


def new_trades(df: pd.DataFrame) -> pd.DataFrame:
    """Rows where the contract's cumulative volume rose: one row per trade (or trade batch)."""
    d = df.dropna(subset=["volume", "ltp"]).sort_values(["canonical_symbol", "recv"], kind="stable")
    prev = d.groupby("canonical_symbol", observed=True)["volume"].shift()
    return d[(d["volume"] > prev) & (d["ltp"] > 0)]


def clock_offset_ms(day: str) -> float | None:
    """Median NTP offset logged during the session (+ = Mac clock behind true time)."""
    path = ROOT / "logs" / f"clock-offset-{day}.log"
    if not path.exists():
        return None
    vals = []
    for line in path.read_text().splitlines():
        parts = line.split()
        if len(parts) >= 2:
            try:
                vals.append(float(parts[1]) * 1000)
            except ValueError:
                pass
    return float(pd.Series(vals).median()) if vals else None

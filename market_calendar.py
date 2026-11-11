#!/usr/bin/env python3
"""NSE trading-day calendar for the daily capture launcher.

Holidays come from Arrow's `/info/holidays` endpoint and are cached to
`market_holidays.json`, so a scheduled run still knows the calendar when the
network or the Arrow token is unavailable. The cache is refreshed whenever it
is older than a week.

Fail-open is deliberate: if we cannot establish the calendar at all we report a
trading day and let the collectors decide. A capture that runs on a holiday
writes an empty file; a capture skipped on a real trading day loses the session.
"""
from __future__ import annotations

import datetime as dt
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent
CACHE_FILE = ROOT / "market_holidays.json"
CACHE_MAX_AGE_DAYS = 7
IST = dt.timezone(dt.timedelta(hours=5, minutes=30))


def _today() -> dt.date:
    return dt.datetime.now(IST).date()


def _parse_ddmmyyyy(text: str) -> dt.date | None:
    try:
        return dt.datetime.strptime(str(text).strip(), "%d-%m-%Y").date()
    except (ValueError, TypeError):
        return None


def _fetch_from_arrow() -> dict | None:
    """Pull the holiday list using whatever Arrow credentials are already set up."""
    try:
        import importlib.util

        spec = importlib.util.spec_from_file_location("arrow_ws", ROOT / "Arrow" / "arrow_websocket.py")
        aw = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(aw)
        from pyarrow_client import ArrowClient

        creds = aw.load_credentials(ROOT)
        if not creds.get("app_id"):
            return None
        token = aw.resolve_access_token(creds)
        if not token:
            return None
        client = ArrowClient(app_id=str(creds["app_id"]))
        client.set_token(str(token))
        payload = client.get_holidays()
        return payload if isinstance(payload, dict) else None
    except Exception:
        return None


def _cache_is_fresh() -> bool:
    if not CACHE_FILE.exists():
        return False
    age = dt.datetime.now(dt.timezone.utc) - dt.datetime.fromtimestamp(
        CACHE_FILE.stat().st_mtime, tz=dt.timezone.utc
    )
    return age.days < CACHE_MAX_AGE_DAYS


def load_calendar(refresh: bool = False) -> dict:
    if not refresh and _cache_is_fresh():
        try:
            return json.loads(CACHE_FILE.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            pass

    payload = _fetch_from_arrow()
    if payload:
        try:
            CACHE_FILE.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        except OSError:
            pass
        return payload

    if CACHE_FILE.exists():
        # Stale beats nothing: the list only changes once a year.
        try:
            return json.loads(CACHE_FILE.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            pass
    return {}


def holiday_name(day: dt.date | None = None, calendar: dict | None = None) -> str | None:
    day = day or _today()
    calendar = calendar if calendar is not None else load_calendar()
    for text, name in (calendar.get("holidays") or {}).items():
        if _parse_ddmmyyyy(text) == day:
            return str(name)
    return None


def is_special_trading_day(day: dt.date | None = None, calendar: dict | None = None) -> bool:
    """Muhurat-style sessions: the exchange is open on an otherwise closed day."""
    day = day or _today()
    calendar = calendar if calendar is not None else load_calendar()
    return any(_parse_ddmmyyyy(t) == day for t in (calendar.get("specialTradingDays") or {}))


def check_trading_day(day: dt.date | None = None, refresh: bool = False) -> tuple[bool, str]:
    """Return (should_capture, human-readable reason)."""
    day = day or _today()
    calendar = load_calendar(refresh=refresh)

    if is_special_trading_day(day, calendar):
        return True, f"{day.isoformat()} is a special trading session"
    if day.weekday() >= 5:
        return False, f"{day.isoformat()} is a {day.strftime('%A')} - market closed"
    name = holiday_name(day, calendar)
    if name:
        return False, f"{day.isoformat()} is an NSE holiday ({name})"
    if not calendar:
        return True, f"{day.isoformat()} is a weekday (holiday list unavailable, assuming open)"
    return True, f"{day.isoformat()} is a trading day"


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser(description="Check whether a date is an NSE trading day.")
    ap.add_argument("date", nargs="?", help="YYYY-MM-DD (default: today IST)")
    ap.add_argument("--refresh", action="store_true", help="force refresh the holiday cache")
    ap.add_argument("--upcoming", type=int, default=0, help="also list the next N days")
    args = ap.parse_args()

    target = dt.date.fromisoformat(args.date) if args.date else _today()
    ok, reason = check_trading_day(target, refresh=args.refresh)
    print(f"{'CAPTURE' if ok else 'SKIP':>8}  {reason}")

    if args.upcoming:
        cal = load_calendar()
        for offset in range(1, args.upcoming + 1):
            d = target + dt.timedelta(days=offset)
            ok, reason = check_trading_day(d)
            print(f"{'CAPTURE' if ok else 'SKIP':>8}  {reason}")
    raise SystemExit(0 if ok else 1)

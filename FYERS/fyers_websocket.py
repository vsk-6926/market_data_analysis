import argparse
import os
from pathlib import Path
from datetime import datetime, timezone, timedelta
import csv
import json
import math
import re
import signal
import sys
import threading
import time
import urllib.request

from fyers_apiv3 import fyersModel
from fyers_apiv3.FyersWebsocket import data_ws, order_ws

try:
    import duckdb
except ImportError:
    duckdb = None


BASE_DIR = Path(__file__).resolve().parent
SHARED_LOG_DIR = BASE_DIR.parent / "logs"
LOGIN_FILE = BASE_DIR / "login.py"
ACCESS_TOKEN_FILE = BASE_DIR / "access_token.txt"
SYMBOL_MASTER_FILE = BASE_DIR / "NSE_FO.csv"
SYMBOL_MASTER_URL = "https://public.fyers.in/sym_details/NSE_FO.csv"
DEFAULT_SYMBOLS = []
TICKS_CSV_BASENAME = "fyers_live_ticks"
TICKS_DUCKDB_BASENAME = "fyers_live_ticks"
TICKS_1S_CSV_BASENAME = "fyers_live_1s"
IST = timezone(timedelta(hours=5, minutes=30))


csv.field_size_limit(sys.maxsize)


def load_client_id(login_file: Path) -> str:
    if not login_file.exists():
        raise FileNotFoundError(f"login.py not found at: {login_file}")

    content = login_file.read_text(encoding="utf-8")
    match = re.search(r'^client_id\s*=\s*"([^"]+)"', content, re.MULTILINE)
    if not match:
        raise ValueError("Could not find client_id in login.py")
    return match.group(1).strip()


def load_access_token(token_file: Path) -> str:
    if not token_file.exists():
        raise FileNotFoundError(f"access_token.txt not found at: {token_file}")

    token = token_file.read_text(encoding="utf-8").strip()
    if not token:
        raise ValueError("access_token.txt is empty")
    return token


def pretty_print(label: str, payload):
    try:
        message = json.dumps(payload, indent=2, sort_keys=True, default=str)
    except TypeError:
        message = str(payload)
    print(f"\n[{label}]\n{message}")


def today_tag(explicit_date: str | None = None) -> str:
    return explicit_date or datetime.now().date().isoformat()


def daily_output_paths(run_date: str | None = None):
    tag = today_tag(run_date)
    return {
        "ticks_csv": BASE_DIR / f"{TICKS_CSV_BASENAME}_{tag}.csv",
        "ticks_duckdb": BASE_DIR / f"{TICKS_DUCKDB_BASENAME}_{tag}.duckdb",
        "ticks_1s_csv": BASE_DIR / f"{TICKS_1S_CSV_BASENAME}_{tag}.csv",
    }


def download_symbol_master(symbol_master_file: Path):
    urllib.request.urlretrieve(SYMBOL_MASTER_URL, symbol_master_file)


def parse_expiry_date(row):
    try:
        return datetime.fromtimestamp(int(row[8])).date()
    except (ValueError, TypeError, IndexError, OSError):
        expiry_str = row[7].strip() if len(row) > 7 else ""
        return datetime.strptime(expiry_str, "%Y-%m-%d").date()


def load_nifty_future_candidates(symbol_master_file: Path):
    if not symbol_master_file.exists():
        raise FileNotFoundError(f"NSE_FO.csv not found at: {symbol_master_file}")

    candidates = []
    with symbol_master_file.open(encoding="utf-8") as csv_file:
        reader = csv.reader(csv_file)
        for row in reader:
            if len(row) < 14:
                continue

            description = row[1].strip()
            symbol = row[9].strip()
            underlying = row[13].strip().upper()

            if underlying != "NIFTY":
                continue
            if not symbol.endswith("FUT"):
                continue
            if "FUT" not in description.upper():
                continue

            try:
                expiry_date = parse_expiry_date(row)
            except ValueError:
                continue

            candidates.append((expiry_date, symbol))

    return candidates


def resolve_nearest_nifty_future(symbol_master_file: Path) -> str:
    today = datetime.now().date()

    try:
        candidates = load_nifty_future_candidates(symbol_master_file)
    except FileNotFoundError:
        download_symbol_master(symbol_master_file)
        candidates = load_nifty_future_candidates(symbol_master_file)

    future_candidates = [item for item in candidates if item[0] >= today]
    if not future_candidates:
        download_symbol_master(symbol_master_file)
        candidates = load_nifty_future_candidates(symbol_master_file)
        future_candidates = [item for item in candidates if item[0] >= today]

    if future_candidates:
        future_candidates.sort(key=lambda item: item[0])
        return future_candidates[0][1]

    if not candidates:
        raise ValueError("Could not find any NIFTY futures contract in NSE_FO.csv")

    candidates.sort(key=lambda item: item[0], reverse=True)
    print("Warning: using latest available NIFTY futures contract from stale symbol master.")
    return candidates[0][1]


def load_nifty_option_candidates(symbol_master_file: Path):
    if not symbol_master_file.exists():
        raise FileNotFoundError(f"NSE_FO.csv not found at: {symbol_master_file}")

    candidates = []
    with symbol_master_file.open(encoding="utf-8") as csv_file:
        reader = csv.reader(csv_file)
        for row in reader:
            if len(row) < 17:
                continue
            symbol = row[9].strip()
            underlying = row[13].strip().upper()
            option_type = row[16].strip().upper()
            strike_str = row[15].strip()
            if underlying != "NIFTY":
                continue
            if option_type not in {"CE", "PE"}:
                continue
            try:
                expiry_date = parse_expiry_date(row)
                strike = float(strike_str)
            except ValueError:
                continue
            candidates.append(
                {
                    "expiry": expiry_date,
                    "symbol": symbol,
                    "strike": strike,
                    "option_type": option_type,
                }
            )
    return candidates


def _extract_first_price(payload):
    if isinstance(payload, dict):
        for key in ("lp", "ltp", "last_price", "price"):
            value = payload.get(key)
            if isinstance(value, (int, float)):
                return float(value)
        for value in payload.values():
            price = _extract_first_price(value)
            if price is not None:
                return price
    elif isinstance(payload, list):
        for item in payload:
            price = _extract_first_price(item)
            if price is not None:
                return price
    return None


def fetch_reference_price(fyers, future_symbol: str):
    quote = fyers.quotes({"symbols": future_symbol})
    price = _extract_first_price(quote)
    if price is None:
        raise RuntimeError(f"Could not extract reference price from FYERS quote: {quote}")
    return price


def build_nifty_universe(symbol_master_file: Path, fyers, strikes: int, expiries: int):
    future_symbol = resolve_nearest_nifty_future(symbol_master_file)
    option_candidates = load_nifty_option_candidates(symbol_master_file)
    if not option_candidates:
        raise ValueError("Could not find any NIFTY option contracts in NSE_FO.csv")
    reference_price = fetch_reference_price(fyers, future_symbol)
    strike_values = sorted({c["strike"] for c in option_candidates})
    steps = [b - a for a, b in zip(strike_values, strike_values[1:]) if b > a]
    step = min(steps) if steps else 50.0
    atm = round(reference_price / step) * step
    valid_expiries = sorted({c["expiry"] for c in option_candidates if c["expiry"] >= datetime.now().date()})
    if not valid_expiries:
        valid_expiries = sorted({c["expiry"] for c in option_candidates})
    chosen_expiries = set(valid_expiries[: max(1, expiries)])
    strike_targets = {atm + step * offset for offset in range(-strikes, strikes + 1)}
    selected = [
        c["symbol"]
        for c in option_candidates
        if c["expiry"] in chosen_expiries and c["strike"] in strike_targets
    ]
    symbols = [future_symbol] + sorted(set(selected))
    return {
        "symbols": symbols,
        "future_symbol": future_symbol,
        "reference_price": reference_price,
        "atm": atm,
        "step": step,
        "expiries": sorted(chosen_expiries),
    }


def normalize_tick(message, received_at_ist=None):
    if not isinstance(message, dict):
        return None

    symbol = message.get("symbol")
    if not symbol:
        return None

    event_time = message.get("last_traded_time") or message.get("exch_feed_time")
    event_datetime = datetime.fromtimestamp(event_time) if event_time else datetime.now()
    listed_at_ist = (
        datetime.fromtimestamp(event_time, tz=timezone.utc).astimezone(IST).isoformat()
        if event_time
        else datetime.now(IST).isoformat()
    )
    if received_at_ist is None:
        received_at_ist = datetime.now(IST).isoformat()

    return {
        "date": event_datetime.date(),
        "datetime": event_datetime,
        "listed_at_ist": listed_at_ist,
        "received_at_ist": received_at_ist,
        "symbol": symbol,
        "ltp": message.get("ltp"),
        "volume": message.get("vol_traded_today"),
        "bid_price": message.get("bid_price"),
        "ask_price": message.get("ask_price"),
        "open_price": message.get("open_price"),
        "high_price": message.get("high_price"),
        "low_price": message.get("low_price"),
        "prev_close_price": message.get("prev_close_price"),
        "open_interest": message.get("OI"),
        "message_type": message.get("type"),
        "raw_json": json.dumps(message, default=str),
    }


class TickStorage:
    def __init__(self, csv_path: Path, duckdb_path: Path, one_second_csv_path: Path):
        self.csv_path = csv_path
        self.duckdb_path = duckdb_path
        self.one_second_csv_path = one_second_csv_path
        self.duckdb_enabled = duckdb is not None
        self.lock = threading.Lock()
        self.pending_1s = {}
        self._init_csv()
        self._init_1s_csv()
        if self.duckdb_enabled:
            self._init_duckdb()

    def _init_csv(self):
        if self.csv_path.exists():
            return
        with self.csv_path.open("w", newline="", encoding="utf-8") as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow([
                "date",
                "datetime",
                "listed_at_ist",
                "received_at_ist",
                "symbol",
                "ltp",
                "volume",
                "bid_price",
                "ask_price",
                "open_price",
                "high_price",
                "low_price",
                "prev_close_price",
                "open_interest",
                "message_type",
                "raw_json",
            ])

    def _init_1s_csv(self):
        if self.one_second_csv_path.exists():
            return
        with self.one_second_csv_path.open("w", newline="", encoding="utf-8") as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow([
                "date",
                "datetime",
                "listed_at_ist",
                "first_received_at_ist",
                "last_received_at_ist",
                "symbol",
                "ltp",
                "open_price",
                "high_price",
                "low_price",
                "close_price",
                "volume",
                "bid_price",
                "ask_price",
                "open_interest",
                "message_type",
            ])

    def _init_duckdb(self):
        conn = duckdb.connect(str(self.duckdb_path))
        conn.execute("""
            CREATE TABLE IF NOT EXISTS live_ticks (
                date DATE,
                datetime TIMESTAMP,
                listed_at_ist VARCHAR,
                received_at_ist VARCHAR,
                symbol VARCHAR,
                ltp DOUBLE,
                volume BIGINT,
                bid_price DOUBLE,
                ask_price DOUBLE,
                open_price DOUBLE,
                high_price DOUBLE,
                low_price DOUBLE,
                prev_close_price DOUBLE,
                open_interest BIGINT,
                message_type VARCHAR,
                raw_json VARCHAR
            )
        """)
        conn.execute("ALTER TABLE live_ticks ADD COLUMN IF NOT EXISTS listed_at_ist VARCHAR")
        conn.execute("ALTER TABLE live_ticks ADD COLUMN IF NOT EXISTS received_at_ist VARCHAR")
        conn.close()

    def persist(self, tick):
        row = [
            tick["date"],
            tick["datetime"],
            tick["listed_at_ist"],
            tick["received_at_ist"],
            tick["symbol"],
            tick["ltp"],
            tick["volume"],
            tick["bid_price"],
            tick["ask_price"],
            tick["open_price"],
            tick["high_price"],
            tick["low_price"],
            tick["prev_close_price"],
            tick["open_interest"],
            tick["message_type"],
            tick["raw_json"],
        ]

        with self.lock:
            with self.csv_path.open("a", newline="", encoding="utf-8") as csv_file:
                writer = csv.writer(csv_file)
                writer.writerow(row)

            if self.duckdb_enabled:
                conn = duckdb.connect(str(self.duckdb_path))
                conn.execute("""
                    INSERT INTO live_ticks
                    (date, datetime, listed_at_ist, received_at_ist, symbol, ltp, volume, bid_price, ask_price,
                     open_price, high_price, low_price, prev_close_price,
                     open_interest, message_type, raw_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, row)
                conn.close()

            self._persist_1s_locked(tick)

    def _persist_1s_locked(self, tick):
        second_dt = tick["datetime"].replace(microsecond=0)
        symbol = tick["symbol"]
        bucket = self.pending_1s.get(symbol)

        if bucket and bucket["datetime"] != second_dt:
            self._write_1s_row_locked(bucket)
            bucket = None

        ltp = tick["ltp"]
        if bucket is None:
            bucket = {
                "date": second_dt.date(),
                "datetime": second_dt,
                "listed_at_ist": tick["listed_at_ist"],
                "first_received_at_ist": tick["received_at_ist"],
                "last_received_at_ist": tick["received_at_ist"],
                "symbol": symbol,
                "ltp": ltp,
                "open_price": ltp,
                "high_price": ltp,
                "low_price": ltp,
                "close_price": ltp,
                "volume": tick["volume"],
                "bid_price": tick["bid_price"],
                "ask_price": tick["ask_price"],
                "open_interest": tick["open_interest"],
                "message_type": tick["message_type"],
            }
            self.pending_1s[symbol] = bucket
            return

        if ltp is not None:
            prices = [v for v in (bucket["high_price"], bucket["low_price"], ltp) if v is not None]
            bucket["high_price"] = max(prices) if prices else None
            bucket["low_price"] = min(prices) if prices else None
            bucket["close_price"] = ltp
            bucket["ltp"] = ltp
            if bucket["open_price"] is None:
                bucket["open_price"] = ltp

        for field in ("volume", "bid_price", "ask_price", "open_interest", "message_type"):
            if tick[field] is not None:
                bucket[field] = tick[field]
        bucket["last_received_at_ist"] = tick["received_at_ist"]
        if tick["listed_at_ist"]:
            bucket["listed_at_ist"] = tick["listed_at_ist"]

    def _write_1s_row_locked(self, row):
        out = [
            row["date"],
            row["datetime"],
            row["listed_at_ist"],
            row["first_received_at_ist"],
            row["last_received_at_ist"],
            row["symbol"],
            row["ltp"],
            row["open_price"],
            row["high_price"],
            row["low_price"],
            row["close_price"],
            row["volume"],
            row["bid_price"],
            row["ask_price"],
            row["open_interest"],
            row["message_type"],
        ]
        with self.one_second_csv_path.open("a", newline="", encoding="utf-8") as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(out)

    def flush(self):
        with self.lock:
            for symbol in sorted(self.pending_1s):
                self._write_1s_row_locked(self.pending_1s[symbol])
            self.pending_1s.clear()


def preflight(symbols, strikes: int = 0, expiries: int = 1):
    client_id = load_client_id(LOGIN_FILE)
    access_token = load_access_token(ACCESS_TOKEN_FILE)
    SHARED_LOG_DIR.mkdir(parents=True, exist_ok=True)
    fyers = fyersModel.FyersModel(
        is_async=False,
        client_id=client_id,
        token=access_token,
        log_path=str(SHARED_LOG_DIR),
    )
    profile = fyers.get_profile()
    if str(profile.get("s", "")).lower() != "ok":
        if profile.get("code") == -8 or "expired" in str(profile.get("message", "")).lower():
            raise RuntimeError(
                "FYERS token expired. Regenerate `FYERS/access_token.txt` by running "
                "`python FYERS/login.py`, then start the launcher again."
            )
        raise RuntimeError(f"FYERS token/profile check failed: {profile}")
    if symbols:
        resolved_symbols = symbols
        universe_info = None
    else:
        universe_info = build_nifty_universe(SYMBOL_MASTER_FILE, fyers, strikes=strikes, expiries=expiries)
        resolved_symbols = universe_info["symbols"]
    return {
        "client_id": client_id,
        "symbols": resolved_symbols,
        "profile_name": profile.get("data", {}).get("name") or profile.get("data", {}).get("fy_id"),
        "universe_info": universe_info,
    }


def main():
    ap = argparse.ArgumentParser(description="Capture FYERS websocket market data to dated daily files.")
    ap.add_argument("symbols", nargs="*", help="optional symbols; default resolves nearest NIFTY future")
    ap.add_argument("--date", dest="run_date", help="output date tag YYYY-MM-DD (default: today)")
    ap.add_argument("--preflight", action="store_true", help="validate login/token/symbol resolution and exit")
    ap.add_argument("--verbose", action="store_true", help="print every websocket payload")
    ap.add_argument("--with-order-socket", action="store_true", help="also connect the order socket")
    ap.add_argument("--strikes", type=int, default=15, help="ATM-centered strikes either side for NIFTY options")
    ap.add_argument("--expiries", type=int, default=1, help="number of nearest option expiries to subscribe")
    args = ap.parse_args()

    chk = preflight(args.symbols, strikes=args.strikes, expiries=args.expiries)
    client_id = chk["client_id"]
    access_token = load_access_token(ACCESS_TOKEN_FILE)
    order_access_token = f"{client_id}:{access_token}"
    symbols = chk["symbols"]
    paths = daily_output_paths(args.run_date)

    if args.preflight:
        print("=" * 70)
        print("FYERS PRECHECK OK")
        print("=" * 70)
        print(f"client_id        : {client_id}")
        print(f"profile          : {chk['profile_name']}")
        print(f"symbols count    : {len(symbols)}")
        if chk["universe_info"]:
            info = chk["universe_info"]
            print(f"future symbol    : {info['future_symbol']}")
            print(f"reference price  : {info['reference_price']:.2f}")
            print(f"ATM / step       : {info['atm']:.0f} / {info['step']:.0f}")
            print(f"expiries         : {', '.join(str(e) for e in info['expiries'])}")
        print(f"symbols          : {', '.join(symbols[:8])}" + (" ..." if len(symbols) > 8 else ""))
        print(f"ticks csv        : {paths['ticks_csv']}")
        print(f"1s csv           : {paths['ticks_1s_csv']}")
        print(f"ticks duckdb     : {paths['ticks_duckdb']}")
        return

    storage = TickStorage(paths["ticks_csv"], paths["ticks_duckdb"], paths["ticks_1s_csv"])

    if not symbols:
        raise ValueError("No symbols provided for market data subscription")

    print("=" * 70)
    print("FYERS WEBSOCKET CLIENT")
    print("=" * 70)
    print(f"client_id        : {client_id}")
    print(f"access_token file: {ACCESS_TOKEN_FILE}")
    print(f"symbols count    : {len(symbols)}")
    if chk["universe_info"]:
        info = chk["universe_info"]
        print(f"future symbol    : {info['future_symbol']}")
        print(f"reference price  : {info['reference_price']:.2f}")
        print(f"ATM / step       : {info['atm']:.0f} / {info['step']:.0f}")
        print(f"expiries         : {', '.join(str(e) for e in info['expiries'])}")
    print(f"symbols sample   : {', '.join(symbols[:8])}" + (" ..." if len(symbols) > 8 else ""))
    print("data socket mode : SymbolUpdate")
    print(f"order socket     : {'enabled' if args.with_order_socket else 'disabled'}")
    print(f"ticks csv        : {paths['ticks_csv']}")
    print(f"1s csv           : {paths['ticks_1s_csv']}")
    if storage.duckdb_enabled:
        print(f"ticks duckdb     : {paths['ticks_duckdb']}")
    else:
        print("ticks duckdb     : disabled (install duckdb to enable)")
    print("=" * 70)

    sockets = {}

    def on_data_message(message):
        received_at_ist = datetime.now(IST).isoformat()  # clock receive time first
        if args.verbose:
            pretty_print("DATA", message)
        tick = normalize_tick(message, received_at_ist)
        if tick is not None:
            storage.persist(tick)

    def on_data_error(message):
        pretty_print("DATA ERROR", message)

    def on_data_close(message):
        pretty_print("DATA CLOSE", message)

    def on_order_event(message):
        if args.verbose:
            pretty_print("ORDER EVENT", message)

    def on_trade_event(message):
        if args.verbose:
            pretty_print("TRADE EVENT", message)

    def on_position_event(message):
        if args.verbose:
            pretty_print("POSITION EVENT", message)

    def on_general_event(message):
        if args.verbose:
            pretty_print("GENERAL EVENT", message)

    def on_order_error(message):
        pretty_print("ORDER ERROR", message)

    def on_order_close(message):
        pretty_print("ORDER CLOSE", message)

    def on_data_connect():
        print("\n[DATA CONNECT] Subscribing to symbols...")
        sockets["data"].subscribe(symbols=symbols, data_type="SymbolUpdate")

    def on_order_connect():
        print("\n[ORDER CONNECT] Subscribing to order streams...")
        sockets["order"].subscribe("OnOrders,OnTrades,OnPositions,OnGeneral")

    sockets["data"] = data_ws.FyersDataSocket(
        access_token=access_token,
        write_to_file=False,
        log_path="",
        litemode=False,
        reconnect=True,
        on_connect=on_data_connect,
        on_close=on_data_close,
        on_error=on_data_error,
        on_message=on_data_message,
        reconnect_retry=10,
    )

    if args.with_order_socket:
        sockets["order"] = order_ws.FyersOrderSocket(
            access_token=order_access_token,
            write_to_file=False,
            log_path="",
            on_connect=on_order_connect,
            on_close=on_order_close,
            on_error=on_order_error,
            on_orders=on_order_event,
            on_trades=on_trade_event,
            on_positions=on_position_event,
            on_general=on_general_event,
            reconnect=True,
            reconnect_retry=10,
        )

    def shutdown(signum=None, frame=None):
        print("\nShutting down websocket connections...")
        storage.flush()
        for socket_name in ("data", "order"):
            socket_obj = sockets.get(socket_name)
            if socket_obj is None:
                continue
            try:
                socket_obj.close_connection()
            except Exception as exc:
                print(f"Failed to close {socket_name} socket cleanly: {exc}")
        raise SystemExit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    sockets["data"].connect()
    sockets["data"].keep_running()

    if args.with_order_socket:
        sockets["order"].connect()
        sockets["order"].keep_running()

    print("\nWebsocket connections started. Press Ctrl+C to stop.")

    while True:
        time.sleep(1)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted by user.")
    except Exception as exc:
        print(f"Startup failed: {exc}")
        sys.exit(1)
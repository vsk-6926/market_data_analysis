from __future__ import annotations

import argparse
import atexit
import signal
import subprocess
import sys
import threading
import time
from datetime import date, datetime
from pathlib import Path
from dotenv import dotenv_values


ROOT = Path(__file__).resolve().parent
LOGS_DIR = ROOT / "logs"
PYTHON = sys.executable
FYERS_SCRIPT = ROOT / "FYERS" / "fyers_websocket.py"
DHAN_SCRIPT = ROOT / "Dhan" / "dhan_websocket.py"
NUBRA_SCRIPT = ROOT / "Nubra" / "capture_nifty_websocket.py"
XTS_SCRIPT = ROOT / "XTS" / "xts_websocket.py"
ARROW_SCRIPT = ROOT / "Arrow" / "arrow_websocket.py"
QUANTICK_SCRIPT = ROOT / "Quantick" / "quantick_websocket.py"


class TeeLogger:
    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.handle = self.path.open("a", encoding="utf-8")
        self.lock = threading.Lock()

    def write(self, text: str = "", *, end: str = "\n", flush: bool = True):
        msg = f"{text}{end}"
        with self.lock:
            sys.stdout.write(msg)
            self.handle.write(msg)
            if flush:
                sys.stdout.flush()
                self.handle.flush()

    def close(self):
        with self.lock:
            if not self.handle.closed:
                self.handle.flush()
                self.handle.close()


def _log_path_for_today():
    return LOGS_DIR / f"{date.today().isoformat()}-start.log"


def _print_block(logger: TeeLogger, title: str, body: str):
    logger.write(f"\n[{title}]")
    logger.write(body.rstrip() or "(no output)")


def _run_preflight(logger: TeeLogger, name: str, cmd: list[str], cwd: Path, keep_going: bool = False):
    """Return True if the broker is cleared to start.

    With keep_going the failure is recorded and the broker is dropped, so one
    broker's expired token cannot cost you the other feeds' whole session.
    """
    logger.write(f"Running {name} preflight...")
    proc = subprocess.run(cmd, cwd=str(cwd), capture_output=True, text=True)
    if proc.stdout:
        _print_block(logger, f"{name} stdout", proc.stdout)
    if proc.stderr:
        _print_block(logger, f"{name} stderr", proc.stderr)
    if proc.returncode == 0:
        return True
    if keep_going:
        logger.write(f"!! {name} preflight FAILED (exit {proc.returncode}) - skipping {name}, continuing with the rest.")
        return False
    raise SystemExit(f"{name} preflight failed with exit code {proc.returncode}")


def _check_nubra_env(logger: TeeLogger, env_path: Path):
    cfg = dotenv_values(env_path)
    missing = [k for k in ("PHONE_NO", "MPIN") if not (cfg.get(k) or "").strip()]
    if missing:
        raise SystemExit(f"Nubra config missing in {env_path}: {', '.join(missing)}")
    mode = "totp" if (cfg.get("TOTP_SECRET") or "").strip() else "otp"
    logger.write(f"Nubra config check ok ({env_path}) | suggested login mode: {mode}")


def _relay_output(name: str, proc: subprocess.Popen, logger: TeeLogger):
    prefix = f"[{name}] "
    while True:
        ch = proc.stdout.read(1)
        if ch == "":
            break
        with logger.lock:
            sys.stdout.write(ch)
            logger.handle.write(ch)
            if ch in ("\n", "\r"):
                sys.stdout.flush()
                logger.handle.flush()
    with logger.lock:
        sys.stdout.flush()
        logger.handle.flush()


def _terminate(children: list[tuple[str, subprocess.Popen]], logger: TeeLogger):
    for name, proc in children:
        if proc.poll() is None:
            logger.write(f"Stopping {name} (pid {proc.pid})...")
            proc.terminate()
    deadline = time.time() + 8
    for name, proc in children:
        if proc.poll() is None:
            try:
                proc.wait(timeout=max(0.1, deadline - time.time()))
            except subprocess.TimeoutExpired:
                logger.write(f"Killing {name} (pid {proc.pid})...")
                proc.kill()


def main():
    logger = TeeLogger(_log_path_for_today())
    atexit.register(logger.close)
    logger.write(f"Launcher log: {logger.path}")
    logger.write(f"Started at: {datetime.now().isoformat(timespec='seconds')}")
    ap = argparse.ArgumentParser(description="Start FYERS, Dhan, Nubra, and optional XTS/Arrow/Quantick daily capture with preflight checks.")
    ap.add_argument("symbols", nargs="*", help="optional FYERS symbols; default is nearest NIFTY future")
    ap.add_argument("--skip-preflight", action="store_true", help="skip preflight validation")
    ap.add_argument("--keep-going", action="store_true", help="if one broker fails, drop it and keep the others running")
    ap.add_argument("--skip-non-trading-days", action="store_true", help="exit immediately on weekends and NSE holidays")
    ap.add_argument("--preflight-only", action="store_true", help="run checks only, do not start capture")
    ap.add_argument("--test-seconds", type=int, default=0, help="auto-stop after N seconds for smoke testing")
    ap.add_argument("--startup-grace", type=int, default=8, help="seconds to watch for early process failure")
    ap.add_argument("--fyers-strikes", type=int, default=15, help="ATM-centered FYERS strikes either side")
    ap.add_argument("--fyers-expiries", type=int, default=1, help="nearest FYERS option expiries to subscribe")
    ap.add_argument("--with-xts", action="store_true", help="also start XTS market-data websocket capture")
    ap.add_argument("--with-arrow", action="store_true", help="also start Arrow market-data websocket capture (ds stream)")
    ap.add_argument("--with-arrow-hft", action="store_true", help="also start a second Arrow capture on the low-latency hft stream")
    ap.add_argument("--with-quantick", action="store_true", help="also start Quantick market-data capture (via the new-portal SSH tunnel)")
    ap.add_argument("--quantick-strikes", type=int, default=15, help="ATM-centered Quantick NIFTY strikes either side")
    ap.add_argument("--quantick-until", default="16:05", help="Quantick stop time in IST HH:MM (later than --until so cash closing-session ticks are final)")
    ap.add_argument("--quantick-no-extras", action="store_true", help="Quantick NIFTY only (skip BANKNIFTY/equities/BSE/BFO/MCX)")
    ap.add_argument("--no-fyers", action="store_false", dest="with_fyers", default=True, help="disable FYERS capture")
    ap.add_argument("--no-nubra", action="store_false", dest="with_nubra", default=True, help="disable Nubra capture")
    ap.add_argument("--with-dhan", action="store_true", default=True, help="start Dhan market-data websocket capture (default: enabled)")
    ap.add_argument("--no-dhan", action="store_false", dest="with_dhan", help="disable Dhan market-data websocket capture")
    ap.add_argument("--xts-strikes", type=int, default=15, help="ATM-centered XTS strikes either side")
    ap.add_argument("--xts-expiries", type=int, default=1, help="nearest XTS option expiries to subscribe")
    ap.add_argument("--xts-step", type=int, default=50, help="XTS strike step used for ATM-centered option selection")
    ap.add_argument("--dhan-strikes", type=int, default=15, help="ATM-centered Dhan strikes either side")
    ap.add_argument("--dhan-expiries", type=int, default=1, help="nearest Dhan option expiries to subscribe")
    ap.add_argument("--dhan-fut-months", type=int, default=3, help="how many NIFTY futures to subscribe on Dhan")
    ap.add_argument("--dhan-refresh-master", action="store_true", help="refresh the Dhan security master before starting")
    ap.add_argument("--arrow-strikes", type=int, default=15, help="ATM-centered Arrow strikes either side")
    ap.add_argument("--arrow-expiries", type=int, default=1, help="nearest Arrow option expiries to subscribe")
    ap.add_argument("--arrow-fut-months", type=int, default=3, help="how many NIFTY futures to subscribe on Arrow")
    ap.add_argument("--arrow-stream", default="ds", choices=["ds", "hft"], help="ds = event-driven data stream, hft = throttled low-latency stream")
    ap.add_argument("--arrow-mode", default="FULL", choices=["LTP", "LTPC", "QUOTE", "FULL"], help="Arrow ds stream subscription mode")
    ap.add_argument("--arrow-hft-latency", type=int, default=100, help="Arrow hft stream tick interval in ms")
    ap.add_argument("--arrow-refresh-master", action="store_true", help="refresh the Arrow instrument master before starting")
    ap.add_argument("--nubra-interval", default="1s", choices=["1s", "1m"], help="Nubra websocket aggregation interval")
    ap.add_argument("--nubra-login-mode", default="otp", choices=["otp", "totp"], help="otp prompts once; totp auto-uses TOTP_SECRET")
    ap.add_argument("--nubra-poll", type=int, default=60, help="legacy compatibility flag; ignored by Nubra websocket mode")
    ap.add_argument("--until", default="15:32", help="Nubra stop time in IST HH:MM")
    ap.add_argument("--with-order-socket", action="store_true", help="also start FYERS order socket")
    ap.add_argument("--nubra-csv", action="store_true", help="also save Nubra CSV alongside parquet")
    args = ap.parse_args()

    if args.skip_non_trading_days:
        try:
            from market_calendar import check_trading_day

            should_run, reason = check_trading_day()
            logger.write(f"Trading-day check: {reason}")
            if not should_run:
                logger.write("Nothing to capture today. Exiting.")
                return
        except Exception as exc:
            # Fail open: losing a session is worse than an empty holiday file.
            logger.write(f"Trading-day check unavailable ({exc}); continuing anyway.")

    enabled = {"fyers": args.with_fyers, "nubra": args.with_nubra, "dhan": args.with_dhan,
               "xts": args.with_xts, "arrow": args.with_arrow, "arrow_hft": args.with_arrow_hft,
               "quantick": args.with_quantick}

    fyers_cmd = [
        PYTHON,
        str(FYERS_SCRIPT),
        "--date",
        date.today().isoformat(),
        "--strikes",
        str(args.fyers_strikes),
        "--expiries",
        str(args.fyers_expiries),
        *args.symbols,
    ]
    if args.with_order_socket:
        fyers_cmd.append("--with-order-socket")

    xts_cmd = [
        PYTHON,
        str(XTS_SCRIPT),
        "--date",
        date.today().isoformat(),
        "--strikes",
        str(args.xts_strikes),
        "--expiries",
        str(args.xts_expiries),
        "--strike-step",
        str(args.xts_step),
    ]

    dhan_cmd = [
        PYTHON,
        str(DHAN_SCRIPT),
        "--cred-dir",
        str(ROOT),
        "--out",
        str(ROOT / "Dhan"),
        "--interval",
        "1s",
        "--strikes",
        str(args.dhan_strikes),
        "--expiries",
        str(args.dhan_expiries),
        "--fut-months",
        str(args.dhan_fut_months),
        "--until",
        args.until,
    ]
    if args.dhan_refresh_master:
        dhan_cmd.append("--refresh-master")

    arrow_cmd = [
        PYTHON,
        str(ARROW_SCRIPT),
        "--cred-dir",
        str(ROOT),
        "--out",
        str(ROOT / "Arrow"),
        "--interval",
        "1s",
        "--strikes",
        str(args.arrow_strikes),
        "--expiries",
        str(args.arrow_expiries),
        "--fut-months",
        str(args.arrow_fut_months),
        "--stream",
        args.arrow_stream,
        "--mode",
        args.arrow_mode,
        "--hft-latency",
        str(args.arrow_hft_latency),
        "--until",
        args.until,
    ]
    if args.arrow_refresh_master:
        arrow_cmd.append("--refresh-master")

    # Second Arrow process on the hft stream. It writes arrow_hft_* files, so the
    # two streams never contend for the same CSV or duckdb lock.
    arrow_hft_cmd = [
        PYTHON,
        str(ARROW_SCRIPT),
        "--cred-dir", str(ROOT),
        "--out", str(ROOT / "Arrow"),
        "--interval", "1s",
        "--strikes", str(args.arrow_strikes),
        "--expiries", str(args.arrow_expiries),
        "--fut-months", str(args.arrow_fut_months),
        "--stream", "hft",
        "--hft-latency", str(args.arrow_hft_latency),
        "--until", args.until,
    ]

    quantick_cmd = [
        PYTHON,
        str(QUANTICK_SCRIPT),
        "--cred-dir", str(ROOT),
        "--out", str(ROOT / "Quantick"),
        "--interval", "1s",
        "--strikes", str(args.quantick_strikes),
        "--until", args.quantick_until,
        *(["--no-extras"] if args.quantick_no_extras else []),
    ]

    nubra_cmd = [
        PYTHON,
        str(NUBRA_SCRIPT),
        "--cred-dir",
        str(ROOT),
        "--out",
        str(ROOT / "Nubra"),
        "--login-mode",
        args.nubra_login_mode,
        "--interval",
        args.nubra_interval,
        "--poll",
        str(args.nubra_poll),
        "--until",
        args.until,
    ]
    if args.nubra_csv:
        nubra_cmd.append("--csv")

    if not args.skip_preflight and enabled["fyers"]:
        enabled["fyers"] = _run_preflight(
            logger,
            "FYERS",
            [
                PYTHON,
                str(FYERS_SCRIPT),
                "--preflight",
                "--strikes",
                str(args.fyers_strikes),
                "--expiries",
                str(args.fyers_expiries),
                *args.symbols,
            ],
            ROOT,
            keep_going=args.keep_going,
        )
    if not args.skip_preflight:
        if enabled["nubra"]:
            try:
                _check_nubra_env(logger, ROOT / ".env")
            except SystemExit as exc:
                if not args.keep_going:
                    raise
                logger.write(f"!! Nubra config check FAILED ({exc}) - skipping Nubra, continuing with the rest.")
                enabled["nubra"] = False
        if enabled["dhan"]:
            enabled["dhan"] = _run_preflight(
                logger,
                "Dhan",
                [
                    PYTHON,
                    str(DHAN_SCRIPT),
                    "--preflight",
                    "--cred-dir",
                    str(ROOT),
                    "--out",
                    str(ROOT / "Dhan"),
                    "--strikes",
                    str(args.dhan_strikes),
                    "--expiries",
                    str(args.dhan_expiries),
                    "--fut-months",
                    str(args.dhan_fut_months),
                    *(["--refresh-master"] if args.dhan_refresh_master else []),
                ],
                ROOT,
                keep_going=args.keep_going,
            )
        if args.nubra_login_mode == "totp" and enabled["nubra"]:
            enabled["nubra"] = _run_preflight(logger, "Nubra", [PYTHON, str(NUBRA_SCRIPT), "--preflight", "--cred-dir", str(ROOT), "--interval", args.nubra_interval, "--login-mode", "totp"], ROOT, keep_going=args.keep_going)
        elif enabled["nubra"]:
            logger.write("Skipping live Nubra websocket preflight in OTP mode because it requires an interactive OTP.")
            logger.write("Nubra will prompt once for OTP when the launcher starts the live websocket capture.")
        if enabled["arrow"]:
            enabled["arrow"] = _run_preflight(
                logger,
                "Arrow",
                [
                    PYTHON,
                    str(ARROW_SCRIPT),
                    "--preflight",
                    "--cred-dir",
                    str(ROOT),
                    "--out",
                    str(ROOT / "Arrow"),
                    "--strikes",
                    str(args.arrow_strikes),
                    "--expiries",
                    str(args.arrow_expiries),
                    "--fut-months",
                    str(args.arrow_fut_months),
                    *(["--refresh-master"] if args.arrow_refresh_master else []),
                ],
                ROOT,
                keep_going=args.keep_going,
            )
        if enabled["quantick"]:
            enabled["quantick"] = _run_preflight(
                logger,
                "Quantick",
                [PYTHON, str(QUANTICK_SCRIPT), "--preflight", "--cred-dir", str(ROOT), "--out", str(ROOT / "Quantick"),
                 "--strikes", str(args.quantick_strikes), *(["--no-extras"] if args.quantick_no_extras else [])],
                ROOT,
                keep_going=args.keep_going,
            )
        if enabled["xts"]:
            enabled["xts"] = _run_preflight(
                logger,
                "XTS",
                [
                    PYTHON,
                    str(XTS_SCRIPT),
                    "--preflight",
                    "--strikes",
                    str(args.xts_strikes),
                    "--expiries",
                    str(args.xts_expiries),
                    "--strike-step",
                    str(args.xts_step),
                ],
                ROOT,
                keep_going=args.keep_going,
            )

    if args.preflight_only:
        logger.write("\nPreflight checks passed.")
        return

    logger.write("\nStarting capture processes...")
    commands = [("fyers", fyers_cmd), ("nubra", nubra_cmd), ("dhan", dhan_cmd),
                ("xts", xts_cmd), ("arrow", arrow_cmd), ("arrow_hft", arrow_hft_cmd),
                ("quantick", quantick_cmd)]
    skipped = [name for name, _ in commands if not enabled.get(name)]
    if skipped:
        logger.write(f"Not starting: {', '.join(skipped)}")
    children = []
    for name, cmd in commands:
        if not enabled.get(name):
            continue
        logger.write(f"{name}: " + " ".join(cmd))
        children.append((name, subprocess.Popen(cmd, cwd=str(ROOT), stdout=subprocess.PIPE, stderr=subprocess.STDOUT, stdin=None, text=True, bufsize=0)))
    if not children:
        logger.write("No broker passed preflight. Nothing to capture.")
        return
    relay_threads = []
    for name, proc in children:
        thread = threading.Thread(target=_relay_output, args=(name, proc, logger), daemon=True)
        thread.start()
        relay_threads.append(thread)

    def _handle_signal(signum, frame):
        logger.write(f"\nReceived signal {signum}, shutting down...")
        _terminate(children, logger)
        raise SystemExit(0)

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    start = time.time()
    while time.time() - start < args.startup_grace:
        for name, proc in list(children):
            code = proc.poll()
            if code is not None:
                if not args.keep_going:
                    _terminate(children, logger)
                    raise SystemExit(f"{name} exited during startup with code {code}")
                logger.write(f"!! {name} exited during startup with code {code} - dropping it, others keep running.")
                children.remove((name, proc))
        if not children:
            logger.write("All capture processes failed during startup.")
            return
        time.sleep(0.5)

    running_names = ", ".join(name for name, _ in children)
    logger.write(f"\nCapture processes are running: {running_names}.")
    if args.test_seconds > 0:
        logger.write(f"Test mode active: auto-stopping after {args.test_seconds}s")
        time.sleep(args.test_seconds)
        _terminate(children, logger)
        for thread in relay_threads:
            thread.join(timeout=2)
        logger.write("Test run completed.")
        return

    while True:
        time.sleep(2)
        for name, proc in list(children):
            code = proc.poll()
            if code is None:
                continue
            if not args.keep_going:
                _terminate(children, logger)
                for thread in relay_threads:
                    thread.join(timeout=2)
                raise SystemExit(f"{name} exited unexpectedly with code {code}")
            # A collector that reaches --until exits 0; anything else is a fault
            # worth shouting about, but never a reason to stop the other feeds.
            verdict = "finished" if code == 0 else f"FAILED with code {code}"
            logger.write(f"{name} {verdict}. Remaining: {', '.join(n for n, _ in children if n != name) or 'none'}")
            children.remove((name, proc))
        if not children:
            for thread in relay_threads:
                thread.join(timeout=2)
            logger.write("\nAll capture processes have stopped.")
            return


if __name__ == "__main__":
    main()

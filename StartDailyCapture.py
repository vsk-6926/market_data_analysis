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


def _run_preflight(logger: TeeLogger, name: str, cmd: list[str], cwd: Path):
    logger.write(f"Running {name} preflight...")
    proc = subprocess.run(cmd, cwd=str(cwd), capture_output=True, text=True)
    if proc.stdout:
        _print_block(logger, f"{name} stdout", proc.stdout)
    if proc.stderr:
        _print_block(logger, f"{name} stderr", proc.stderr)
    if proc.returncode != 0:
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
    ap = argparse.ArgumentParser(description="Start FYERS, Dhan, Nubra, and optional XTS daily capture with preflight checks.")
    ap.add_argument("symbols", nargs="*", help="optional FYERS symbols; default is nearest NIFTY future")
    ap.add_argument("--skip-preflight", action="store_true", help="skip preflight validation")
    ap.add_argument("--preflight-only", action="store_true", help="run checks only, do not start capture")
    ap.add_argument("--test-seconds", type=int, default=0, help="auto-stop after N seconds for smoke testing")
    ap.add_argument("--startup-grace", type=int, default=8, help="seconds to watch for early process failure")
    ap.add_argument("--fyers-strikes", type=int, default=15, help="ATM-centered FYERS strikes either side")
    ap.add_argument("--fyers-expiries", type=int, default=1, help="nearest FYERS option expiries to subscribe")
    ap.add_argument("--with-xts", action="store_true", help="also start XTS market-data websocket capture")
    ap.add_argument("--with-dhan", action="store_true", default=True, help="start Dhan market-data websocket capture (default: enabled)")
    ap.add_argument("--no-dhan", action="store_false", dest="with_dhan", help="disable Dhan market-data websocket capture")
    ap.add_argument("--xts-strikes", type=int, default=15, help="ATM-centered XTS strikes either side")
    ap.add_argument("--xts-expiries", type=int, default=1, help="nearest XTS option expiries to subscribe")
    ap.add_argument("--xts-step", type=int, default=50, help="XTS strike step used for ATM-centered option selection")
    ap.add_argument("--dhan-strikes", type=int, default=15, help="ATM-centered Dhan strikes either side")
    ap.add_argument("--dhan-expiries", type=int, default=1, help="nearest Dhan option expiries to subscribe")
    ap.add_argument("--dhan-fut-months", type=int, default=3, help="how many NIFTY futures to subscribe on Dhan")
    ap.add_argument("--dhan-refresh-master", action="store_true", help="refresh the Dhan security master before starting")
    ap.add_argument("--nubra-interval", default="1s", choices=["1s", "1m"], help="Nubra websocket aggregation interval")
    ap.add_argument("--nubra-login-mode", default="otp", choices=["otp", "totp"], help="otp prompts once; totp auto-uses TOTP_SECRET")
    ap.add_argument("--nubra-poll", type=int, default=60, help="legacy compatibility flag; ignored by Nubra websocket mode")
    ap.add_argument("--until", default="15:32", help="Nubra stop time in IST HH:MM")
    ap.add_argument("--with-order-socket", action="store_true", help="also start FYERS order socket")
    ap.add_argument("--nubra-csv", action="store_true", help="also save Nubra CSV alongside parquet")
    args = ap.parse_args()

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

    if not args.skip_preflight:
        _run_preflight(
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
        )
        _check_nubra_env(logger, ROOT / ".env")
        if args.with_dhan:
            _run_preflight(
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
            )
        if args.nubra_login_mode == "totp":
            _run_preflight(logger, "Nubra", [PYTHON, str(NUBRA_SCRIPT), "--preflight", "--cred-dir", str(ROOT), "--interval", args.nubra_interval, "--login-mode", "totp"], ROOT)
        else:
            logger.write("Skipping live Nubra websocket preflight in OTP mode because it requires an interactive OTP.")
            logger.write("Nubra will prompt once for OTP when the launcher starts the live websocket capture.")
        if args.with_xts:
            _run_preflight(
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
            )

    if args.preflight_only:
        logger.write("\nPreflight checks passed.")
        return

    logger.write("\nStarting capture processes...")
    logger.write("FYERS: " + " ".join(fyers_cmd))
    if args.with_dhan:
        logger.write("Dhan: " + " ".join(dhan_cmd))
    logger.write("Nubra: " + " ".join(nubra_cmd))
    if args.with_xts:
        logger.write("XTS: " + " ".join(xts_cmd))
    children = [
        ("fyers", subprocess.Popen(fyers_cmd, cwd=str(ROOT), stdout=subprocess.PIPE, stderr=subprocess.STDOUT, stdin=None, text=True, bufsize=0)),
        ("nubra", subprocess.Popen(nubra_cmd, cwd=str(ROOT), stdout=subprocess.PIPE, stderr=subprocess.STDOUT, stdin=None, text=True, bufsize=0)),
    ]
    if args.with_dhan:
        children.append(("dhan", subprocess.Popen(dhan_cmd, cwd=str(ROOT), stdout=subprocess.PIPE, stderr=subprocess.STDOUT, stdin=None, text=True, bufsize=0)))
    if args.with_xts:
        children.append(("xts", subprocess.Popen(xts_cmd, cwd=str(ROOT), stdout=subprocess.PIPE, stderr=subprocess.STDOUT, stdin=None, text=True, bufsize=0)))
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
        for name, proc in children:
            code = proc.poll()
            if code is not None:
                _terminate(children, logger)
                raise SystemExit(f"{name} exited during startup with code {code}")
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
        for name, proc in children:
            code = proc.poll()
            if code is not None:
                _terminate(children, logger)
                for thread in relay_threads:
                    thread.join(timeout=2)
                raise SystemExit(f"{name} exited unexpectedly with code {code}")


if __name__ == "__main__":
    main()

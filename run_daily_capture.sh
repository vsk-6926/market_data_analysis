#!/bin/zsh
# Daily NIFTY market-data capture, started by launchd on weekday mornings.
#
# Launches at 09:05 IST so logins and preflight finish before the open; each
# collector then waits for 09:15 on its own and stops at --until.
#
# Run by hand with:  ./run_daily_capture.sh
# Logs:              logs/capture-<date>.log  (plus logs/<date>-start.log)

set -u

ROOT="/Users/vanshsameer/Desktop/Market Data"
PYTHON="$ROOT/.venv/bin/python"
cd "$ROOT" || exit 1

mkdir -p logs
DAY="$(date +%Y-%m-%d)"
LOG="$ROOT/logs/capture-$DAY.log"

echo "=========================================================" >> "$LOG"
echo "launch: $(date '+%Y-%m-%d %H:%M:%S %Z')" >> "$LOG"

# --keep-going            one broker's expired token must not cost the others
# --skip-non-trading-days weekends and NSE holidays exit immediately
# --until 15:32           a couple of minutes past the 15:30 close, to flush
"$PYTHON" -u StartDailyCapture.py \
    --with-arrow \
    --with-dhan \
    --nubra-login-mode totp \
    --nubra-interval 1s \
    --keep-going \
    --skip-non-trading-days \
    --fyers-strikes 15 \
    --dhan-strikes 15 \
    --arrow-strikes 15 \
    --until 15:32 >> "$LOG" 2>&1

STATUS=$?
echo "exit: $STATUS at $(date '+%Y-%m-%d %H:%M:%S %Z')" >> "$LOG"
exit $STATUS

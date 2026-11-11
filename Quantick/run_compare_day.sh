#!/bin/zsh
# One unattended day: capture Quantick + Arrow (ds, hft) + Nubra, then build the reports.
# Start it any time before 09:00 under caffeinate so the Mac does not idle-sleep:
#   nohup caffeinate -i zsh Quantick/run_compare_day.sh >> logs/quantick-auto-$(date +%F).log 2>&1 &
cd "/Users/vanshsameer/Desktop/Market Data" || exit 1
source .venv/bin/activate
DAY=$(date +%F)

wait_until() {
  local target=$(date -j -f "%Y-%m-%d %H:%M" "$DAY $1" +%s)
  echo "[$(date +%T)] waiting until $1"
  while [ "$(date +%s)" -lt "$target" ]; do sleep 30; done
}

wait_until 09:00
echo "[$(date +%T)] starting capture"
python StartDailyCapture.py --keep-going --skip-non-trading-days --no-fyers --no-dhan \
  --with-arrow --with-arrow-hft --with-quantick --nubra-login-mode totp

echo "[$(date +%T)] capture finished; live comparison"
python Quantick/compare_feeds.py live --date "$DAY"

# NSE publishes the F&O bhavcopy in the evening.
wait_until 19:30
echo "[$(date +%T)] EOD + historical comparison"
python Quantick/compare_feeds.py all --date "$DAY"
echo "[$(date +%T)] done: Quantick/reports/$DAY/summary.md"

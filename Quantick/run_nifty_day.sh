#!/bin/zsh
# NIFTY options + futures day in the Market Data bench: Quantick vs Nubra, plus
# arrival on this Mac vs each tick's last-traded time (LTT). Live ticks only.
# Start any time before 09:05, Mac on charger, lid open:
#   nohup caffeinate -i zsh Quantick/run_nifty_day.sh >> logs/nifty-day-$(date +%F).log 2>&1 &
cd "/Users/vanshsameer/Desktop/Market Data" || exit 1
source .venv/bin/activate
DAY=$(date +%F)

wait_until() {
  local target=$(date -j -f "%Y-%m-%d %H:%M" "$DAY $1" +%s)
  echo "[$(date +%T)] waiting until $1"
  while [ "$(date +%s)" -lt "$target" ]; do sleep 30; done
}

wait_until 09:05
echo "[$(date +%T)] starting capture"
python StartDailyCapture.py --keep-going --skip-non-trading-days --no-dhan --no-fyers \
  --with-quantick --quantick-no-extras --quantick-until 15:35 --nubra-login-mode totp

echo "[$(date +%T)] capture finished; reports"
python Quantick/compare_feeds.py live --date "$DAY"

echo "[$(date +%T)] done: Quantick/reports/$DAY/"

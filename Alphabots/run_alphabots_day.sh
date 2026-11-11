#!/bin/zsh
# Quantick vs the ALPHABOTS NSE tick-by-tick feed, NIFTY options + futures, live ticks only.
# Start any time before 08:58, Mac on charger, lid open:
#   nohup caffeinate -is zsh Alphabots/run_alphabots_day.sh >> logs/alphabots-day-$(date +%F).log 2>&1 &
cd "/Users/vanshsameer/Desktop/Market Data" || exit 1
source .venv/bin/activate
DAY=$(date +%F)

wait_until() {
  local target=$(date -j -f "%Y-%m-%d %H:%M" "$DAY $1" +%s)
  echo "[$(date +%T)] waiting until $1"
  while [ "$(date +%s)" -lt "$target" ]; do sleep 30; done
}

python -c "from market_calendar import check_trading_day; import sys; ok, why = check_trading_day(); print(why); sys.exit(0 if ok else 1)" || exit 0

wait_until 08:58
# The Alphabots feed opens its session at 09:00; connect before so the snapshot is in place.
echo "[$(date +%T)] starting Alphabots capture"
python Alphabots/alphabots_capture.py --until 15:35 >> "logs/alphabots-capture-$DAY.log" 2>&1 &
ALPHA=$!

# Quantick: same expiry as the Alphabots feed (nearest NIFTY weekly), ATM +/-30 in 50-point steps so it
# covers the feed's 100-point strikes around the money; no Nubra/Dhan/FYERS today.
echo "[$(date +%T)] starting Quantick capture"
python StartDailyCapture.py --keep-going --skip-non-trading-days --no-dhan --no-fyers --no-nubra \
  --with-quantick --quantick-no-extras --quantick-strikes 30 --quantick-until 15:35

wait $ALPHA
echo "[$(date +%T)] captures finished; live comparison"
python TBT/compare_tbt.py --date "$DAY" --ref alphabots

# EOD vs live needs NSE's F&O bhavcopy (published in the evening or late night).
for i in {1..12}; do
  python Quantick/eod_live_vs_official.py --date "$DAY" && break
  sleep 1800
done
echo "[$(date +%T)] done: Quantick/reports/$DAY/summary_alphabots.md"

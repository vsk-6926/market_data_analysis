#!/bin/zsh
# Morning-only re-test (09:15-12:00): Quantick vs Alphabots ticks + Quantick 1-minute candle API
# after Quantick switched their candles to "immediate" (21 Oct evening).
#   nohup caffeinate -is zsh Alphabots/run_until_noon.sh >> logs/noon-run-$(date +%F).log 2>&1 &
cd "/Users/vanshsameer/Desktop/Market Data" || exit 1
source .venv/bin/activate
DAY=$(date +%F)
STOP=12:00

wait_until() {
  local target=$(date -j -f "%Y-%m-%d %H:%M" "$DAY $1" +%s)
  echo "[$(date +%T)] waiting until $1"
  while [ "$(date +%s)" -lt "$target" ]; do sleep 30; done
}

python -c "from market_calendar import check_trading_day; import sys; ok, why = check_trading_day(); print(why); sys.exit(0 if ok else 1)" || exit 0

# Clock offset every 5 min, so absolute delays can be corrected (the Mac runs ~40-65 ms behind NTP).
zsh -c "while [ \"\$(date +%H%M)\" -lt 1205 ]; do for s in time.apple.com time.google.com; do echo \"\$(date +%Y-%m-%dT%H:%M:%S%z) \$(sntp -t 3 \$s 2>&1 | tail -1)\"; done; sleep 300; done" >> "logs/clock-offset-$DAY.log" 2>&1 &

wait_until 08:58
echo "[$(date +%T)] starting Alphabots capture"
python Alphabots/alphabots_capture.py --until $STOP >> "logs/alphabots-capture-$DAY.log" 2>&1 &
ALPHA=$!

echo "[$(date +%T)] starting Quantick capture"
python StartDailyCapture.py --keep-going --skip-non-trading-days --no-dhan --no-fyers --no-nubra \
  --with-quantick --quantick-no-extras --quantick-strikes 30 --quantick-until $STOP --until $STOP &
QUANT=$!

# Quantick 1-minute candle API: is a candle final as soon as it appears now? 3 series, own tunnel port.
wait_until 09:14
echo "[$(date +%T)] starting Quantick candle poll"
QUANTICK_TUNNEL_PORT=18084 python Quantick/candle_latency.py poll --sources quantick --roles FUT ATM_CE ATM_PE --until $STOP \
  >> "logs/quantick-candle-poll-$DAY.log" 2>&1 &
POLL=$!

wait $ALPHA $QUANT $POLL
echo "[$(date +%T)] captures finished; reports"
python TBT/compare_tbt.py --date "$DAY" --ref alphabots
QUANTICK_TUNNEL_PORT=18083 python TBT/candle_reconcile.py --date "$DAY"
python Quantick/vendor_report.py --date "$DAY"
python Quantick/candle_latency.py report --date "$DAY"
echo "[$(date +%T)] reports done: Quantick/reports/$DAY/"

# EOD vs official needs NSE's F&O bhavcopy (evening / night).
for i in {1..16}; do
  python Quantick/eod_live_vs_official.py --date "$DAY" && break
  sleep 1800
done
echo "[$(date +%T)] all done"

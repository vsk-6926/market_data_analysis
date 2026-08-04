# Debug Session: dhan-live-verify
- **Status**: [OPEN]
- **Issue**: Verify live Dhan credentials/feed, start Dhan collector, confirm 1s CSV ingestion/UI visibility, and assess/add Dhan plus Nubra support in Paired Compare.
- **Debug Server**: Pending
- **Log File**: .dbg/trae-debug-log-dhan-live-verify.ndjson

## Reproduction Steps
1. Inspect current Dhan, Nubra, and Paired Compare wiring.
2. Run a Dhan credential/feed preflight using the configured root `.env`.
3. Start the Dhan collector and verify a fresh 1s CSV is produced.
4. Verify the UI can query the generated data end to end.
5. Extend Paired Compare to include Dhan, and Nubra 1s support if the feed provides it.

## Hypotheses & Verification
| ID | Hypothesis | Likelihood | Effort | Evidence |
|----|------------|------------|--------|----------|
| A | Dhan credentials in root `.env` are valid, but runtime failures may still occur due to instrument subscription or websocket auth shape. | High | Low | Pending |
| B | The Dhan collector already writes 1s bars or is close, and the main missing piece is UI/query wiring. | High | Medium | Pending |
| C | Paired Compare is hard-coded to FYERS/Nubra source lists and needs both backend and frontend option expansion for Dhan. | High | Medium | Pending |
| D | Nubra exposes live ticks, but 1s availability may currently be synthesized locally rather than delivered natively. | Medium | Medium | Pending |
| E | End-to-end verification may fail because the server only enumerates known source directories/files and does not yet include Dhan. | High | Medium | Pending |

## Log Evidence
- `python3 Dhan/dhan_websocket.py --preflight ...` failed in the shell environment first because global `python3` did not have `pandas`; rerunning with `.venv/bin/python` used the project interpreter correctly.
- Pre-fix Dhan preflight reached credentials and downloaded `Dhan/api-scrip-master-detailed.csv`, but failed with `Could not resolve a Dhan reference price for the nearest NIFTY future.`
- Direct Dhan REST probes for the chosen `NIFTY AUG FUT` security returned `{"status":"failure","data":""}` for `ticker_data`, `quote_data`, and `ohlc_data`.
- Direct websocket connect probe using the same credentials succeeded (`CONNECTED` / `CLOSED`), showing the runtime risk is not basic websocket auth.
- Post-fix Dhan preflight succeeded by falling back to a `security_master_midpoint` reference price, resolving ATM `24650` and a 65-instrument NIFTY universe.
- The live Dhan collector now starts successfully and is waiting for `09:15 IST` before it can produce real ticks/1s rows.

## Verification Conclusion
- Confirmed:
  - Hypothesis A: credentials are usable, but the REST quote path was the failing sub-step.
  - Hypothesis C: Paired Compare needed explicit expansion beyond FYERS/Nubra.
  - Hypothesis D: Nubra already provides live websocket ticks and a local 1-second rollup path.
  - Hypothesis E: default-path discovery needed updates, especially to avoid selecting Dhan's security-master CSV.
- Remaining open runtime check:
  - Real Dhan tick arrival and UI visibility cannot be fully verified until market hours because the collector is currently waiting for market open.

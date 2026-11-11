NIFTY options, 1-minute candles, 1 Jan 2025 - 31 Oct 2026
==========================================================

Source: Dhan Data API, expired options endpoint (POST /v2/charts/rollingoption), pulled 11-12 Nov 2026.

HOW TO OPEN
-----------
The zip is split into parts (GitHub allows max 100 MB per file). Download ALL parts
(NIFTY_options_1min.z01, .z02, ... and NIFTY_options_1min.zip) into one folder, then:
  - Windows: open NIFTY_options_1min.zip with 7-Zip or WinRAR (Explorer can't open split zips).
  - Mac/Linux:  zip -s 0 NIFTY_options_1min.zip --out full.zip && unzip full.zip

LAYOUT
------
<YYYY-MM>/NIFTY_<YYYY-MM-DD>_1m.csv   one file per trading day (09:15-15:29 IST)
Market holidays have a header-only (empty) file.

COLUMNS
-------
datetime      candle start, IST (YYYY-MM-DD HH:MM:SS)
underlying    NIFTY
series        CW = current week, NW = next week, CM = current month, NM = next month expiry
strike_label  ATM, ATM-1 .. ATM-10, ATM+1 .. ATM+10 (relative to spot, re-centred EVERY MINUTE)
option_type   CE / PE
strike        actual strike of that row's contract
spot          NIFTY spot at that minute
open, high, low, close, volume, oi, iv

NOTES / LIMITATIONS
-------------------
1. Strikes are ATM-relative, re-centred each minute (Dhan's API design). Use the `strike` column
   to rebuild a fixed contract's series; strikes far from spot only have the minutes they were
   within the range.
2. From late Nov 2025, Dhan returns only ATM-3..ATM+3 for NM (next month) on all days and for
   NW (next week) on roughly half the days. CW and CM are ATM+-10 throughout.
3. Minutes with no trades are absent (not forward-filled).
4. iv is 0 for some deep in-the-money strikes.
5. No expiry-date column (Dhan does not return it); derive from the NSE expiry calendar if needed.

NIFTY options ATM +/-10, 1-minute candles, 1 Jan 2025 - 31 Oct 2026  (for Sudhir)
===============================================================================

Source: Dhan Data API expired-options endpoint (rollingoption), pulled 11-12 Nov 2026,
with the strikes Dhan could not provide filled from the Fyers expired F&O API
(pulled 13-14 Nov 2026). Version 2 (14 Nov 2026): replaces the earlier Dhan-only zip.

HOW TO OPEN
-----------
The zip is split into parts (GitHub allows max 100 MB per file). Download ALL parts
(Sudhir_NIFTY_ATM10_1min.z01, .z02, ... and Sudhir_NIFTY_ATM10_1min.zip) into one folder, then:
  - Windows: open the .zip with 7-Zip or WinRAR (Explorer can't open split zips).
  - Mac/Linux:  zip -s 0 Sudhir_NIFTY_ATM10_1min.zip --out full.zip && unzip full.zip

LAYOUT
------
<YYYY-MM>/NIFTY_<YYYY-MM-DD>_1m.csv   one file per trading day (09:15-15:29 IST)
Market holidays have a header-only (empty) file.

COLUMNS
-------
datetime      candle start, IST (YYYY-MM-DD HH:MM:SS)
underlying    NIFTY
series        CW = current week, NW = next week, CM = current month, NM = next month expiry
strike_label  ATM, ATM-1 .. ATM-10, ATM+1 .. ATM+10 (ATM = spot rounded to 50, re-centred EVERY MINUTE)
option_type   CE / PE
strike        actual strike of that row's contract
spot          NIFTY spot at that minute
open, high, low, close, volume, oi, iv

NOTES
-----
1. Strikes are ATM-relative, re-centred each minute. Use the `strike` column to rebuild a
   fixed contract's series; strikes far from spot only have the minutes they were in range.
2. All four series have ATM-10..ATM+10 on every trading day. Dhan only returned ATM+-3 for NM
   (from late Nov 2025) and for NW on about half the days; those ATM+-4..+-10 rows
   come from Fyers per-contract data (2.07 million rows), mapped to the same labels with the
   same ATM rule. Where both sources traded in the same minute, closes agree within 1% for
   99.7% of rows. Exception: 8 day/side combinations of NM miss 1-2 far labels that did not
   trade at all that day.
3. Treat volume = 0 as "no trade in that minute". Dhan omits many no-trade minutes but
   keeps some with volume 0; the Fyers-filled rows include traded minutes only.
4. April 2025 fix: Dhan used the 30 Apr 2025 WEEKLY contract as the April monthly (from 21 Mar
   for NM, from 28 Mar for CM). The real April monthly expired 24 Apr (NIFTY25APR...). For
   NM on 21-27 Mar 2025, CM on 28 Mar-30 Apr 2025 and NM on 25-30 Apr 2025, the whole series
   (ATM-10..ATM+10) was rebuilt from Fyers data on the correct contracts.
5. Removed from the original Dhan data: 348,581 no-trade (volume 0) rows that carried a wrong
   spot and strike (BANKNIFTY-level, ~50,000); where Dhan had wrong-spot minutes for NW/NM,
   the labels for those minutes come from Fyers.
6. iv is 0 for some deep in-the-money strikes. On Fyers-filled rows iv is Fyers' value.
7. No expiry-date column (Dhan does not return it).

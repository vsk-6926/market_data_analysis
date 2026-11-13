NIFTY full option chain, 1-minute candles, 1 Jan 2025 - 31 Oct 2026  (for Krish)
==========================================================================

Source: Fyers expired F&O history API (per contract), pulled 13-14 Nov 2026.
Contracts that had not expired at pull time (23 Nov and 29 Dec 2026 expiries) come
from Fyers' regular history API.

HOW TO OPEN
-----------
The zip is split into parts (GitHub allows max 100 MB per file). Download ALL parts
(Krish_NIFTY_option_chain_1min.z01, .z02, ... and Krish_NIFTY_option_chain_1min.zip)
into one folder, then:
  - Windows: open the .zip with 7-Zip or WinRAR (Explorer can't open split zips).
  - Mac/Linux:  zip -s 0 Krish_NIFTY_option_chain_1min.zip --out full.zip && unzip full.zip

LAYOUT
------
<YYYY-MM>/NIFTY_<YYYY-MM-DD>_1m.csv   one file per trading day (454 days)

WHAT EACH DAY CONTAINS
----------------------
- Expiries: CW = current week, NW = next week, CM = current month, NM = next month,
  as seen on that day (on expiry day, CW / CM is the contract expiring that day).
- Strikes: every 50-point strike from (day's NIFTY low - 600) to (day's NIFTY high + 600).
- CE and PE for every strike/expiry. Fixed contracts (not ATM-relative), so each row is
  one real instrument; the `symbol` column is the Fyers symbol.

COLUMNS
-------
datetime      candle start, IST (YYYY-MM-DD HH:MM:SS)
underlying    NIFTY
series        CW / NW / CM / NM. When two series are the same contract (e.g. in the last
              week of a month the next-week contract is also the monthly), it appears ONCE
              with a combined tag such as "NW|CM" or "CW|CM".
expiry        contract expiry date
strike        strike price
option_type   CE / PE
symbol        Fyers symbol, e.g. NSE:NIFTY25APR23000CE (monthly) or NSE:NIFTY2510223000CE (weekly)
spot          NIFTY 50 index close for that minute
open, high, low, close, volume, oi, iv

NOTES
-----
1. Fyers forward-fills minutes with no trade: a flat candle (O=H=L=C = previous close) with
   volume 0. Treat volume = 0 as "no trade in that minute" (~30% of rows).
2. Strikes far from spot sometimes never traded on a given day; they are then absent.
   Average coverage of wanted strikes: CW 100%, NW 99.5%, CM 99.8%, NM 96.6%
   (far-month strikes trade thinly; lowest NM day 72%).
3. Contracts still live at pull time (23 Nov / 29 Dec 2026 expiries) can have candles up to
   15:39 (closing session) on the last few days of October 2026.
4. April 2025: the 1 May weekly expiry was moved to Wed 30 Apr (holiday). The monthly is
   correctly taken as 24 Apr (NIFTY25APR...).

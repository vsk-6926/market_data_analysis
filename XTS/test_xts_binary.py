"""Validate the XTS binary-packet parser against synthetic packets.

Layout is reconstructed from the official SDK's TouchlineEvent / MarketDepthEvent /
OpenInterestEvent deserializers and the MarketdataSocketExample framing loop.
All integers little-endian.
"""
import struct
import zlib

# ---- BinaryReader ---------------------------------------------------------
class BinaryReader:
    def __init__(self, data):
        self.buf = memoryview(bytes(data))
        self.pos = 0

    def _read(self, fmt, size):
        v = struct.unpack_from(fmt, self.buf, self.pos)[0]
        self.pos += size
        return v

    def read_int8(self):   return self._read('<b', 1)
    def read_uint8(self):  return self._read('<B', 1)
    def read_int16(self):  return self._read('<h', 2)
    def read_uint16(self): return self._read('<H', 2)
    def read_int32(self):  return self._read('<i', 4)
    def read_uint32(self): return self._read('<I', 4)
    def read_int64(self):  return self._read('<q', 8)
    def read_uint64(self): return self._read('<Q', 8)
    def read_double(self): return self._read('<d', 8)
    def remaining(self):   return len(self.buf) - self.pos

MSG_VERSION_SEQ = 4  # ApplicationMessageVersion.Version_1_0_1_2983

def _read_depth_row(r):
    size = r.read_int32()
    price = r.read_double()
    orders = r.read_int32()
    bmm = r.read_int16()
    return {"size": size, "price": price, "orders": orders, "bmm": bmm}

def _read_common_head(r):
    mv = r.read_uint16()
    _app = r.read_uint16()
    _tok = r.read_uint64()
    if mv >= MSG_VERSION_SEQ:
        r.read_uint64()  # sequenceNumber
        r.read_int32()   # skipBytes
    seg = r.read_int16()
    instr = r.read_int32()
    ts = r.read_uint64()
    return mv, seg, instr, ts

def parse_touchline(r):
    mv, seg, instr, ts = _read_common_head(r)
    bid = _read_depth_row(r)
    ask = _read_depth_row(r)
    lut = r.read_uint64()
    ltp = r.read_double()
    ltq = r.read_int32()
    tbq = r.read_uint32()
    tsq = r.read_uint32()
    ttq = r.read_uint32()
    atp = r.read_double()
    ltt = r.read_int64()
    pct = r.read_double()
    o = r.read_double(); h = r.read_double(); l = r.read_double(); c = r.read_double()
    tvt = r.read_double()
    return {
        "code": 1501, "seg": seg, "instr": instr, "exch_ts": ts, "lut": lut,
        "ltp": ltp, "ltq": ltq, "bid": bid["price"], "ask": ask["price"],
        "bid_qty": bid["size"], "ask_qty": ask["size"],
        "tbq": tbq, "tsq": tsq, "ttq": ttq, "atp": atp, "ltt": ltt,
        "open": o, "high": h, "low": l, "close": c, "tvt": tvt,
    }

def parse_oi(r):
    mv = r.read_uint16(); r.read_uint16(); r.read_uint64()
    if mv >= MSG_VERSION_SEQ:
        r.read_uint64(); r.read_int32()
    seg = r.read_int16()
    instr = r.read_int32()
    ts = r.read_uint64()
    r.read_int16()  # MarketType
    oi = r.read_int32()
    return {"code": 1510, "seg": seg, "instr": instr, "exch_ts": ts, "oi": oi}

def parse_packets(data):
    buf = bytes(data)
    n = len(buf)
    offset = 0
    out = []
    while offset < n:
        gzip_flag = buf[offset]; offset += 1
        if offset + 16 > n:
            break
        code, seg, instr, book, mtype, unc, comp = struct.unpack_from('<HhihhHH', buf, offset)
        offset += 16
        try:
            if gzip_flag == 1:
                body = zlib.decompressobj(-15).decompress(buf[offset:offset + comp])
                offset += comp
                r = BinaryReader(body)
                r.read_uint16()  # messageCode repeated at head of decompressed body
            else:
                body = buf[offset:offset + unc]
                offset += unc
                r = BinaryReader(body)
            if code == 1501:
                out.append(parse_touchline(r))
            elif code == 1510:
                out.append(parse_oi(r))
        except (struct.error, zlib.error):
            break
    return out

# ---- Builders (mirror the layout to round-trip) ---------------------------
def build_touchline_body(instr, exch_ts, ltp, bid, ask, o, h, l, c, ttq, mv=4):
    b = b''
    b += struct.pack('<H', mv)          # messageVersion
    b += struct.pack('<H', 0)           # applicationType
    b += struct.pack('<Q', 0)           # tokenID
    if mv >= 4:
        b += struct.pack('<Q', 1)       # sequenceNumber
        b += struct.pack('<i', 0)       # skipBytes
    b += struct.pack('<h', 2)           # exchangeSegment (NSEFO=2)
    b += struct.pack('<i', instr)       # exchangeInstrumentID
    b += struct.pack('<Q', exch_ts)     # exchangeTimestamp
    for price, size in ((bid, 100), (ask, 150)):  # bid row, ask row
        b += struct.pack('<i', size) + struct.pack('<d', price) + struct.pack('<i', 3) + struct.pack('<h', 0)
    b += struct.pack('<Q', exch_ts)     # lut
    b += struct.pack('<d', ltp)         # LTP
    b += struct.pack('<i', 50)          # ltq
    b += struct.pack('<I', 1000)        # totalBuyQuantity
    b += struct.pack('<I', 2000)        # totalSellQuantity
    b += struct.pack('<I', ttq)         # totalTradedQuantity
    b += struct.pack('<d', ltp)         # averageTradedPrice
    b += struct.pack('<q', exch_ts)     # lastTradedTime
    b += struct.pack('<d', 1.23)        # percentChange
    b += struct.pack('<d', o) + struct.pack('<d', h) + struct.pack('<d', l) + struct.pack('<d', c)
    b += struct.pack('<d', 99999.0)     # totalvaluetraded
    b += struct.pack('<h', 0) + struct.pack('<h', 0) + struct.pack('<h', 1) + struct.pack('<h', 1)
    return b

def frame(code, body, gzip=False):
    if gzip:
        co = zlib.compressobj(9, zlib.DEFLATED, -15)
        comp = co.compress(struct.pack('<H', code) + body) + co.flush()
        header = struct.pack('<HhihhHH', code, 2, 0, 0, 1, len(body), len(comp))
        return struct.pack('<B', 1) + header + comp
    else:
        header = struct.pack('<HhihhHH', code, 2, 0, 0, 1, len(body), 0)
        return struct.pack('<B', 0) + header + body

# ---- Tests ----------------------------------------------------------------
def main():
    body = build_touchline_body(48225, 1785400000, ltp=114.5, bid=114.4, ask=114.6,
                                o=120.0, h=125.0, l=110.0, c=114.5, ttq=1234567)
    # single uncompressed
    pkt = frame(1501, body, gzip=False)
    res = parse_packets(pkt)
    assert len(res) == 1, res
    t = res[0]
    assert t["instr"] == 48225 and abs(t["ltp"] - 114.5) < 1e-9
    assert abs(t["bid"] - 114.4) < 1e-9 and abs(t["ask"] - 114.6) < 1e-9
    assert t["exch_ts"] == 1785400000 and t["ttq"] == 1234567
    assert abs(t["high"] - 125.0) < 1e-9 and abs(t["low"] - 110.0) < 1e-9
    print("uncompressed touchline OK:", {k: t[k] for k in ("instr","ltp","bid","ask","high","low","exch_ts")})

    # gzip-compressed
    pkt_gz = frame(1501, body, gzip=True)
    res2 = parse_packets(pkt_gz)
    assert len(res2) == 1 and res2[0]["instr"] == 48225 and abs(res2[0]["ltp"] - 114.5) < 1e-9
    print("gzip touchline OK:", {k: res2[0][k] for k in ("instr","ltp","exch_ts")})

    # multiple packets concatenated in one frame
    body2 = build_touchline_body(48226, 1785400005, ltp=55.5, bid=55.4, ask=55.6,
                                 o=60, h=61, l=54, c=55.5, ttq=42)
    multi = frame(1501, body, False) + frame(1501, body2, False)
    res3 = parse_packets(multi)
    assert len(res3) == 2, [r["instr"] for r in res3]
    assert res3[0]["instr"] == 48225 and res3[1]["instr"] == 48226
    print("multi-packet OK:", [r["instr"] for r in res3])

    # OI packet
    oi_body = b''
    oi_body += struct.pack('<H', 4) + struct.pack('<H', 0) + struct.pack('<Q', 0)
    oi_body += struct.pack('<Q', 1) + struct.pack('<i', 0)   # seq + skip
    oi_body += struct.pack('<h', 2) + struct.pack('<i', 48225) + struct.pack('<Q', 1785400010)
    oi_body += struct.pack('<h', 1)      # MarketType
    oi_body += struct.pack('<i', 987654) # openInterest
    oi_pkt = frame(1510, oi_body, False)
    res4 = parse_packets(oi_pkt)
    assert len(res4) == 1 and res4[0]["oi"] == 987654 and res4[0]["instr"] == 48225
    print("OI OK:", res4[0])

    print("\nALL XTS BINARY PARSER TESTS PASSED")

if __name__ == "__main__":
    main()
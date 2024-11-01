from utils import Span
from collections import InlineArray


@value
struct Reader[origin: MutableOrigin]:
    var buf: Span[Byte, origin]

    fn __init__(inout self, ref [origin]list: List[Byte]):
        self.buf = Span[Byte, origin](list)

    fn read[o: MutableOrigin](inout self, inout buf: Span[Byte, o]) -> Int:
        amt = min(len(buf), len(self.buf))
        a = self.buf[:amt]
        b = self.buf[amt:]
        if amt == 1:
            buf[0] = a[0]
        else:
            buf[:amt].copy_from(a)
        self.buf = b
        return amt

    fn read_exact[
        o: MutableOrigin
    ](inout self, inout buf: Span[Byte, o]) raises:
        if len(buf) > len(self.buf):
            self.buf = self.buf[len(self.buf) :]
            raise Error("read exact eof")
        amt = min(len(buf), len(self.buf))
        a = self.buf[:amt]
        b = self.buf[amt:]
        if amt == 1:
            buf[0] = a[0]
        else:
            buf[:amt].copy_from(a)
        self.buf = b

    fn take(inout self, limit: UInt64) -> Take[origin, __origin_of(self)]:
        return Take(Pointer.address_of(self), limit)


@value
struct Writer[origin: MutableOrigin]:
    var buf: Span[Byte, origin]

    fn write(inout self, data: Span[Byte]) -> Int:
        amt = min(len(data), len(self.buf))
        a = self.buf[:amt]
        b = self.buf[amt:]
        a.copy_from(data[:amt])
        self.buf = b
        return amt


@value
struct Take[origin: MutableOrigin, r_origin: MutableOrigin]:
    var inner: Pointer[Reader[origin], r_origin]
    var limit: UInt64

    fn read[o: MutableOrigin](inout self, inout buf: Span[UInt8, o]) -> Int:
        if self.limit == 0:
            return 0
        max = int(min(len(buf), self.limit))
        b = buf[:max]
        n = self.inner[].read(b)
        self.limit -= n
        return n

    fn read_to_end(inout self, inout buf: List[Byte]) raises:
        s = InlineArray[Byte, 32](fill=0)
        sp = Span(s)
        while True:
            n = self.read(sp)
            if n == 0:
                break

            buf.reserve(len(buf) + n)
            for i in range(n):
                buf.append(s[i])


# Taken from dag-cbrrr.
# See https://github.com/DavidBuchanan314/dag-cbrrr.
alias B32_CHARSET = "abcdefghijklmnopqrstuvwxyz234567".as_bytes()


# Taken from dag-cbrrr.
# See https://github.com/DavidBuchanan314/dag-cbrrr.
fn bytes_to_b32_multibase(data: Span[Byte]) raises -> String:
    res = List[Byte](capacity=(1 + (len(data) * 8 + 4) // 5))
    alias B = ord("b")  # b prefix indicates multibase base32
    res.append(B)
    data_i = 0
    while data_i + 4 < len(data):
        a = data[data_i]
        data_i += 1
        b = data[data_i]
        data_i += 1
        c = data[data_i]
        data_i += 1
        d = data[data_i]
        data_i += 1
        e = data[data_i]
        data_i += 1
        # 76543 21076 54321 07654 32107 65432 10765 43210
        # aaaaa aaabb bbbbb bcccc ccccd ddddd ddeee eeeee
        # 43210 43210 43210 43210 43210 43210 43210 43210
        res.append(B32_CHARSET[int(((a >> 3)) & 0x1F)])
        res.append(B32_CHARSET[int(((a << 2) | (b >> 6)) & 0x1F)])
        res.append(B32_CHARSET[int(((b >> 1)) & 0x1F)])
        res.append(B32_CHARSET[int(((b << 4) | (c >> 4)) & 0x1F)])
        res.append(B32_CHARSET[int(((c << 1) | (d >> 7)) & 0x1F)])
        res.append(B32_CHARSET[int(((d >> 2)) & 0x1F)])
        res.append(B32_CHARSET[int(((d << 3) | (e >> 5)) & 0x1F)])
        res.append(B32_CHARSET[int(((e << 0)) & 0x1F)])
    rest_len = len(data) - data_i
    if rest_len == 4:
        a = data[data_i]
        data_i += 1
        b = data[data_i]
        data_i += 1
        c = data[data_i]
        data_i += 1
        d = data[data_i]
        res.append(B32_CHARSET[int(((a >> 3)) & 0x1F)])
        res.append(B32_CHARSET[int(((a << 2) | (b >> 6)) & 0x1F)])
        res.append(B32_CHARSET[int(((b >> 1)) & 0x1F)])
        res.append(B32_CHARSET[int(((b << 4) | (c >> 4)) & 0x1F)])
        res.append(B32_CHARSET[int(((c << 1) | (d >> 7)) & 0x1F)])
        res.append(B32_CHARSET[int(((d >> 2)) & 0x1F)])
        res.append(B32_CHARSET[int(((d << 3)) & 0x1F)])
    elif rest_len == 3:
        a = data[data_i]
        data_i += 1
        b = data[data_i]
        data_i += 1
        c = data[data_i]
        res.append(B32_CHARSET[int(((a >> 3)) & 0x1F)])
        res.append(B32_CHARSET[int(((a << 2) | (b >> 6)) & 0x1F)])
        res.append(B32_CHARSET[int(((b >> 1)) & 0x1F)])
        res.append(B32_CHARSET[int(((b << 4) | (c >> 4)) & 0x1F)])
        res.append(B32_CHARSET[int(((c << 1)) & 0x1F)])
    elif rest_len == 2:
        a = data[data_i]
        data_i += 1
        b = data[data_i]
        res.append(B32_CHARSET[int(((a >> 3)) & 0x1F)])
        res.append(B32_CHARSET[int(((a << 2) | (b >> 6)) & 0x1F)])
        res.append(B32_CHARSET[int(((b >> 1)) & 0x1F)])
        res.append(B32_CHARSET[int(((b << 4)) & 0x1F)])
    elif rest_len == 1:
        a = data[data_i]
        res.append(B32_CHARSET[int(((a >> 3)) & 0x1F)])
        res.append(B32_CHARSET[int(((a << 2)) & 0x1F)])
    elif rest_len == 0:
        # nothing to do here
        pass
    else:
        raise Error("unreachable!?")
    return res

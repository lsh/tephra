"""
Adapted from unsigned-varint.

See https://github.com/paritytech/unsigned-varint.
"""


from .utils import Reader
from utils import Span
from collections import InlineArray


@always_inline
fn decode[dtype: DType](inout buf: Span[UInt8]) raises -> Scalar[dtype]:
    var max_bytes: Scalar[dtype]

    @parameter
    if dtype == UInt8.type:
        max_bytes = 1
    elif dtype == UInt16.type:
        max_bytes = 2
    elif dtype == UInt32.type:
        max_bytes = 4
    elif dtype == UInt64.type:
        max_bytes = 9
    else:
        # Not allowed
        constrained[False]()
        max_bytes = 0

    n = Scalar[dtype](0)
    i = n
    for b in buf:
        k = (b[] & 0x7F).cast[dtype]()
        n |= k << (i * 7)
        if is_last(b[]):
            if b[] == 0 and i > 0:
                # If last byte (of a multi-byte varint) is zero, it could have been "more
                # minimally" encoded by dropping that trailing zero.
                raise Error("not minimal")
            buf = buf[int(i + 1) :]
            return n
        if i == max_bytes:
            raise Error("overflow")
        i += 1
    raise Error("insufficient")


@always_inline
fn is_last(b: UInt8) -> Bool:
    """Return True if this is the last byte of an unsigned varint."""
    return b & 0x80 == 0


@always_inline
fn encoding_length[dtype: DType]() -> Int:
    constrained[dtype.is_unsigned()]()

    @parameter
    if dtype == UInt8.type:
        return 2
    elif dtype == UInt16.type:
        return 3
    elif dtype == UInt32.type:
        return 5
    elif dtype == UInt64.type:
        return 5
    else:
        return 0


@always_inline
fn buffer[dtype: DType]() -> InlineArray[UInt8, encoding_length[dtype]()]:
    return InlineArray[UInt8, encoding_length[dtype]()](fill=0)


@always_inline
fn encode[
    origin: MutableOrigin
](number: Scalar, inout buf: Span[UInt8, origin]) -> Span[UInt8, origin]:
    n = number
    i = 0
    for _ in range(len(buf)):
        buf[i] = n.cast[UInt8.type]() | 0x80
        n >>= 7
        if n == 0:
            buf[i] &= 0x7F
            break
        i += 1
    debug_assert(n == 0)
    return buf[0 : i + 1]


fn read[dtype: DType](inout reader: Reader) raises -> Scalar[dtype]:
    b = buffer[dtype]()
    bspan = Span(b)
    for i in range(len(b)):
        bs = bspan[i : i + 1]
        n = reader.read(bs)
        if n == 0:
            raise Error("unexpected EOF")
        if is_last(b[i]):
            bs = bspan[: i + 1]
            return decode[dtype](bs)
    raise Error("overflow")

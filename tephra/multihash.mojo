"""
Heavily adapted from rust-multihash.

See https://github.com/multiformats/rust-multihash
"""

from collections import InlineArray
from utils import Span
from memory import memcpy

import .varint
from .utils import Reader


@value
struct Multihash[BufSize: Int]:
    var _code: UInt64
    """ The code of the Multihash."""
    var _size: UInt8
    """The actual size of the digest in bytes (not the allocated size)."""
    var _digest: InlineArray[UInt8, BufSize]
    """The digest."""

    fn __init__(inout self):
        self._code = 0
        self._size = 0
        self._digest = InlineArray[UInt8, BufSize](fill=0)

    fn __eq__(self, rhs: Self) -> Bool:
        if self._code != rhs._code or self._size != rhs._size:
            return False
        for i in range(int(self._size)):
            if self._digest[i] != rhs._digest[i]:
                return False
        return True

    fn __ne__(self, rhs: Self) -> Bool:
        return not (self == rhs)

    @staticmethod
    fn wrap(code: UInt64, input_digest: Span[UInt8]) raises -> Self:
        """Wraps the digest in a multihash."""
        if len(input_digest) > BufSize:
            raise Error("invalid size")

        size = len(input_digest)
        digest = InlineArray[UInt8, BufSize](fill=0)
        i = 0

        while i < size:
            digest[i] = input_digest[i]
            i += 1

        return Self(code, size, digest)

    @always_inline
    fn code(self) -> UInt64:
        """Returns the code of the multihash."""
        return self._code

    @always_inline
    fn size(self) -> UInt8:
        """Returns the size of the digest."""
        return self._size

    @always_inline
    fn digest(self) -> Span[UInt8, __origin_of(self._digest)]:
        """Returns the digest."""
        return Span(self._digest)[: int(self._size)]

    @staticmethod
    @always_inline
    fn read(inout r: Reader) raises -> Self:
        code, size, digest = read_multihash[BufSize](r)
        return Self(code, size, digest)

    @always_inline
    fn write_to[W: Writer](self, inout w: W):
        write_multihash(w, self.code(), self.size(), self.digest())

    @staticmethod
    @always_inline
    fn from_bytes[o: MutableOrigin](bytes: Span[UInt8, o]) raises -> Self:
        """
        Parses a multihash from a bytes.

        You need to make sure the passed in bytes have the correct length. The digest length
        needs to match the `size` value of the multihash.
        """
        buf = bytes
        r = Reader(buf)
        res = Self.read(r)
        # There were more bytes supplied than read
        if buf:
            raise Error(
                "Currently the maximum size is 255, therefore always fits into"
                " usize"
            )
        return res

    @always_inline
    fn encoded_len(self) -> Int:
        """Returns the length in bytes needed to encode this multihash into bytes.
        """
        code_buf = varint.buffer[UInt64.type]()
        code_span = Span(code_buf)
        code = varint.encode(self._code, code_span)
        size_buf = varint.buffer[UInt8.type]()
        size_span = Span(size_buf)
        size = varint.encode(self._size, size_span)
        return int(len(code) + len(size) + self._size)

    @always_inline
    fn truncate(self, size: UInt8) -> Self:
        """
        Truncates the multihash to the given size. It's up to the caller to ensure that the new size
        is secure (cryptographically) to use.

        If the new size is larger than the current size, this method does nothing.
        """
        mh = self
        mh._size = min(mh._size, size)
        return mh

    @always_inline
    fn resize[NewBufSize: Int](self) raises -> Multihash[NewBufSize]:
        """
        Resizes the backing multihash buffer.

        This function fails if the hash digest is larger than the target size.
        """
        size = self._size
        if size > NewBufSize:
            raise Error("invalid size: " + str(self._size))
        mh = Multihash(
            _code=self._code,
            _size=self._size,
            _digest=InlineArray[UInt8, NewBufSize](fill=0),
        )
        memcpy(mh.digest().unsafe_ptr(), self.digest().unsafe_ptr(), int(size))
        return mh

    @always_inline
    fn into_inner(self) -> (UInt64, InlineArray[UInt8, BufSize], UInt8):
        """
        Decomposes struct, useful when needing a `Sized` array or moving all the data into another type

        It is recommended to use `digest()` `code()` and `size()` for most cases.
        """
        return self._code, self._digest, self._size


fn write_multihash[
    W: Writer
](inout w: W, code: UInt64, size: UInt8, digest: Span[UInt8]):
    """Writes the multihash to a byte stream."""
    code_buf = varint.buffer[UInt64.type]()
    cs = Span(code_buf)
    code_ = varint.encode(code, cs)

    size_buf = varint.buffer[UInt8.type]()
    ss = Span(size_buf)
    size_ = varint.encode(size, ss)

    w.write_bytes(code_)
    w.write_bytes(size_)
    w.write_bytes(digest)


fn read_multihash[
    BufSize: Int
](inout r: Reader) raises -> (UInt64, UInt8, InlineArray[UInt8, BufSize],):
    """
    Reads a multihash from a byte stream that contains a full multihash (code, size and the digest).

    Returns the code, size and the digest. The size is the actual size and not the
    maximum/allocated size of the digest.

    Currently the maximum size for a digest is 255 bytes.
    """
    code = read_u64(r)
    size = read_u64(r)
    if size > BufSize or size > UInt8.MAX.cast[UInt64.type]():
        raise Error("invalid size: " + str(size))
    digest = InlineArray[UInt8, BufSize](fill=0)
    ds = Span(digest)[: int(size)]
    r.read_exact(ds)

    return code, size.cast[UInt8.type](), digest


fn read_u64(inout r: Reader) raises -> UInt64:
    """
    Reads 64 bits from a byte array into a u64
    Adapted from unsigned-varint's generated read_u64 function at
    https://github.com/paritytech/unsigned-varint/blob/master/src/io.rs
    """
    b = varint.buffer[UInt64.type]()
    bspan = Span(b)
    for i in range(len(b)):
        bs = bspan[i : i + 1]
        n = r.read(bs)
        if n == 0:
            raise Error("insufficient varint bytes")
        elif varint.is_last(b[i]):
            bs = bspan[: i + 1]
            return varint.decode[UInt64.type](bs)
    raise Error("overflow")

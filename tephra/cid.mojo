"""
This module is heavily adapted from rust-cid.

See https://github.com/multiformats/rust-cid.
"""

from utils import StringSlice, Span
from collections import InlineArray
from .multihash import Multihash
import .varint
from .utils import Reader, bytes_to_b32_multibase


alias DAG_PB = 0x70
"""DAG-PB multicodec code"""
alias SHA2_256 = 0x12
"""The SHA_256 multicodec code"""

alias Cid = CidGeneric[64]


struct CidGeneric[size: Int]:
    """
    Representation of a CID.

    The generic is about the allocated size of the multihash.
    """

    var _version: Version
    """The version of CID."""
    var _codec: UInt64
    """The codec of CID."""
    var _hash: Multihash[size]
    """The multihash of CID."""

    fn __init__(
        inout self, version: Version, codec: UInt64, hash: Multihash[size]
    ) raises:
        """Create a new CID."""
        if version == Version.v0:
            if codec != DAG_PB:
                raise Error("invalid cid v0 codec")
            self = Self.v0(hash)
        else:
            self = Self.v1(codec, hash)

    fn __init__(inout self):
        self._version = Version.v1
        self._codec = 0
        self._hash = Multihash[size]()

    fn __moveinit__(inout self, owned rhs: Self):
        self._version = rhs._version
        self._codec = rhs._codec
        self._hash = rhs._hash

    fn __copyinit__(inout self, rhs: Self):
        self._version = rhs._version
        self._codec = rhs._codec
        self._hash = rhs._hash

    fn __hash__(self) -> UInt:
        s = String()
        self.write_to(s)
        return s.__hash__()

    fn __eq__(self, rhs: Self) -> Bool:
        return (
            self._version == rhs._version
            and self._codec == rhs._codec
            and self._hash == rhs._hash
        )

    fn __ne__(self, rhs: Self) -> Bool:
        return not (self == rhs)

    @staticmethod
    fn v0(hash: Multihash[size]) raises -> Self:
        """Create a new CIDv0."""
        if hash.code() != SHA2_256 or hash.size() != 32:
            raise Error("invalid cid v0 multihash")
        return Self(version=Version.v0, codec=DAG_PB, hash=hash)

    @staticmethod
    fn v1(codec: UInt64, hash: Multihash[size]) -> Self:
        """Create a new CIDv1."""
        res = Self()
        res._codec = codec
        res._hash = hash
        return res

    fn into_v1(self) raises -> Self:
        """Convert a CIDv0 to a CIDv1. Returns unchanged if already a CIDv1."""
        if self._version == Version.v0:
            if self._codec != DAG_PB:
                raise Error("invalid cid v0 codec")
            return Self.v1(self._codec, self._hash)
        else:
            return self

    fn version(self) -> Version:
        """Returns the cid version."""
        return self._version

    fn codec(self) -> UInt64:
        """Returns the cid codec."""
        return self._codec

    fn hash(ref [_]self) -> ref [__origin_of(self._hash)] Multihash[size]:
        """Returns the cid multihash."""
        return self._hash

    @staticmethod
    fn read_bytes(inout r: Reader) raises -> Self:
        """Reads the bytes from a byte stream."""
        version = varint_read_u64(r)
        codec = varint_read_u64(r)

        # CIDv0 has the fixed `0x12 0x20` prefix
        if version == 0x12 and codec == 0x20:
            digest = InlineArray[UInt8, 32](fill=0)
            ds = Span(digest)
            r.read_exact(ds)
            mh = Multihash[size].wrap(version, digest)
            return Self.v0(mh)

        version_ = Version(version)
        if version_ == Version.v0:
            raise Error("invalid explicit cid v0")
        else:
            mh = Multihash[size].read(r)
            return Self(version=version_, codec=codec, hash=mh)

    fn write_bytes_v1[W: Writer](self, inout w: W):
        version_buf = varint.buffer[UInt64.type]()
        vs = Span(version_buf)
        version = varint.encode(self._version._value, vs)

        codec_buf = varint.buffer[UInt64.type]()
        cs = Span(codec_buf)
        codec = varint.encode(self._codec, cs)

        w.write_bytes(version)
        w.write_bytes(codec)
        w.write(self._hash)

    fn write_bytes[W: Writer](self, inout w: W):
        """Writes the bytes to a byte stream."""
        self.write_bytes_v1(w)

    fn to_bytes(self) -> List[Byte]:
        """Returns the encoded bytes of the `Cid`."""
        bytes = String()
        self.write_bytes(bytes)
        return bytes.as_bytes()

    fn to_string_v1(self) raises -> String:
        return bytes_to_b32_multibase(self.to_bytes())

    fn write_to[W: Writer](self, inout w: W):
        try:
            w.write(self.to_string_v1())
        except:
            pass


fn varint_read_u64(inout r: Reader) raises -> UInt64:
    """
    Reads 64 bits from a byte array into a u64.

    Adapted from unsigned-varint's generated read_u64 function at
    https://github.com/paritytech/unsigned-varint/blob/master/src/io.rs.
    """
    b = varint.buffer[UInt64.type]()
    bspan = Span(b)
    for i in range(len(b)):
        bs = bspan[i : i + 1]
        n = r.read(bs)
        if n == 0:
            raise Error("decode error")
        elif varint.is_last(b[i]):
            bs = bspan[: i + 1]
            return varint.decode[UInt64.type](bs)
    raise Error("decode error")


@value
struct Version:
    """The version of the CID."""

    var _value: UInt8
    alias v0 = Self(UInt8(0))
    """CID version 0."""
    alias v1 = Self(UInt8(1))
    """CID version 1."""

    fn __init__(inout self, raw: UInt64) raises:
        """Convert a number to the matching version, or `Error` if no valid version is matching.
        """
        if raw == 0 or raw == 1:
            self._value = raw.cast[UInt8.type]()
        else:
            raise Error("invalid CID version")

    @always_inline
    fn __eq__(self, rhs: Self) -> Bool:
        return self._value == rhs._value

    @always_inline
    fn to_uint64(self) -> UInt64:
        return self._value.cast[UInt64.type]()

    @staticmethod
    @always_inline
    fn is_v0_str(data: StringSlice) -> Bool:
        """Check if the version of `data` string is CIDv0."""
        # v0 is a Base58Btc encoded sha hash, so it has
        # fixed length and always begins with "Qm"
        return len(data) == 46 and data[0] == "Q" and data[1] == "m"

    @staticmethod
    @always_inline
    fn is_v0_binary(data: Span[UInt8]) -> Bool:
        """Check if the version of `data` bytes is CIDv0."""
        return len(data) == 34 and data[0] == 0x12 and data[1] == 0x20

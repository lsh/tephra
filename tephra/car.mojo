"""
This is heavily adapted from iroh-car.

See https://github.com/n0-computer/iroh-car.
"""


from utils import Span, Variant
from collections import Optional

from .cid import Cid
import .varint
from .cbor import Value
from .utils import Reader


struct CarHeader:
    """A car header."""

    var value: Variant[CarHeaderV1]

    fn __init__(inout self, v1: CarHeaderV1):
        self.value = v1

    fn __copyinit__(inout self, rhs: Self):
        self.value = rhs.value

    fn __moveinit__(inout self, owned rhs: Self):
        self.value = rhs.value

    @staticmethod
    fn v1(roots: List[Cid]) -> Self:
        return Self(CarHeaderV1(roots))

    @staticmethod
    fn decode[
        origin: MutableOrigin
    ](buffer: Span[UInt8, origin]) raises -> Self:
        r = Reader(buffer)
        val = Value.decode(r)
        roots_val = val.dict()["roots"].list()
        version = val.dict()["version"].uint()
        roots = List[Cid](capacity=len(roots_val))
        for val in roots_val:
            roots.append(val[].cid())

        if not roots:
            raise Error("empty CAR file")
        if version != 1:
            raise Error("Only CAR file version 1 is supported")
        return CarHeader.v1(roots)

    fn roots(self) -> Span[Cid, __origin_of(self.value[CarHeaderV1].roots)]:
        return self.value[CarHeaderV1].roots

    fn version(self) -> UInt64:
        return 1


@value
struct CarHeaderV1:
    """CAR file header version 1."""

    var roots: List[Cid]
    var version: UInt64

    fn __init__(inout self, roots: List[Cid], version: UInt64 = 1):
        """Creates a new CAR file header."""
        self.roots = roots
        self.version = version


alias MAX_ALLOC = 4 * 1024 * 1024
"""Maximum size that is used for single node."""


@always_inline
fn ld_read(
    inout r: Reader, inout buf: List[Byte]
) raises -> Span[Byte, __origin_of(buf)]:
    length = int(varint.read[UInt64.type](r))
    if length > MAX_ALLOC:
        raise Error("ld read too large: " + str(length))
    if length > len(buf):
        buf.resize(length, 0)

    bs = Span(buf)[:length]
    r.read_exact(bs)
    return bs


struct CarReader[origin: MutableOrigin, r_origin: MutableOrigin]:
    """Reads CAR files that are in a Reader."""

    var _reader: Pointer[Reader[origin], r_origin]
    var _header: CarHeader
    var _buffer: List[Byte]

    fn __init__(inout self, ref [r_origin]reader: Reader[origin]) raises:
        """Creates a new CarReader and parses the CarHeader."""
        buffer = List[Byte]()
        buf = ld_read(reader, buffer)
        self._header = CarHeader.decode(buf)
        self._reader = Pointer.address_of(reader)
        self._buffer = buffer^

    fn header(self) -> ref [__origin_of(self._header)] CarHeader:
        return self._header

    fn next_block(inout self) raises -> (Cid, List[Byte]):
        buf = ld_read(self._reader[], self._buffer)
        br = Reader(buf)
        c = Cid.read_bytes(br)
        return c, List[Byte](br.buf)

"""
This is heavily adapted from libipld.

See https://github.com/ipld/libipld.
"""

from memory import UnsafePointer, bitcast, memcpy
from collections import List, Dict, Optional, InlineArray
from collections.optional import _NoneType
from utils import StaticString, Span, StringSlice, Variant
from utils.numerics import isnan, isinf
from bit import byte_swap
import sys
import testing

from .cid import Cid
from .utils import Reader


@value
struct ListValue:
    var value: List[Value]


@value
struct DictValue:
    var value: Dict[String, Value]


struct Value(Writable):
    alias _storage = Variant[
        UInt64,
        Int64,
        Float64,
        String,
        List[UInt8],
        ListValue,
        DictValue,
        Bool,
        Cid,
        _NoneType,
    ]
    var _value: Self._storage

    fn __init__(inout self):
        self._value = _NoneType()

    fn __init__(inout self, none: NoneType):
        self._value = _NoneType()

    fn __init__(inout self, int: UInt64):
        self._value = int

    fn __init__(inout self, int: Int64):
        self._value = int

    fn __init__(inout self, float: Float64):
        self._value = float

    fn __init__(inout self, str: String):
        self._value = str

    fn __init__(inout self, bytes: List[UInt8]):
        self._value = bytes

    fn __init__(inout self, list: List[Value]):
        self._value = ListValue(list)

    fn __init__(inout self, dict: Dict[String, Value]):
        self._value = DictValue(dict)

    fn __init__(inout self, bool: Bool):
        self._value = bool

    fn __init__(inout self, cid: Cid):
        self._value = cid

    fn __copyinit__(inout self, rhs: Self):
        self._value = rhs._value

    fn __moveinit__(inout self, owned rhs: Self):
        self._value = rhs._value

    fn is_some(self) -> Bool:
        return not self.is_none()

    fn is_none(self) -> Bool:
        return self._value.isa[_NoneType]()

    fn dict(
        ref [_]self,
    ) -> ref [__origin_of(self._value[DictValue].value)] Dict[String, Value]:
        return self._value[DictValue].value

    fn list(
        ref [_]self,
    ) -> ref [__origin_of(self._value[ListValue].value)] List[Value]:
        return self._value[ListValue].value

    fn bytes(
        ref [_]self,
    ) -> ref [__origin_of(self._value)] List[UInt8]:
        return self._value[List[UInt8]]

    fn string(ref [_]self) -> ref [__origin_of(self._value)] String:
        return self._value[String]

    fn uint(ref [_]self) -> UInt64:
        return self._value[UInt64]

    fn int(ref [_]self) -> Int64:
        return self._value[Int64]

    fn cid(ref [_]self) -> ref [__origin_of(self._value)] Cid:
        return self._value[Cid]

    fn opt_cid(ref [_]self) -> Optional[Cid]:
        if self.is_some():
            return self._value[Cid]
        return None

    fn write_to[W: Writer](self, inout w: W):
        if self._value.isa[UInt64]():
            return w.write(self._value[UInt64])
        elif self._value.isa[Int64]():
            return w.write(self._value[Int64])
        elif self._value.isa[Float64]():
            return w.write(self._value[Float64])
        elif self._value.isa[String]():
            w.write('"')
            w.write(self._value[String])
            w.write('"')
        elif self._value.isa[List[UInt8]]():
            w.write("[")
            l = len(self._value[List[UInt8]])
            for i in range(l):
                w.write(self._value[List[UInt8]][i])
                if i < l - 1:
                    w.write(", ")
            w.write("]")
        elif self._value.isa[ListValue]():
            w.write("[")
            l = len(self._value[ListValue].value)
            for i in range(l):
                w.write(self._value[ListValue].value[i])
                if i < l - 1:
                    w.write(", ")
            w.write("]")
        elif self._value.isa[DictValue]():
            w.write("{")
            i = 0
            l = len(self._value[DictValue].value)
            for key in self._value[DictValue].value.keys():
                w.write('"')
                w.write(key[])
                w.write('": ')
                try:
                    w.write(self._value[DictValue].value[key[]])
                except:
                    pass
                if i < l - 1:
                    w.write(", ")
                i += 1
            w.write("}")
        elif self._value.isa[Bool]():
            return w.write(self._value[Bool])
        elif self._value.isa[Cid]():
            w.write("Cid(")
            w.write(self._value[Cid])
            w.write(")")
        elif self._value.isa[_NoneType]():
            w.write("None")

    @staticmethod
    fn decode(inout r: Reader) raises -> Self:
        major = read_major(r)
        kind = major.kind()
        if kind == MajorKind.unsigned_int:
            return read_uint(r, major)
        elif kind == MajorKind.negative_int:
            return (-1 - read_uint(r, major)).cast[Int64.type]()
        elif kind == MajorKind.byte_string:
            len = read_uint(r, major)
            return read_bytes(r, len)
        elif kind == MajorKind.text_string:
            len = read_uint(r, major)
            return read_str(r, len)
        elif kind == MajorKind.array:
            len = read_uint(r, major)
            return read_list(r, len)
        elif kind == MajorKind.map:
            len = read_uint(r, major)
            return read_map(r, len)
        elif kind == MajorKind.tag:
            value = read_uint(r, major)
            if value == 42:
                return read_link(r)
            else:
                raise Error("unknown tag")
        elif kind == MajorKind.other:
            if major == FALSE:
                return False
            elif major == TRUE:
                return True
            elif major == NULL:
                return None
            elif major == F32:
                return read_num[Float32.type](r).cast[Float64.type]()
            elif major == F64:
                return read_num[Float64.type](r)
            else:
                raise Error("unexpected code: " + str(major.value))
        else:
            raise Error("unexpected major kind: " + str(kind.value))


struct Major:
    """Represents a major "byte". This includes both the major bits and the additional info.
    """

    var value: UInt8

    fn __init__(inout self, value: UInt8) raises:
        # This is the core of the validation logic. Every major type passes through here giving us a chance
        # to determine if it's something we allow.

        # We don't allow any major types with additional info 28-31 inclusive.
        # Or the bitmask 0b00011100 = 28.
        if value & 28 == 28:
            raise Error("unexpected code in major constructor: " + str(value))
        elif (value >> 5) == MajorKind.other.value:
            v = value & 0x1
            if UInt8(20) <= v <= 22:
                # False, True, Null. TODO: Allow undefined?
                pass
            elif UInt8(25) <= v <= 27:
                # Floats. TODO: forbid f16 & f32?
                pass
            else:
                # Everything is forbidden.
                # raise Error(
                #     "unexpected code in major constructor: " + str(value)
                # )
                pass
        self.value = value

    fn __init__(inout self, kind: MajorKind, info: UInt8):
        self.value = (kind.value << 5) | info

    fn __copyinit__(inout self, rhs: Self):
        self.value = rhs.value

    fn __moveinit__(inout self, owned rhs: Self):
        self.value = rhs.value

    @always_inline
    fn kind(self) -> MajorKind:
        """Returns the major type."""
        # This is a 3 bit value, so value 0-7 are covered.
        return self.value >> 5

    @always_inline
    fn info(self) -> UInt8:
        """Returns the additional info."""
        return self.value & 0x1F

    @always_inline
    fn len(self) -> UInt8:
        """Interprets the additioanl info as a number of additional bytes that should be consumed.
        """
        # All major types follow the same rules for "additioanl bytes".
        # 24 -> 1, 25 -> 2, 26 -> 4, 27 -> 8
        info = self.info()
        if UInt8(24) <= info <= 27:
            return 1 << (info - 24)
        else:
            return 0

    @always_inline
    fn __eq__(self, rhs: Self) -> Bool:
        return self.value == rhs.value

    @always_inline
    fn __ne__(self, rhs: Self) -> Bool:
        return self.value != rhs.value


alias FALSE = Major(MajorKind.other, 20)
"""The constant FALSE."""
alias TRUE = Major(MajorKind.other, 21)
"""The constant TRUE."""
alias NULL = Major(MajorKind.other, 22)
"""The constant NULL."""
alias F16 = Major(MajorKind.other, 25)
"""The major "byte" indicating that a 16 bit float follows."""
alias F32 = Major(MajorKind.other, 26)
"""The major "byte" indicating that a 32 bit float follows."""
alias F64 = Major(MajorKind.other, 27)
"""The major "byte" indicating that a 64 bit float follows."""


@value
struct MajorKind:
    var value: UInt8

    alias unsigned_int = Self(0)
    """Non-negative integer (major type 0)."""
    alias negative_int = Self(1)
    """Negative integer (major type 1)."""
    alias byte_string = Self(2)
    """Byte string (major type 2)."""
    alias text_string = Self(3)
    """Unicode text string (major type 3)."""
    alias array = Self(4)
    """Array (major type 4)."""
    alias map = Self(5)
    """Map (major type 5)."""
    alias tag = Self(6)
    """Tag (major type 6)."""
    alias other = Self(7)
    """Other (major type 7)."""

    fn write_to[W: Writer](self, inout w: W):
        if self == Self.unsigned_int:
            w.write("MajorType.unsigned_int")
        elif self == Self.negative_int:
            w.write("MajorType.negative_int")
        elif self == Self.byte_string:
            w.write("MajorType.byte_string")
        elif self == Self.text_string:
            w.write("MajorType.text_string")
        elif self == Self.array:
            w.write("MajorType.array")
        elif self == Self.map:
            w.write("MajorType.map")
        elif self == Self.tag:
            w.write("MajorType.tag")
        elif self == Self.other:
            w.write("MajorType.other")

    @always_inline
    fn __eq__(self, rhs: Self) -> Bool:
        return self.value == rhs.value

    @always_inline
    fn __ne__(self, rhs: Self) -> Bool:
        return self.value != rhs.value


@always_inline
fn read_num[dtype: DType](inout r: Reader) raises -> Scalar[dtype]:
    """Reads a number from a byte stream."""
    alias width = sys.info.sizeof[Scalar[dtype]]()
    buf = InlineArray[UInt8, sys.info.sizeof[Scalar[dtype]]()](fill=0)
    bs = Span(buf)
    r.read_exact(bs)
    b = bitcast[dtype, 1](buf.unsafe_ptr().load[width=width]())
    return b


@always_inline
fn read_bytes(inout r: Reader, length: UInt64) raises -> List[UInt8]:
    """Reads `length` number of bytes from a byte stream."""
    buf = List[UInt8](capacity=min(int(length), 16 * 1024))
    t = r.take(length)
    t.read_to_end(buf)
    if len(buf) != int(length):
        raise Error("unexpected EOF")
    return buf


fn read_str(inout r: Reader, length: UInt64) raises -> String:
    """Reads `len` number of bytes from a byte stream and converts them to a string.
    """
    buf = read_bytes(r, length)
    buf.append(0)  # null termination
    return String(buf)


fn read_list(inout r: Reader, length: UInt64) raises -> List[Value]:
    """Reads a list of any type that implements `TryReadCbor` from a stream of cbor encoded bytes.
    """

    # Limit up-front allocations to 16KiB as the length is user controlled.
    alias max_alloc = (16 * 1024) // sys.info.sizeof[Value]()

    list = List[Value](capacity=min(int(length), max_alloc))
    for _ in range(length):
        list.append(Value.decode(r))
    return list


fn read_map(inout r: Reader, length: UInt64) raises -> Dict[String, Value]:
    map = Dict[String, Value]()
    for _ in range(length):
        major = read_major(r)
        if major.kind() != MajorKind.text_string:
            raise Error("unexpected code: " + str(major.value))
        strlen = read_uint(r, major)
        key = read_str(r, strlen)
        value = Value.decode(r)
        if key in map:
            raise Error("duplicate key")
        map[key] = value
    return map


fn read_link(inout r: Reader) raises -> Cid:
    """Reads a cid from a stream of cbor encoded bytes."""
    major = read_major(r)

    if major.kind() != MajorKind.byte_string:
        raise Error("unexpected code: " + str(major.value))

    len = read_uint(r, major)
    if len < 1:
        raise Error("length out of range")

    # skip the first byte per
    # https://github.com/ipld/specs/blob/master/block-layer/codecs/dag-cbor.md#links
    prefix = read_num[UInt8.type](r)
    if prefix != 0:
        raise Error("invalid CID prefix: " + str(prefix))

    # Read the CID. No need to limit the size, the CID will do this for us.
    cid = Cid.read_bytes(r)
    return cid


fn read_major(inout r: Reader) raises -> Major:
    """Read a and validate major "byte". This includes both the major type and the additional info.
    """
    m = Major(read_num[UInt8.type](r))
    return m


fn read_uint(inout r: Reader, major: Major) raises -> UInt64:
    """
    Read the uint argument to the given major type. This function errors if:
    1. The major type doesn't expect an integer argument.
    2. The integer argument is not "minimally" encoded per the IPLD spec."""
    alias MAX_SHORT: UInt64 = 23
    alias MAX_1BYTE: UInt64 = UInt8.MAX.cast[UInt64.type]()
    alias MAX_2BYTE: UInt64 = UInt16.MAX.cast[UInt64.type]()
    alias MAX_4BYTE: UInt64 = UInt32.MAX.cast[UInt64.type]()

    if major.kind() == MajorKind.other:
        raise Error("unexpected code: " + str(major.value))

    info = major.info()
    if UInt8(0) <= info <= 23:
        return info.cast[UInt64.type]()
    elif info == 24:
        value = read_num[UInt8.type](r).cast[UInt64.type]()
        if UInt64(0) <= value <= MAX_SHORT:
            raise Error("Number not minimal")
        return value
    elif info == 25:
        value = byte_swap(read_num[UInt16.type](r)).cast[UInt64.type]()
        if UInt64(0) <= value <= MAX_1BYTE:
            raise Error("Number not minimal")
        return value
    elif info == 26:
        value = byte_swap(read_num[UInt32.type](r)).cast[UInt64.type]()
        if UInt64(0) <= value <= MAX_2BYTE:
            raise Error("Number not minimal")
        return value
    elif info == 27:
        value = byte_swap(read_num[UInt64.type](r)).cast[UInt64.type]()
        if UInt64(0) <= value <= MAX_4BYTE:
            raise Error("Number not minimal")
        return value
    else:
        raise Error("unexpected code: " + str(major.value))


@always_inline
fn write_null[W: Writer](inout w: W) raises:
    """Writes a null byte to a cbor encoded byte stream."""
    w.write(0xF6)

from ipaddress import IPv4Interface, IPv6Interface, IPv4Network, IPv6Network
import json
from struct import calcsize
from uuid import UUID

from . import constants
from .common import (get_array_bin_reader, BaseParameterHandler,
                     get_single_reader)
from .lib import Error
from .text import read_text


def _read_tid_text(crs):
    tid = crs.advance_text()
    if tid[0] != '(' or tid[-1] != ')':
        raise Error("Invalid value")
    tid1, tid2 = tid[1:-1].split(',')
    return int(tid1), int(tid2)


def _read_tid_bin(crs):
    return crs.advance_struct_format("IH")


def _read_uuid_text(crs):
    return UUID(crs.advance_text())


def _read_uuid_bin(crs):
    return UUID(bytes=crs.advance_bytes())


def _read_point_bin(crs):
    return crs.advance_struct_format("2d")


def _read_line_bin(crs):
    return crs.advance_struct_format("3d")


def _read_lseg_bin(crs):
    x1, y1, x2, y2 = crs.advance_struct_format("4d")
    return ((x1, y1), (x2, y2))


def _read_polygon_bin(crs):
    npoints = crs.advance_single("i")
    fmt = "{0}d".format(npoints * 2)
    coords = crs.advance_struct_format(fmt)
    args = [iter(coords)] * 2
    return list(zip(*args))


def _read_path_bin(crs):
    closed = crs.advance_single("?")
    path = _read_polygon_bin(crs)
    return {"closed": closed, "path": path}


def _read_circle_bin(crs):
    x, y, r = crs.advance_struct_format("3d")
    return ((x, y), r)


PGSQL_AF_INET = 2
PGSQL_AF_INET6 = PGSQL_AF_INET + 1


def _read_inet_bin(crs):
    family, mask, is_cidr, size = crs.advance_struct_format("4B")

    if family == PGSQL_AF_INET:
        correct_size = 4
        if is_cidr:
            cls = IPv4Network
        else:
            cls = IPv4Interface
        addr_data = crs.advance_single("I")
    elif family == PGSQL_AF_INET6:
        correct_size = 16
        if is_cidr:
            cls = IPv6Network
        else:
            cls = IPv6Interface
        addr_data = crs.advance_bytes(16)
    else:
        raise Error("Invalid address family")
    if size != correct_size:
        raise Error("Invalid address size")
    return cls((addr_data, mask))


def _read_mac_bin(crs):
    mac1, mac2 = crs.advance_struct_format("HI")
    return (mac1 << 32) + mac2


def _read_json_bin(crs):
    return json.loads(crs.advance_text())


def _read_jsonb_bin(crs):
    if crs.advance_single("B") != 1:
        raise ValueError("Unknown jsonb version")
    return _read_json_bin(crs)


def _read_bit_text(crs):
    """ Reads a bitstring as a Python integer

    Format:
        * a string composed of '1' and '0' characters

    """
    val = 0
    one = ord(b'1')
    zero = ord(b'0')
    for char in crs:
        val *= 2
        if char == one:
            val += 1
        elif char != zero:
            raise Error('Invalid character in bit string')
    return val


def _read_bit_bin(crs):
    """ Reads a bitstring as a Python integer

    Format:
        * signed int: number of bits (bit_len)
        * bytes: All the bits left aligned

    """
    # first get the number of bits in the bit string
    bit_len = crs.advance_single("i")

    # calculate number of data bytes
    quot, rest = divmod(bit_len, 8)
    byte_len = quot + bool(rest)

    # add the value byte by byte, python ints have no upper limit, so this
    # works even for bitstrings longer than 64 bits
    val = 0
    for char in crs.advance_bytes(byte_len):
        val = (val << 8) | char

    if rest:
        # correct for the fact that the bitstring is left aligned
        val >>= (8 - rest)

    return val


def get_various_converters():
    return {
        constants.XMLOID: (None, read_text),
        constants.XMLARRAYOID: (None, get_array_bin_reader(constants.XMLOID)),
        constants.UNKNOWNOID: (None, read_text),
        constants.UUIDOID: (_read_uuid_text, _read_uuid_bin),
        constants.UUIDARRAYOID: (
            None, get_array_bin_reader(constants.UUIDOID)),
        constants.TIDOID: (_read_tid_text, _read_tid_bin),
        constants.TIDARRAYOID: (None, get_array_bin_reader(constants.TIDOID)),
        constants.POINTOID: (None, _read_point_bin),
        constants.POINTARRAYOID: (
            None, get_array_bin_reader(constants.POINTOID)),
        constants.BOXOID: (None, _read_lseg_bin),
        constants.BOXARRAYOID: (None, get_array_bin_reader(constants.BOXOID)),
        constants.LSEGOID: (None, _read_lseg_bin),
        constants.LSEGARRAYOID: (
            None, get_array_bin_reader(constants.LSEGOID)),
        constants.POLYGONOID: (None, _read_polygon_bin),
        constants.POLYGONARRAYOID: (
            None, get_array_bin_reader(constants.POLYGONOID)),
        constants.PATHOID: (None, _read_path_bin),
        constants.PATHARRAYOID: (
            None, get_array_bin_reader(constants.PATHOID)),
        constants.CIRCLEOID: (None, _read_circle_bin),
        constants.CIRCLEARRAYOID: (
            None, get_array_bin_reader(constants.CIRCLEOID)),
        constants.CIDROID: (None, _read_inet_bin),
        constants.CIDRARRAYOID: (
            None, get_array_bin_reader(constants.CIDROID)),
        constants.INETOID: (None, _read_inet_bin),
        constants.INETARRAYOID: (
            None, get_array_bin_reader(constants.INETOID)),
        constants.MACADDROID: (None, _read_mac_bin),
        constants.MACADDRARRAYOID: (
            None, get_array_bin_reader(constants.MACADDROID)),
        constants.MACADDR8OID: (None, get_single_reader("Q")),
        constants.MACADDR8ARRAYOID: (
            None, get_array_bin_reader(constants.MACADDR8OID)),
        constants.JSONOID: (_read_json_bin, _read_json_bin),
        constants.JSONARRAYOID: (
            None, get_array_bin_reader(constants.JSONOID)),
        constants.JSONBOID: (_read_json_bin, _read_jsonb_bin),
        constants.JSONBARRAYOID: (
            None, get_array_bin_reader(constants.JSONBOID)),
        constants.LINEOID: (None, _read_line_bin),
        constants.LINEARRAYOID: (
            None, get_array_bin_reader(constants.LINEOID)),
        constants.BITOID: (_read_bit_text, _read_bit_bin),
        constants.BITARRAYOID: (None, get_array_bin_reader(constants.BITOID)),
        constants.VARBITOID: (_read_bit_text, _read_bit_bin),
        constants.VARBITARRAYOID: (
            None, get_array_bin_reader(constants.VARBITOID)),
    }


class UuidParameterHandler(BaseParameterHandler):

    oid = constants.UUIDOID
    array_oid = constants.UUIDARRAYOID
    fmt = "16s"

    def binary_value(self, val):
        return val.bytes


class BaseIPParameterHandler(BaseParameterHandler):

    ip4_fmt = "4B4s"
    ip6_fmt = "4B16s"
    ip4_size = calcsize(ip4_fmt)
    ip6_size = calcsize(ip6_fmt)

    def get_item_size(self, val):
        return self.ip4_size if val.version == 4 else self.ip6_size

    def get_format(self, val):
        return self.ip4_fmt if val.version == 4 else self.ip6_fmt

    def binary_values(self, val):
        packed = self.get_packed(val)
        return (PGSQL_AF_INET if val.version == 4 else PGSQL_AF_INET6,
                self.get_prefixlen(val),
                self.oid == constants.CIDROID,
                len(packed),
                self.get_packed(val))

    def type_allowed(self, typ):
        return typ in self.allowed_types


class InterfaceParameterHandler(BaseIPParameterHandler):
    oid = constants.INETOID
    array_oid = constants.INETARRAYOID
    allowed_types = (IPv4Interface, IPv6Interface)

    def get_packed(self, val):
        return val.packed

    def get_prefixlen(self, val):
        return val.network.prefixlen


class NetworkParameterHandler(BaseIPParameterHandler):
    oid = constants.CIDROID
    array_oid = constants.CIDRARRAYOID
    allowed_types = (IPv4Network, IPv6Network)

    def get_packed(self, val):
        return val.network_address.packed

    def get_prefixlen(self, val):
        return val.prefixlen

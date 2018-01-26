#include "poque_type.h"


static PyObject *
mac_binval(data_crs *crs)
{
    poque_uint16 first;
    PY_UINT32_T second;

    if (crs_read_uint16(crs, &first) < 0)
        return NULL;
    if (crs_read_uint32(crs, &second) < 0)
        return NULL;
    return PyLong_FromLongLong(((PY_UINT64_T)first << 32) + second);
}


static PyObject *
mac8_binval(data_crs *crs)
{
    PY_UINT64_T val;

    if (crs_read_uint64(crs, &val) < 0)
        return NULL;
    return PyLong_FromUnsignedLongLong(val);
}

static PyObject *IPv4Network;
static PyObject *IPv4Interface;
static PyObject *IPv6Network;
static PyObject *IPv6Interface;

/* These constants are a bit weird. PGSQL_AF_INET has the value of whatever
 * AF_INET is on the server. PGSQL_AF_INET6 is that value plus one.
 * AF_INET seems to be consistently 2 on all platforms. If that is true, there's
 * no problem.
 */
#define PGSQL_AF_INET 2
#define PGSQL_AF_INET6 3

static PyObject *
inet_binval(data_crs *crs)
{
    PyObject *cls;
    unsigned char *cr;
    int mask, size, is_cidr, family;

    cr = (unsigned char *)crs_advance(crs, 4);
    if (cr == NULL)
        return NULL;
    family = cr[0];
    mask = cr[1];
    is_cidr = cr[2];
    size = cr[3];
    if (is_cidr > 1) {
        PyErr_SetString(PoqueError, "Invalid value for is_cidr");
        return NULL;
    }

    if (family == PGSQL_AF_INET ) {
        PY_UINT32_T addr_data;

        if (size != 4) {
            PyErr_SetString(PoqueError, "Invalid address size");
            return NULL;
        }

        /* get IP4 address as 4 byte integer */
        if (crs_read_uint32(crs, &addr_data) < 0) {
            return NULL;
        }

        /* get class */
        cls = is_cidr ? IPv4Network : IPv4Interface;

        /* instantiate class */
        return PyObject_CallFunction(cls, "((Ii))", addr_data, mask);
    }
    else if (family == PGSQL_AF_INET6) {
        char *addr_data;

        if (size != 16) {
            PyErr_SetString(PoqueError, "Invalid address size");
            return NULL;
        }

        /* get IP6 address as 16 bytes */
        addr_data = crs_advance(crs, 16);
        if (addr_data == NULL) {
            return NULL;
        }

        /* get class */
        cls = is_cidr ? IPv6Network : IPv6Interface;

        /* instantiate class */
        return PyObject_CallFunction(cls, "((y#i))", addr_data, 16, mask);
    }
    else {
        PyErr_SetString(PoqueError, "Unknown network family");
        return NULL;
    }
}

/* ======== initialization ================================================== */

static PoqueTypeEntry network_value_handlers[] = {
    {MACADDROID, mac_binval, NULL, InvalidOid, NULL},
    {MACADDR8OID, mac8_binval, NULL, InvalidOid, NULL},
    {INETOID, inet_binval, NULL, InvalidOid, NULL},
    {CIDROID, inet_binval, NULL, InvalidOid, NULL},

    {MACADDRARRAYOID, array_binval, NULL, MACADDROID, NULL},
    {MACADDR8ARRAYOID, array_binval, NULL, MACADDR8OID, NULL},
    {INETARRAYOID, array_binval, NULL, INETOID, NULL},
    {CIDRARRAYOID, array_binval, NULL, CIDROID, NULL},

    {InvalidOid}
};

int
init_network(void)
{
    IPv4Network = load_python_object("ipaddress", "IPv4Network");
    IPv4Interface = load_python_object("ipaddress", "IPv4Interface");
    IPv6Network = load_python_object("ipaddress", "IPv6Network");
    IPv6Interface = load_python_object("ipaddress", "IPv6Interface");

    register_value_handler_table(network_value_handlers);
    return 0;
};

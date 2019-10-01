#include "poque_type.h"
#include "text.h"

static PyObject *
mac_binval(PoqueResult *result, char *data, int len,PoqueValueHandler *unused)
{
    poque_uint16 first;
    PY_UINT32_T second;

    if (len != 6) {
        PyErr_SetString(PoqueError, "Invalid mac address value");
        return NULL;
    }

    first = read_uint16(data);
    second = read_uint32(data + 2);
    return PyLong_FromLongLong(((long long)first << 32) | second);
}


static PyObject *
mac_strval(PoqueResult *result, char *data, int len,PoqueValueHandler *unused)
{
    unsigned int a, b, c, d, e, f;
    int count;

    if (len != 17) {
        PyErr_SetString(PoqueError, "Invalid mac address value");
        return NULL;
    }

    count = sscanf(data, "%2x:%2x:%2x:%2x:%2x:%2x", &a, &b, &c, &d, &e, &f);
    if (count != 6) {
        PyErr_SetString(PoqueError, "Invalid mac address value");
        return NULL;
    }
    return PyLong_FromUnsignedLongLong(
        (unsigned long long)a << 40 | (unsigned long long)b << 32 | c << 24 |
		 d << 16 | e << 8 | f
	);
}


static PyObject *
mac8_binval(PoqueResult *result, char *data, int len,PoqueValueHandler *unused)
{
    PY_UINT64_T val;

    if (len != 8) {
        PyErr_SetString(PoqueError, "Invalid mac8 address value");
        return NULL;
    }

    val = read_uint64(data);
    return PyLong_FromUnsignedLongLong(val);
}


static PyObject *
mac8_strval(PoqueResult *result, char *data, int len,PoqueValueHandler *unused)
{
    unsigned int a, b, c, d, e, f, g, h;
    int count;

    if (len != 23) {
        PyErr_SetString(PoqueError, "Invalid mac address value");
        return NULL;
    }

    count = sscanf(
        data, "%2x:%2x:%2x:%2x:%2x:%2x:%2x:%2x", &a, &b, &c, &d, &e, &f, &g, &h
	);
    if (count != 8) {
        PyErr_SetString(PoqueError, "Invalid mac8 address value");
        return NULL;
    }
    return PyLong_FromUnsignedLongLong(
        (unsigned long long)a << 56 | (unsigned long long)b << 48 |
		(unsigned long long)c << 40 | (unsigned long long)d << 32 | e << 24 |
		f << 16 | g << 8 | h
	);
}

static PyTypeObject *IPv4Network;
static PyTypeObject *IPv4Interface;
static PyTypeObject *IPv6Network;
static PyTypeObject *IPv6Interface;

/* These constants are a bit weird. PGSQL_AF_INET has the value of whatever
 * AF_INET is on the server. PGSQL_AF_INET6 is that value plus one.
 * AF_INET seems to be consistently 2 on all platforms. If that is true, there's
 * no problem.
 */
#define PGSQL_AF_INET 2
#define PGSQL_AF_INET6 3


static PyObject *
ip_binval(
    char *data, int len, int cidr, PyTypeObject *v4_cls, PyTypeObject *v6_cls)
{
    unsigned char *cr;
    int mask, size, is_cidr, family;

    if (len < 4) {
        PyErr_SetString(PoqueError, "Invalid ip value");
        return NULL;
    }

    cr = (unsigned char *)data;
    if (cr == NULL)
        return NULL;
    family = cr[0];
    mask = cr[1];
    is_cidr = cr[2];
    size = cr[3];

    if (is_cidr != cidr) {
        PyErr_SetString(PoqueError, "Wrong value for cidr flag");
        return NULL;
    }
    if (family == PGSQL_AF_INET ) {
        PY_UINT32_T addr_data;

        if (size != 4) {
            PyErr_SetString(PoqueError, "Invalid address size");
            return NULL;
        }

        if (len != 8) {
            PyErr_SetString(PoqueError, "Invalid ip value");
            return NULL;
        }

        /* get IP4 address as 4 byte integer */
        addr_data = read_uint32(data + 4);

        /* instantiate class */
        return PyObject_CallFunction(
                (PyObject *)v4_cls, "((Ii))", addr_data, mask);
    }
    else if (family == PGSQL_AF_INET6) {
        char *addr_data;

        if (size != 16) {
            PyErr_SetString(PoqueError, "Invalid address size");
            return NULL;
        }

        if (len != 20) {
            PyErr_SetString(PoqueError, "Invalid ip value");
            return NULL;
        }

        /* get IP6 address as 16 bytes */
        addr_data = data + 4;
        if (addr_data == NULL) {
            return NULL;
        }

        /* instantiate class */
        return PyObject_CallFunction(
                (PyObject *)v6_cls, "((y#i))", addr_data, 16, mask);
    }
    else {
        PyErr_SetString(PoqueError, "Unknown network family");
        return NULL;
    }
}


static PyObject *
inet_binval(PoqueResult *result, char *data, int len,PoqueValueHandler *unused)
{
    return ip_binval(data, len, 0, IPv4Interface, IPv6Interface);
}

static PyObject *
inet_strval(PoqueResult *result, char *data, int len,PoqueValueHandler *unused)
{
	PyTypeObject *inet_cls;

	if (memchr(data, ':', len) == NULL)
	{
		inet_cls = IPv4Interface;
	}
	else
	{
		inet_cls = IPv6Interface;
	}
	return PyObject_CallFunction((PyObject *)inet_cls, "s#", data, len);
}


static PyObject *
cidr_binval(PoqueResult *result, char *data, int len,PoqueValueHandler *unused)
{
    return ip_binval(data, len, 1, IPv4Network, IPv6Network);
}


static PyObject *
cidr_strval(PoqueResult *result, char *data, int len,PoqueValueHandler *unused)
{
	PyTypeObject *inet_cls;

	if (memchr(data, ':', len) == NULL)
	{
		inet_cls = IPv4Network;
	}
	else
	{
		inet_cls = IPv6Network;
	}
	return PyObject_CallFunction((PyObject *)inet_cls, "s#", data, len);
}

/* ==== ip interface and ip network parameter handlers ====================== */

#define ip_examine(p, t) (Py_TYPE(p) == t ? 8 : 20)


static int
ip_encode_at(int family, int prefixlen, int is_cidr, PyObject *packed,
        char *loc) {
    Py_ssize_t size;
    unsigned char *data;

    size = PyBytes_GET_SIZE(packed);

    data = (unsigned char *)loc;
    data[0] = family;
    data[1] = prefixlen;
    data[2] = is_cidr;
    data[3] = size;
    memcpy(loc + 4, PyBytes_AS_STRING(packed), size);
    return 4 + size;
}

#define get_family(p, t) (Py_TYPE(p) == t ? PGSQL_AF_INET : PGSQL_AF_INET6)


/* ==== ip interface parameter handler specifics ============================ */

static int
ip_interface_examine(param_handler *handler, PyObject *param) {
    return ip_examine(param, IPv4Interface);
}


static int
ip_interface_encode_at(param_handler *handler, PyObject *param,
        char *loc) {

    int family, ret;
    long prefixlen;
    PyObject *py_obj;

    family = get_family(param, IPv4Interface);

    /* prefixlen = param.network.prefixlen */
    py_obj = PyObject_GetAttrString(param, "network");
    if (py_obj == NULL) {
        return -1;
    }
    if (pyobj_long_attr(py_obj, "prefixlen", &prefixlen) == -1) {
        return -1;
    }
    Py_DECREF(py_obj);

    /* packed = param.packed */
    py_obj = PyObject_GetAttrString(param, "packed");
    if (py_obj== NULL) {
        return -1;
    }

    ret = ip_encode_at(family, prefixlen, 0, py_obj, loc);
    Py_DECREF(py_obj);
    return ret;
}


static param_handler ip_interface_handler = {
    ip_interface_examine,       /* examine */
    NULL,                       /* total_size */
    NULL,                       /* encode */
    ip_interface_encode_at,     /* encode_at */
    NULL,                       /* free */
    INETOID,                    /* oid */
    INETARRAYOID                /* array_oid */
}; /* static initialized handler */


static param_handler *
new_ip_interface_param_handler(int num_param) {
    return &ip_interface_handler;
}


/* ==== ip network parameter handler specifics ============================== */

static int
ip_network_examine(param_handler *handler, PyObject *param) {
    return ip_examine(param, IPv4Network);
}


static int
ip_network_encode_at(param_handler *handler, PyObject *param,
        char *loc) {

    int family, ret;
    long prefixlen;
    PyObject *py_obj, *py_packed;

    family = get_family(param, IPv4Network);

    /* prefixlen = param.prefixlen */
    if (pyobj_long_attr(param, "prefixlen", &prefixlen) == -1) {
        return -1;
    }

    /* packed = param.network_address.packed */
    py_obj = PyObject_GetAttrString(param, "network_address");
    if (py_obj == NULL) {
        return -1;
    }
    py_packed = PyObject_GetAttrString(py_obj, "packed");
    Py_DECREF(py_obj);
    if (py_packed == NULL) {
        return -1;
    }

    ret = ip_encode_at(family, prefixlen, 1, py_packed, loc);
    Py_DECREF(py_packed);
    return ret;
}


static param_handler ip_network_handler = {
    ip_network_examine,         /* examine */
    NULL,                       /* total_size */
    NULL,                       /* encode */
    ip_network_encode_at,       /* encode_at */
    NULL,                       /* free */
    CIDROID,                    /* oid */
    CIDRARRAYOID                /* array_oid */
}; /* static initialized handler */


static param_handler *
new_ip_network_param_handler(int num_param) {
    return &ip_network_handler;
}

/* ======== initialization ================================================== */

PoqueValueHandler mac_val_handler = {{mac_strval, mac_binval}, ',', NULL};
PoqueValueHandler mac8_val_handler = {{mac8_strval, mac8_binval}, ',', NULL};
PoqueValueHandler inet_val_handler = {{inet_strval, inet_binval}, ',', NULL};
PoqueValueHandler cidr_val_handler = {{cidr_strval, cidr_binval}, ',', NULL};

PoqueValueHandler macarray_val_handler = {
        {array_strval, array_binval}, ',', &mac_val_handler};
PoqueValueHandler mac8array_val_handler = {
        {array_strval, array_binval}, ',', &mac8_val_handler};
PoqueValueHandler inetarray_val_handler = {
        {array_strval, array_binval}, ',', &inet_val_handler};
PoqueValueHandler cidrarray_val_handler = {
        {array_strval, array_binval}, ',', &cidr_val_handler};


int
init_network(void)
{
    IPv4Network = load_python_type("ipaddress", "IPv4Network");
    IPv4Interface = load_python_type("ipaddress", "IPv4Interface");
    IPv6Network = load_python_type("ipaddress", "IPv6Network");
    IPv6Interface = load_python_type("ipaddress", "IPv6Interface");

    register_parameter_handler(IPv4Interface, new_ip_interface_param_handler);
    register_parameter_handler(IPv6Interface, new_ip_interface_param_handler);
    register_parameter_handler(IPv4Network, new_ip_network_param_handler);
    register_parameter_handler(IPv6Network, new_ip_network_param_handler);
    register_compatible_param(IPv4Interface, IPv6Interface);
    register_compatible_param(IPv4Network, IPv6Network);
    return 0;
};

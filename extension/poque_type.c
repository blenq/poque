#include "poque.h"
#include "cursor.h"
#include "numeric.h"
#include <datetime.h>


static PyObject *
load_python_object(const char *module_name, const char *obj_name) {
    PyObject *module, *obj;

    module = PyImport_ImportModule(module_name);
    if (module == NULL)
        return NULL;

    obj = PyObject_GetAttrString(module, obj_name);
    Py_DECREF(module);
    return obj;
}


static long min_year;
static long max_year;

static int
datetime_long_attr(PyObject *mod, const char *attr, long *value) {
    PyObject *py_value;

    py_value = PyObject_GetAttrString(mod, attr);
    if (py_value == NULL) {
        return -1;
    }
    *value = PyLong_AsLong(py_value);
    Py_DECREF(py_value);
    return 0;
}

int
init_datetime(void)
{   /* Initializes datetime API and get min and max year */

    PyObject *datetime_module;

    /* necessary to call PyDate API */
    PyDateTime_IMPORT;

    /* load datetime module */
    datetime_module = PyImport_ImportModule("datetime");
    if (datetime_module == NULL)
        return -1;

    /* get min and max year */
    if ((datetime_long_attr(datetime_module, "MINYEAR", &min_year) != 0) ||
            (datetime_long_attr(datetime_module, "MAXYEAR", &max_year) != 0)) {
        Py_DECREF(datetime_module);
        return -1;
    }
    Py_DECREF(datetime_module);
    return 0;
}

typedef PyObject *(*pq_read)(data_crs *crs);

typedef struct {
    Oid oid;
    pq_read binval;
    pq_read strval;
} PoqueTypeDef;


static char
hex_to_char(char hex) {
    char c = -1;
    static const char const hex_vals[] = {
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
        0, 1, 2, 3, 4, 5, 6, 7, 8, 9, -1, -1, -1, -1, -1, -1,
        -1, 10, 11, 12, 13, 14, 15, -1, -1, -1, -1, -1, -1, -1, -1, -1,
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
        -1, 10, 11, 12, 13, 14, 15
    };

    if (hex >= 0x30 && hex <= 0x66)
        c = hex_vals[(unsigned char)hex];
    if (c == -1)
        PyErr_SetString(PoqueError, "Invalid hexadecimal character");
    return c;
}


static int
bytea_fill_fromhex(char *data, char *end, char *dest) {
    /* Converts a hexadecimal bytea value to a binary value */

    char *src;

    src = data + 2;  /* skip hex prefix ('\x') */
    while (src < end) {
        char v1, v2;

        v1 = hex_to_char(*src++);
        if (v1 == -1) {
            return -1;
        }
        if (src == end) {
            PyErr_SetString(
                PoqueError,
                "Odd number of hexadecimal characters in bytea value");
            return -1;
        }
        v2 = hex_to_char(*src++);
        if (v2 == -1) {
            return -1;
        }
        *dest++ = (v1 << 4) | v2;
    }
    return 0;
}


static int
bytea_fill_fromescape(char *data, char *end, char *dest) {
    /* Converts a classically escaped bytea value to a binary value */

    while (data < end) {
        /* fill destination buffer */
        if (data[0] != '\\')
            /* regular byte */
        	*dest = *data++;
        else if ((data[1] >= '0' && data[1] <= '3') &&
                 (data[2] >= '0' && data[2] <= '7') &&
                 (data[3] >= '0' && data[3] <= '7')) {
            /* escaped octal value */
        	*dest = (data[1] - '0') << 6 | (data[2] - '0') << 3 | (data[3] - '0');
        	data += 4;
        }
        else if (data[1] == '\\') {
            /* escaped backslash */
        	*dest = '\\';
        	data += 2;
        }
        else {
            /* Should be impossible, but compiler complains */
            PyErr_SetString(PoqueError, "Invalid escaped bytea value");
            return -1;
        }
        dest++;
    }
    return 0;
}


static PyObject *
bytea_strval(data_crs *crs)
{
    /* converts the textual representation of a bytea value to a Python
     * bytes value
     */
    int bytea_len;
    char *data, *dest, *end;
    PyObject *bytea = NULL;
    int (*fill_func)(char *, char *, char *);

    data = crs_advance_end(crs);
    end = crs_end(crs);

    /* determine number of bytes and parse function based on format */
    if (strncmp(data, "\\x", 2) == 0) {
        /* hexadecimal format */
        bytea_len = (crs_len(crs) - 2) / 2;
        fill_func = bytea_fill_fromhex;
    } else {
        /* escape format */
        char *src = data;
        bytea_len = 0;
        while (src < end) {
            /* get length of value in bytes */
            if (src[0] != '\\')
                /* just a byte */
                src++;
            else if (end - src >= 4 &&
            		 (src[1] >= '0' && src[1] <= '3') &&
                     (src[2] >= '0' && src[2] <= '7') &&
                     (src[3] >= '0' && src[3] <= '7'))
                /* octal value */
                src += 4;
            else if (end - src >= 2 && src[1] == '\\')
                /* escaped backslash */
                src += 2;
            else {
                /* erronous value */
                PyErr_SetString(PoqueError, "Invalid escaped bytea value");
                return NULL;
            }
            bytea_len++;
        }
        fill_func = bytea_fill_fromescape;
    }

    /* Create the Python bytes value using the determined length */
    bytea = PyBytes_FromStringAndSize(NULL, bytea_len);
    if (bytea == NULL)
        return NULL;

    /* Fill the newly created bytes value with the appropriate function */
    dest = PyBytes_AsString(bytea);
    if (fill_func(data, end, dest) < 0) {
        Py_DECREF(bytea);
        return NULL;
    }

    return bytea;
}

static PyObject *bytea_binval(data_crs* crs)
{
    char *data;

    data = crs_advance_end(crs);
    return PyBytes_FromStringAndSize(data, crs_len(crs));
}


static PyObject *char_binval(data_crs* crs)
{
    char data;

    if (crs_read_char(crs, &data) < 0)
        return NULL;
    return PyBytes_FromStringAndSize(&data, 1);
}


static PyObject *
text_val(data_crs* crs)
{
    char *data;

    data = crs_advance_end(crs);
    return PyUnicode_FromStringAndSize(data, crs_len(crs));
}


static pq_read
get_read_func(Oid oid, int format, Oid *el_oid);


static PyObject *
read_value(char *data, int len, pq_read read_func, Oid el_oid) {
    data_crs crs;
    PyObject *val;

    crs_init(&crs, data, len, el_oid);
    val = read_func(&crs);
    if (val == NULL)
        return NULL;
    if (crs_at_end(&crs)) {
        return val;
    }
    /* read function has not consumed all the data */
    Py_DECREF(val);
    PyErr_SetString(PoqueError, "Invalid data format");
    return NULL;
}


static PyObject *
get_arr_value(data_crs *crs, PY_INT32_T *arraydims, pq_read read_func,
		      Oid el_oid)
{
    /* Get multidimensional array as a nested list of item values
     * crs: The data cursor
     * arraydims: A -1 terminated array of dim lengths
     * read_func: the function used to read an item
     */
    PY_INT32_T dim;

    dim = arraydims[0];
    if (dim == -1) {
        /* At a leaf within the lists tree structure, add actual item */
        PY_INT32_T item_len;
        char *data;

        /* get item length */
        if (crs_read_int32(crs, &item_len) < 0) {
            return NULL;
        }
        /* -1 indicates NULL value */
        if (item_len == -1)
            Py_RETURN_NONE;

        if (item_len < 0) {
            PyErr_SetString(PoqueError, "Invalid length");
            return NULL;
        }

        /* advance cursor past item */
        data = crs_advance(crs, item_len);
        if (data == NULL)
            return NULL;

        /* read the item, this will use its own cursor */
        return read_value(data, item_len, read_func, el_oid);
    }
    else {
        /* At a container level, create a list and fill with items
         * recursively
         */
        int i;
        PyObject *lst, *new_item;

        /* create a list */
        lst = PyList_New(dim);
        if (lst == NULL)
            return NULL;

        /* fill the list with items */
        for (i = 0; i < dim; i++) {
            new_item = get_arr_value(crs, arraydims + 1, read_func, el_oid);
            if (new_item == NULL) {
                Py_DECREF(lst);
                return NULL;
            }
            PyList_SET_ITEM(lst, i, new_item);
        }
        return lst;
    }
}


static PyObject *
array_binval(data_crs *crs) {
    int i;
    PY_UINT32_T dims;
    PY_INT32_T flags, arraydims[7];
    Oid elem_type, sub_elem_type=InvalidOid;
    pq_read read_func;

    /* get number of dimensions */
    if (crs_read_uint32(crs, &dims) < 0)
        return NULL;
    if (dims > 6) {
        PyErr_SetString(PoqueError, "Number of dimensions exceeded");
        return NULL;
    }

    /* get flags */
    if (crs_read_int32(crs, &flags) < 0)
        return NULL;
    if ((flags & 1) != flags) {
        PyErr_SetString(PoqueError, "Invalid value for array flags");
        return NULL;
    }

    /* get the element datatype */
    if (crs_read_uint32(crs, &elem_type) < 0)
        return NULL;

    /* check if element type corresponds with array type */
    if (elem_type != crs_el_oid(crs)) {
        PyErr_SetString(PoqueError, "Unexpected element type");
        return NULL;
    }

    /* zero dimension array, just return an empty list */
    if (dims == 0)
        return PyList_New(0);

    /* find corresponding read function */
    read_func = get_read_func(elem_type, 1, &sub_elem_type);
    if (read_func == NULL)
        return NULL;

    /* fill array with dimension lengths */
    for (i = 0; i < dims; i++) {
        int arraydim;

        if (crs_read_int32(crs, &arraydim) < 0)
            return NULL;
        if (arraydim < 0) {
            PyErr_SetString(PoqueError, "Negative number of items");
            return NULL;
        }
        arraydims[i] = arraydim;

        crs_advance(crs, 4); /* skip lower bounds */
    }
    arraydims[i] = -1;  /* terminate dimensions array */

    /* actually get the array */
    return get_arr_value(crs, arraydims, read_func, sub_elem_type);
}


static PyObject *
tid_binval(data_crs *crs)
{
    PY_UINT32_T block_num;
    poque_uint16 offset;
    PyObject *tid, *tmp;

    if (crs_read_uint32(crs, &block_num) < 0)
        return NULL;
    if (crs_read_uint16(crs, &offset) < 0)
        return NULL;
    tid = PyTuple_New(2);
    if (tid == NULL)
        return NULL;
    tmp = PyLong_FromLongLong(block_num);
    if (tmp == NULL) {
        Py_DECREF(tid);
        return NULL;
    }
    PyTuple_SET_ITEM(tid, 0, tmp);
    tmp = PyLong_FromLong(offset);
    if (tmp == NULL) {
        Py_DECREF(tid);
        return NULL;
    }
    PyTuple_SET_ITEM(tid, 1, tmp);
    return tid;
}


static PyObject *
tid_strval(data_crs *crs)
{
    PyObject *tid, *bl_num;
    char *dt, *bl_data, *pend;

    dt = crs_advance(crs, 1);
    if (dt == NULL)
        return NULL;
    if (dt[0] != '(' || dt[crs_len(crs) - 1] != ')') {
        PyErr_SetString(PoqueError, "Invalid tid value");
        return NULL;
    }
    bl_data = dt + 1;
    while (1) {
        dt = crs_advance(crs, 1);
        if (dt == NULL) {
            PyErr_SetString(PoqueError, "Invalid tid value");
            return NULL;
        }
        if (dt[0] == ',') {
            break;
        }
    }

    tid = PyTuple_New(2);
    if (tid == NULL)
        return NULL;

    dt[0] = '\0';
    bl_num = PyLong_FromString(bl_data, &pend, 10);
    if (pend != dt)
    	PyErr_SetString(PoqueError, "Invalid tid value");
    dt[0] = ',';
    if (bl_num == NULL) {
        Py_DECREF(tid);
        return NULL;
    }
    PyTuple_SET_ITEM(tid, 0, bl_num);
    *(crs_end(crs) - 1) = '\0';
    bl_num = PyLong_FromString(dt + 1, &pend, 10);
    if (pend != crs_end(crs) - 1)
    	PyErr_SetString(PoqueError, "Invalid tid value");
    *(crs_end(crs) - 1) = ')';
    crs_advance_end(crs);
    if (bl_num == NULL) {
        Py_DECREF(tid);
        return NULL;
    }
    PyTuple_SET_ITEM(tid, 1, bl_num);
    return tid;
}

static PyObject *json_loads;

static PyObject *
json_val(data_crs *crs)
{
    PyObject *ret;
    int remaining;

    remaining = crs_remaining(crs);
    ret = PyObject_CallFunction(
    		json_loads, "s#", crs_advance_end(crs), remaining);
    return ret;
}


static PyObject *
jsonb_bin_val(data_crs *crs)
{
    if (crs_advance(crs, 1)[0] != 1)
        PyErr_SetString(PoqueError, "Invalid jsonb version");
    return json_val(crs);
}

#define UUID_LEN    16


static PyObject *
uuid_binval(data_crs *crs) {
    char *data;
    PyObject *uuid_cls, *uuid;

    data = crs_advance(crs, UUID_LEN);
    if (data == NULL)
        return NULL;
    uuid_cls = load_python_object("uuid", "UUID");
    if (uuid_cls == NULL)
        return NULL;
    uuid = PyObject_CallFunction(uuid_cls, "sy#", NULL, data, UUID_LEN);
    Py_DECREF(uuid_cls);
    return uuid;
}


static PyObject *
uuid_strval(data_crs *crs)
{
    char *data;
    PyObject *uuid_cls, *uuid;

    data = crs_advance_end(crs);
    uuid_cls = load_python_object("uuid", "UUID");
    if (uuid_cls == NULL)
        return NULL;
    uuid = PyObject_CallFunction(uuid_cls, "s#", data, crs_end(crs) - data);
    Py_DECREF(uuid_cls);
    return uuid;
}


static int
add_float_to_tuple(PyObject *tup, data_crs *crs, int idx)
{
    PyObject *float_val;

    float_val = float64_binval(crs);
    if (float_val == NULL) {
        Py_DECREF(tup);
        return -1;
    }
    PyTuple_SET_ITEM(tup, idx, float_val);
    return 0;
}


static PyObject *
float_tuple_binval(data_crs *crs, int len) {
    PyObject *tup;
    int i;

    tup = PyTuple_New(len);
    if (tup == NULL)
        return NULL;
    for (i=0; i < len; i++) {
        if (add_float_to_tuple(tup, crs, i) < 0)
            return NULL;
    }
    return tup;
}

static PyObject *
point_binval(data_crs *crs) {
    return float_tuple_binval(crs, 2);
}


static PyObject *
line_binval(data_crs *crs) {
    return float_tuple_binval(crs, 3);
}


static PyObject *
lseg_binval(data_crs *crs)
{
    PyObject *point, *lseg;
    int i;

    lseg = PyTuple_New(2);
    if (lseg == NULL)
        return NULL;
    for (i = 0; i < 2; i++) {
        point = point_binval(crs);
        if (point == NULL) {
            Py_DECREF(lseg);
            return NULL;
        }
        PyTuple_SET_ITEM(lseg, i, point);
    }
    return lseg;
}


static PyObject *
polygon_binval(data_crs *crs) {
    PyObject *points;
    PY_INT32_T npoints, i;

    if (crs_read_int32(crs, &npoints) < 0) {
        return NULL;
    }
    if (npoints < 0) {
        PyErr_SetString(PoqueError, "Path length can not be less than zero");
        return NULL;
    }
    points = PyList_New(npoints);
    for (i = 0; i < npoints; i++) {
        PyObject *point;

        point = point_binval(crs);
        if (point == NULL) {
            Py_DECREF(points);
            return NULL;
        }
        PyList_SET_ITEM(points, i, point);
    }
    return points;
}


static PyObject *
path_binval(data_crs *crs)
{
    PyObject *path, *points, *closed;

    closed = bool_binval(crs);
    if (closed == NULL)
        return NULL;

    points = polygon_binval(crs);
    if (points == NULL) {
        Py_DECREF(closed);
        return NULL;
    }

    path = PyDict_New();
    if (path == NULL) {
        Py_DECREF(closed);
        Py_DECREF(points);
    }

    if ((PyDict_SetItemString(path, "closed", closed) < 0) ||
            (PyDict_SetItemString(path, "path", points) < 0)) {
        Py_DECREF(path);
        path = NULL;
    }
    Py_DECREF(closed);
    Py_DECREF(points);
    return path;
}


static PyObject *
abstime_binval(data_crs *crs)
{
    PyObject *abstime, *seconds, *args;
    PY_INT32_T value;

    if (crs_read_int32(crs, &value) < 0)
        return NULL;
    seconds = PyLong_FromLong(value);
    if (seconds == NULL)
        return NULL;
    args = PyTuple_New(1);
    if (args == NULL) {
        Py_DECREF(seconds);
        return NULL;
    }
    PyTuple_SET_ITEM(args, 0, seconds);
    abstime = PyDateTime_FromTimestamp(args);
    Py_DECREF(args);
    return abstime;
}


static PyObject *
reltime_binval(data_crs *crs)
{
    PY_INT32_T value;

    if (crs_read_int32(crs, &value) < 0)
        return NULL;
    return PyDelta_FromDSU(0, value, 0);
}


static PyObject *
tinterval_binval(data_crs *crs) {
    PyObject *tinterval, *abstime;
    int i;

    crs_advance(crs, 4);
    tinterval = PyTuple_New(2);
    if (tinterval == NULL)
        return NULL;
    for (i = 0; i < 2; i++) {
        abstime = abstime_binval(crs);
        if (abstime == NULL) {
            Py_DECREF(tinterval);
            return NULL;
        }
        PyTuple_SET_ITEM(tinterval, i, abstime);
    }
    return tinterval;
}


static PyObject *
circle_binval(data_crs *crs) {
    PyObject *circle, *center;

    circle = PyTuple_New(2);
    if (circle == NULL) {
        return NULL;
    }
    center = point_binval(crs);
    if (center == NULL) {
        Py_DECREF(circle);
        return NULL;
    }
    PyTuple_SET_ITEM(circle, 0, center);
    if (add_float_to_tuple(circle, crs, 1) < 0) {
        Py_DECREF(circle);
        return NULL;
    }
    return circle;
}


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


#define PGSQL_AF_INET 2
#define PGSQL_AF_INET6 3

static PyObject *
inet_binval(data_crs *crs)
{
    PyObject *addr_cls, *addr_str, *address=NULL;
    char *cls_name;
    unsigned char mask, *cr, family, size, is_cidr;

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

    /* Get address as string and Python class name */
    if (family == PGSQL_AF_INET ) {

        if (size != 4) {
            PyErr_SetString(PoqueError, "Invalid address size");
            return NULL;
        }
        cr = (unsigned char *)crs_advance(crs, size);
        if (cr == NULL)
            return NULL;
        addr_str = PyUnicode_FromFormat(
            "%u.%u.%u.%u/%u", cr[0], cr[1], cr[2], cr[3], mask);
        if (is_cidr)
            cls_name = "IPv4Network";
        else
            cls_name = "IPv4Interface";
    } else if (family == PGSQL_AF_INET6) {
        poque_uint16 parts[8];
        int i;

        if (size != 16) {
            PyErr_SetString(PoqueError, "Invalid address size");
            return NULL;
        }
        for (i = 0; i < 8; i++) {
            if (crs_read_uint16(crs, parts + i) < 0)
                return NULL;
        }
        addr_str = PyUnicode_FromFormat(
            "%x:%x:%x:%x:%x:%x:%x:%x/%u", parts[0], parts[1], parts[2],
            parts[3], parts[4], parts[5], parts[6], parts[7], mask);
        if (is_cidr)
            cls_name = "IPv6Network";
        else
            cls_name = "IPv6Interface";
    } else {
        PyErr_SetString(PoqueError, "Unknown network family");
        return NULL;
    }
    if (addr_str == NULL)
        return NULL;

    /* Instantiate network class */
    addr_cls = load_python_object("ipaddress", cls_name);
    if (addr_cls == NULL)
        goto end;
    address = PyObject_CallFunctionObjArgs(addr_cls, addr_str, NULL);
end:
    Py_DECREF(addr_str);
    Py_XDECREF(addr_cls);
    return address;
}


#define POSTGRES_EPOCH_JDATE   2451545

static void
date_vals_from_int(PY_INT32_T jd, int *year, int *month, int *day)
{
    unsigned int julian, quad, extra;
    int y;

    /* julian day magic to retrieve day, month and year, shamelessly copied
     * from postgres server code */
    julian = jd + POSTGRES_EPOCH_JDATE;
    julian += 32044;
    quad = julian / 146097;
    extra = (julian - quad * 146097) * 4 + 3;
    julian += 60 + quad * 3 + extra / 146097;
    quad = julian / 1461;
    julian -= quad * 1461;
    y = julian * 4 / 1461;
    julian = ((y != 0) ? ((julian + 305) % 365) : ((julian + 306) % 366))
           + 123;
    y += quad * 4;
    *year = y - 4800;
    quad = julian * 2141 / 65536;
    *day = julian - 7834 * quad / 256;
    *month = (quad + 10) % 12 + 1;
}


static PyObject *
date_binval(data_crs *crs)
{
    PY_INT32_T jd;
    int year, month, day;
    char *fmt;

    if (crs_read_int32(crs, &jd) < 0)
        return NULL;

    date_vals_from_int(jd, &year, &month, &day);

    /* if outside python date range convert to a string */
    if (year > max_year)
        fmt = "%i-%02i-%02i";
    else if (year < min_year) {
        fmt = "%04i-%02i-%02i BC";
        year = -1 * (year - 1);  /* There is no year zero */
    }
    else
        return PyDate_FromDate(year, month, day);
    return PyUnicode_FromFormat(fmt, year, month, day);
}


#define USECS_PER_DAY       86400000000
#define USECS_PER_HOUR      Py_LL(3600000000) /* this might become a uint32 */
#define USECS_PER_MINUTE    60000000
#define USECS_PER_SEC       1000000

static int
time_vals_from_int(PY_INT64_T tm, int *hour, int *minute, int *second,
                   int *usec)
{
    PY_INT64_T hr;

    hr = (int)(tm / USECS_PER_HOUR);
    if (tm < 0 || hr > 23) {
        PyErr_SetString(PoqueError, "Invalid time value");
        return -1;
    }
    *hour = (int)hr;
    tm -= hr * USECS_PER_HOUR;
    *minute = (int)(tm / USECS_PER_MINUTE);
    tm -= *minute * USECS_PER_MINUTE;
    *second = (int)(tm / USECS_PER_SEC);
    *usec = (int)(tm - *second * USECS_PER_SEC);
    return 0;
}


static PyObject *
_time_binval(data_crs *crs, PY_INT64_T value, PyObject *tz)
{
    int hour, minute, second, usec;

    if (time_vals_from_int(value, &hour, &minute, &second, &usec) < 0)
        return NULL;
    return PyDateTimeAPI->Time_FromTime(hour, minute, second, usec, tz,
                                        PyDateTimeAPI->TimeType);
}


static PyObject *
time_binval(data_crs *crs)
{
    PY_INT64_T value;

    if (crs_read_int64(crs, &value) < 0)
        return NULL;
    return _time_binval(crs, value, Py_None);
}


static PyObject *
get_utc(void) {

    static PyObject *utc;
    PyObject *tz;

    if (utc == NULL) {
        tz = load_python_object("datetime", "timezone");
        if (tz == NULL)
            return NULL;
        utc = PyObject_GetAttrString(tz, "utc");
        Py_DECREF(tz);
    }
    return utc;
}


static PyObject *
timetz_binval(data_crs *crs)
{
    PyObject *tz, *timedelta, *ret, *offset, *timezone;
    PY_INT64_T value;
    int seconds;

    if (crs_read_int64(crs, &value) < 0)
        return NULL;
    if (crs_read_int32(crs, &seconds) < 0)
        return NULL;

    timedelta = load_python_object("datetime", "timedelta");
    if (timedelta == NULL)
        return NULL;
    offset = PyObject_CallFunction(timedelta, "ii", 0, -seconds);
    Py_DECREF(timedelta);
    if (offset == NULL)
        return NULL;

    tz = load_python_object("datetime", "timezone");
    if (tz == NULL) {
        Py_DECREF(offset);
        return NULL;
    }
    timezone = PyObject_CallFunctionObjArgs(tz, offset, NULL);
    Py_DECREF(offset);
    Py_DECREF(tz);
    if (timezone == NULL)
        return NULL;

    ret = _time_binval(crs, value, timezone);
    Py_DECREF(timezone);
    return ret;
}


static PyObject *
_timestamp_binval(data_crs *crs, PyObject *tz)
{
    PY_INT64_T value, time;
    PY_INT32_T date;
    int year, month, day, hour, minute, second, usec;
    char *fmt;

    if (crs_read_int64(crs, &value) < 0)
        return NULL;
    if (value == PY_LLONG_MAX)
        return PyUnicode_FromString("infinity");
    if (value == PY_LLONG_MIN)
        return PyUnicode_FromString("-infinity");
    date = (PY_INT32_T)(value / USECS_PER_DAY);
    time = value - date * USECS_PER_DAY;
    if (time < 0) {
        time += USECS_PER_DAY;
        date -= 1;
    }

    date_vals_from_int(date, &year, &month, &day);
    if (time_vals_from_int(time, &hour, &minute, &second, &usec) < 0)
        return NULL;
    if (year > max_year) {
        fmt = "%i-%02i-%02i %02i:%02i:%02i.%06i";
    }
    else if (year < min_year) {
        year = -1 * (year - 1);  /* There is no year zero */
        fmt = "%04i-%02i-%02i %02i:%02i:%02i.%06i BC";
    }
    else
        return PyDateTimeAPI->DateTime_FromDateAndTime(
            year, month, day, hour, minute, second, usec, tz,
            PyDateTimeAPI->DateTimeType);
    return PyUnicode_FromFormat(fmt, year, month, day, hour, minute, second,
                                usec);
}


static PyObject *
timestamp_binval(data_crs *crs) {
    return _timestamp_binval(crs, Py_None);
}


static PyObject *
timestamptz_binval(data_crs *crs)
{
    PyObject *utc;

    utc = get_utc();
    if (utc == NULL)
        return NULL;
    return _timestamp_binval(crs, utc);
}


static PyObject *
interval_binval(data_crs *crs)
{
    PY_INT64_T secs, usecs;
    PY_INT32_T days, months;
    PyObject *interval, *value;

    if (crs_read_int64(crs, &usecs) < 0)
        return NULL;
    if (crs_read_int32(crs, &days) < 0)
        return NULL;
    if (crs_read_int32(crs, &months) < 0)
        return NULL;
    interval = PyTuple_New(2);
    if (interval == NULL)
        return NULL;
    value = PyLong_FromLong(months);
    if (value == NULL) {
        Py_DECREF(interval);
        return NULL;
    }
    PyTuple_SET_ITEM(interval, 0, value);
    secs = usecs / USECS_PER_SEC;
    usecs -= secs * USECS_PER_SEC;
    value = PyDelta_FromDSU(days, secs, usecs);
    if (value == NULL) {
        Py_DECREF(interval);
        return NULL;
    }
    PyTuple_SET_ITEM(interval, 1, value);
    return interval;
}

/* reference to decimal.Decimal */
PyObject *PyDecimal;

static PyObject *
numeric_strval(data_crs *crs) {
    /* Create a Decimal from a text value */
    PyObject *strval, *args, *ret;

    /* create text argument tuple */
    args = PyTuple_New(1);
    if (args == NULL) {
        return NULL;
    }
    strval = text_val(crs);
    if (strval == NULL) {
        Py_DECREF(args);
        return NULL;
    }
    PyTuple_SET_ITEM(args, 0, strval);

    /* call decimal constructor */
    ret = PyObject_CallObject(PyDecimal, args);
    Py_DECREF(args);
    return ret;
}


static PyObject *
bit_strval(data_crs *crs) {
    PyObject *val, *one;

    /* initialize return value */
    val = PyLong_FromLong(0);
    if (val == NULL)
        return NULL;

    /* initialize Python integer with value 1 */
    one = PyLong_FromLong(1);
    if (one == NULL) {
        goto error_2;
    }

    /* interpret characters as bits */
    while (!crs_at_end(crs)) {
        char byte;
        PyObject *new_val;

        /* new bit, shift the return value one bit to make space */
        new_val = PyNumber_InPlaceLshift(val, one);
        if (new_val == NULL)
            goto error;
        Py_DECREF(val);
        val = new_val;

        /* Get the new bit as character */
        if (crs_read_char(crs, &byte) < 0)
            goto error;

        /* interpret bit */
        if (byte == '1') {
            /* add the bit to the return value */
            new_val = PyNumber_InPlaceOr(val, one);
            if (new_val == NULL)
                goto error;
            Py_DECREF(val);
            val = new_val;
        }
        else if (byte != '0') {
            PyErr_SetString(PoqueError, "Invalid character in bit string");
            goto error;
        }
    }
    Py_DECREF(one);
    return val;

error:
    Py_DECREF(one);
error_2:
    Py_DECREF(val);
    return NULL;
}


static PyObject *
bit_binval(data_crs *crs) {
    /* Reads a bitstring as a Python integer

    Format:
       * signed int: number of bits (bit_len)
       * bytes: All the bits left aligned

    */
    int bit_len, quot, rest, byte_len, i;
    PyObject *val, *eight, *new_val;

    /* first get the number of bits in the bit string */
    if (crs_read_int32(crs, &bit_len) < 0)
        return NULL;
    if (bit_len < 0) {
        PyErr_SetString(PoqueError,
                        "Invalid length value in binary bit string");
        return NULL;
    }

    /* initialize return value */
    val = PyLong_FromLong(0);
    if (val == NULL)
        return NULL;

    /* initialize Python integer with value 8 */
    eight = PyLong_FromLong(8);
    if (eight == NULL) {
        Py_DECREF(val);
        return NULL;
    }

    quot = bit_len / 8;  /* number of bytes completely filled */
    rest = bit_len % 8;  /* number of bits in remaining byte */
    byte_len = quot + (rest ? 1: 0); /* total number of data bytes */

    /* add the value byte by byte, python ints have no upper limit, so this
     * works even for bitstrings longer than 64 bits */
    for (i = 0; i < byte_len; i++) {
        unsigned char byte;
        PyObject *byte_val;

        /* new byte, first shift the return value one byte to the left, to make
         * space */
        new_val = PyNumber_InPlaceLshift(val, eight);
        if (new_val == NULL)
            goto error;
        Py_DECREF(val);
        val = new_val;

        /* read the new byte */
        if (crs_read_uchar(crs, &byte) < 0)
            goto error;
        byte_val = PyLong_FromLong(byte);
        if (byte_val == NULL)
            goto error;

        /* add the new byte to the return value */
        new_val = PyNumber_InPlaceOr(val, byte_val);
        Py_DECREF(byte_val);
        if (new_val == NULL)
            goto error;
        Py_DECREF(val);
        val = new_val;
    }
    if (rest) {
        /* correct for the fact that the bitstring is left aligned */
        PyObject *shift_val;

        /* get the number of bits to shift the entire value */
        shift_val = PyLong_FromLong(8 - rest);
        if (shift_val == NULL)
            goto error;

        /* shift the value */
        new_val = PyNumber_InPlaceRshift(val, shift_val);
        Py_DECREF(shift_val);
        if (new_val == NULL)
            goto error;
        Py_DECREF(val);
        val = new_val;
    }
    Py_DECREF(eight);
    return val;

error:
    Py_DECREF(eight);
    Py_DECREF(val);
    return NULL;
}


typedef struct _poqueTypeEntry {
    Oid oid;
    pq_read binval;
    pq_read strval;
    Oid el_oid;		/* type of subelement for array_binval converter */
    struct _poqueTypeEntry *next;
} PoqueTypeEntry;

/* static definition of types with value converters */
static PoqueTypeEntry type_table[] = {
    {INT4OID, int32_binval, int_strval, InvalidOid, NULL},
    {VARCHAROID, text_val, NULL, InvalidOid, NULL},
    {INT8OID, int64_binval, int_strval, InvalidOid, NULL},
    {INT2OID, int16_binval, int_strval, InvalidOid, NULL},
    {BOOLOID, bool_binval, bool_strval, InvalidOid, NULL},
    {BYTEAOID, bytea_binval, bytea_strval, InvalidOid, NULL},
    {CHAROID, char_binval, char_binval, InvalidOid, NULL},
    {NAMEOID, text_val, NULL, InvalidOid, NULL},
    {INT2VECTOROID, array_binval, NULL, INT2OID, NULL},
    {REGPROCOID, uint32_binval, NULL, InvalidOid, NULL},
    {TEXTOID, text_val, NULL, InvalidOid, NULL},
    {CSTRINGOID, text_val, NULL, InvalidOid, NULL},
    {CSTRINGARRAYOID, array_binval, NULL, CSTRINGOID, NULL},
    {BPCHAROID, text_val, NULL, InvalidOid, NULL},
    {OIDOID, uint32_binval, int_strval, InvalidOid, NULL},
    {TIDOID, tid_binval, tid_strval, InvalidOid, NULL},
    {XIDOID, uint32_binval, int_strval, InvalidOid, NULL},
    {CIDOID, uint32_binval, int_strval, InvalidOid, NULL},
    {OIDVECTOROID, array_binval, NULL, OIDOID, NULL},
    {JSONOID, json_val, json_val, InvalidOid, NULL},
    {JSONBOID, jsonb_bin_val, json_val, InvalidOid, NULL},
    {XMLOID, text_val, text_val, InvalidOid, NULL},
    {XMLARRAYOID, array_binval, NULL, XMLOID, NULL},
    {JSONARRAYOID, array_binval, NULL, JSONOID, NULL},
    {JSONBARRAYOID, array_binval, NULL, JSONBOID, NULL},
    {POINTOID, point_binval, NULL, InvalidOid, NULL},
    {LINEOID, line_binval, NULL, InvalidOid, NULL},
    {LINEARRAYOID, array_binval, NULL, LINEOID, NULL},
    {LSEGOID, lseg_binval, NULL, InvalidOid, NULL},
    {PATHOID, path_binval, NULL, InvalidOid, NULL},
    {BOXOID, lseg_binval, NULL, InvalidOid, NULL},
    {POLYGONOID, polygon_binval, NULL, InvalidOid, NULL},
    {ABSTIMEOID, abstime_binval, NULL, InvalidOid, NULL},
    {RELTIMEOID, reltime_binval, NULL, InvalidOid, NULL},
    {TINTERVALOID, tinterval_binval, NULL, InvalidOid, NULL},
    {UNKNOWNOID, text_val, NULL, InvalidOid, NULL},
    {CIRCLEOID, circle_binval, NULL, InvalidOid, NULL},
    {CIRCLEARRAYOID, array_binval, NULL, CIRCLEOID, NULL},
    {CASHOID, int64_binval, NULL, InvalidOid, NULL},
    {CASHARRAYOID, array_binval, NULL, CASHOID, NULL},
    {MACADDROID, mac_binval, NULL, InvalidOid, NULL},
	{MACADDR8OID, mac8_binval, NULL, InvalidOid, NULL},
    {INETOID, inet_binval, NULL, InvalidOid, NULL},
    {CIDROID, inet_binval, NULL, InvalidOid, NULL},
    {BOOLARRAYOID, array_binval, NULL, BOOLOID, NULL},
    {BYTEAARRAYOID, array_binval, NULL, BYTEAOID, NULL},
    {CHARARRAYOID, array_binval, NULL, CHAROID, NULL},
    {NAMEARRAYOID, array_binval, NULL, NAMEOID, NULL},
    {INT2ARRAYOID, array_binval, NULL, INT2OID, NULL},
    {INT2VECTORARRAYOID, array_binval, NULL, INT2VECTOROID, NULL},
    {REGPROCARRAYOID, array_binval, NULL, REGPROCOID, NULL},
    {TEXTARRAYOID, array_binval, NULL, TEXTOID, NULL},
    {OIDARRAYOID, array_binval, NULL, OIDOID, NULL},
    {TIDARRAYOID, array_binval, NULL, TIDOID, NULL},
    {XIDARRAYOID, array_binval, NULL, XIDOID, NULL},
    {CIDARRAYOID, array_binval, NULL, CIDOID, NULL},
    {BPCHARARRAYOID, array_binval, NULL, BPCHAROID, NULL},
    {VARCHARARRAYOID, array_binval, NULL, VARCHAROID, NULL},
    {OIDVECTORARRAYOID, array_binval, NULL, OIDVECTOROID, NULL},
    {INT8ARRAYOID, array_binval, NULL, INT8OID, NULL},
    {POINTARRAYOID, array_binval, NULL, POINTOID, NULL},
    {LSEGARRAYOID, array_binval, NULL, LSEGOID, NULL},
    {PATHARRAYOID, array_binval, NULL, PATHOID, NULL},
    {BOXARRAYOID, array_binval, NULL, BOXOID, NULL},
    {FLOAT4ARRAYOID, array_binval, NULL, FLOAT4OID, NULL},
    {FLOAT8ARRAYOID, array_binval, NULL, FLOAT8OID, NULL},
    {ABSTIMEARRAYOID, array_binval, NULL, ABSTIMEOID, NULL},
    {RELTIMEARRAYOID, array_binval, NULL, RELTIMEOID, NULL},
    {TINTERVALARRAYOID, array_binval, NULL, TINTERVALOID, NULL},
    {POLYGONARRAYOID, array_binval, NULL, POLYGONOID, NULL},
    {MACADDRARRAYOID, array_binval, NULL, MACADDROID, NULL},
    {MACADDR8ARRAYOID, array_binval, NULL, MACADDR8OID, NULL},
    {INETARRAYOID, array_binval, NULL, INETOID, NULL},
    {CIDRARRAYOID, array_binval, NULL, CIDROID, NULL},
    {FLOAT8OID, float64_binval, float_strval, InvalidOid, NULL},
    {FLOAT4OID, float32_binval, float_strval, InvalidOid, NULL},
    {INT4ARRAYOID, array_binval, NULL, INT4OID, NULL},
    {UUIDOID, uuid_binval, uuid_strval, InvalidOid, NULL},
    {DATEOID, date_binval, NULL, InvalidOid, NULL},
    {TIMEOID, time_binval, NULL, InvalidOid, NULL},
    {TIMETZOID, timetz_binval, NULL, InvalidOid, NULL},
    {TIMESTAMPOID, timestamp_binval, NULL, InvalidOid, NULL},
    {TIMESTAMPTZOID, timestamptz_binval, NULL, InvalidOid, NULL},
    {DATEARRAYOID, array_binval, NULL, DATEOID, NULL},
    {TIMESTAMPARRAYOID, array_binval, NULL, TIMESTAMPOID, NULL},
    {TIMESTAMPTZARRAYOID, array_binval, NULL, TIMESTAMPTZOID, NULL},
    {TIMEARRAYOID, array_binval, NULL, TIMEOID, NULL},
    {INTERVALOID, interval_binval, NULL, InvalidOid, NULL},
    {INTERVALARRAYOID, array_binval, NULL, INTERVALOID, NULL},
    {NUMERICOID, numeric_binval, numeric_strval, InvalidOid, NULL},
    {NUMERICARRAYOID, array_binval, NULL, NUMERICOID, NULL},
    {BITOID, bit_binval, bit_strval, InvalidOid, NULL},
    {BITARRAYOID, array_binval, NULL, BITOID, NULL},
    {VARBITOID, bit_binval, bit_strval, InvalidOid, NULL},
    {VARBITARRAYOID, array_binval, NULL, VARBITOID, NULL},
    {InvalidOid}
};

#define TYPEMAP_SIZE 128

/* hash table of value converters */
static PoqueTypeEntry *type_map[TYPEMAP_SIZE];


int
init_type_map(void) {
    PoqueTypeEntry *entry;
//    int j = 0;

    PyDecimal = load_python_object("decimal", "Decimal");
    if (PyDecimal == NULL)
        return -1;

    json_loads = load_python_object("json", "loads");
    if (json_loads == NULL)
    	return -1;

    /* initialize hash table of value converters */
    entry = type_table;
    while (entry->oid != InvalidOid) {
        size_t idx = entry->oid % TYPEMAP_SIZE;
        PoqueTypeEntry *prev = type_map[idx];
        if (prev == NULL) {
            type_map[idx] = entry;
        }
        else {
            while (prev->next) {
                prev = prev->next;
            }
            prev->next = entry;
        }
        entry++;
        //j++;
    }
/*    int i;
    int filled = 0;
    for (i = 0; i < TYPEMAP_SIZE; i++) {
        if (type_map[i])
            filled++;
    }
    printf("%f\n", 1.0 * j / TYPEMAP_SIZE);
    printf("%f\n", 1.0 * filled / TYPEMAP_SIZE); */
    return 0;
}


static pq_read
get_read_func(Oid oid, int format, Oid *el_oid) {
    size_t idx;
    PoqueTypeEntry *entry;

    idx = oid % TYPEMAP_SIZE;
    for (entry = type_map[idx]; entry; entry = entry->next) {
        if (entry->oid != oid)
            continue;
        *el_oid = entry->el_oid;
        if (format == 1) {
            return entry->binval ? entry->binval : bytea_binval;
        } else {
            return entry->strval ? entry->strval : text_val;
        }
    }
    if (format == 1)
        return bytea_binval;
    return text_val;
}


PyObject *
Poque_value(Oid oid, int format, char *data, int len) {
    pq_read read_func;
    Oid el_oid=InvalidOid;

    if (format == 0 && data[len] != '\0') {
        PyErr_SetString(PoqueError, "Invalid text format");
        return NULL;
    }

    read_func = get_read_func(oid, format, &el_oid);
    if (read_func == NULL) {
        return NULL;
    }

    return read_value(data, len, read_func, el_oid);
}

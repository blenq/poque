#include "poque.h"
#include <datetime.h>
#include <endian.h>


void
init_datetime(void)
{
    PyDateTime_IMPORT;
}


typedef struct {
    char *data;
    int len;
    int item_len;
} data_crs;

typedef PyObject *(*pq_read)(data_crs *curs);

typedef struct {
    Oid oid;
    pq_read binval;
    pq_read strval;
} PoqueTypeDef;


static char *
advance_cursor(data_crs *curs) {
    char *data;

    data = curs->data;
    if (curs->len < curs->item_len) {
        PyErr_SetString(PoqueError, "Item length exceeds data length");
        return NULL;
    }
    curs->data += curs->item_len;
    curs->len -= curs->item_len;
    return data;
}


static char *
advance_cursor_len(data_crs *curs, int len) {
    curs->item_len = len;
    return advance_cursor(curs);
}


static int
read_uint2_binval(data_crs *curs, uint16_t *value)
{
    uint16_t val;
    char *data;

    data = advance_cursor_len(curs, 2);
    if (data == NULL)
        return -1;

    val = *(uint16_t *)data;
    *value = be16toh(val);
    return 0;
}


static PyObject *
int2_binval(data_crs *curs)
{
    uint16_t value;
    if (read_uint2_binval(curs, &value) < 0) {
        return NULL;
    }
    return PyLong_FromLong((int16_t)value);
}


static int
read_uint4_binval(data_crs *curs, PY_UINT32_T *value)
{
    PY_UINT32_T val;
    char *data;

    data = advance_cursor_len(curs, 4);
    if (data == NULL)
        return -1;

    val = *(PY_UINT32_T *)data;
    *value = be32toh(val);
    return 0;
}


static PyObject *
uint32_binval(data_crs *curs)
{
    PY_UINT32_T value;
    if (read_uint4_binval(curs, &value) < 0) {
        return NULL;
    }
    return PyLong_FromLongLong(value);
}


static int
read_int4_binval(data_crs *curs, PY_INT32_T *value) {
    return read_uint4_binval(curs, (PY_UINT32_T *)value);
}


static PyObject *
int4_binval(data_crs *curs) {
    PY_INT32_T value;
    if (read_int4_binval(curs, &value) < 0) {
        return NULL;
    }
    return PyLong_FromLong(value);
}

static int
read_uint8_binval(data_crs *curs, PY_UINT64_T *value)
{
    PY_UINT64_T val;
    char *data;

    data = advance_cursor_len(curs, 8);
    if (data == NULL)
        return -1;

    val = *(PY_UINT64_T *)data;
    *value = be64toh(val);
    return 0;
}


static PyObject *
int8_binval(data_crs *curs) {
    PY_UINT64_T value;
    if (read_uint8_binval(curs, &value) < 0) {
        return NULL;
    }
    return PyLong_FromLongLong((PY_INT64_T)value);
}


static PyObject *int_strval(data_crs *curs) {

    char *data, *pend;
    PyObject *value;

    if (curs->item_len < 1) {
        PyErr_SetString(PoqueError, "Invalid length for text integer value");
        return NULL;
    }
    data = advance_cursor(curs);
    if (data == NULL)
        return NULL;

    value = PyLong_FromString(data, &pend, 10);
    if (value != NULL && pend != curs->data) {
        PyErr_SetString(PoqueError, "Invalid value for text integer value");
    }
    return value;
}


static PyObject *bool_binval(data_crs *curs) {
    char *data;

    data = advance_cursor_len(curs, 1);
    if (data == NULL)
        return NULL;
    return PyBool_FromLong(data[0]);
}


static PyObject *bool_strval(data_crs *curs) {
    char *data;

    data = advance_cursor_len(curs, 1);
    if (data == NULL)
        return NULL;
    return PyBool_FromLong(data[0] == 't');
}


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

    if (hex >= 0x30 && hex <= 0x66) {
        c = hex_vals[(unsigned char)hex];
    }
    if (c == -1) {
        PyErr_SetString(PoqueError, "Invalid hexadecimal character");
    }
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

    char *src = data;

    while (src < end) {
        /* fill destination buffer */
        char v;
        if (*src != '\\')
            /* regular byte */
            v = *src++;
        else if ((src[1] >= '0' && src[1] <= '3') &&
                 (src[2] >= '0' && src[2] <= '7') &&
                 (src[3] >= '0' && src[3] <= '7')) {
            /* escaped octal value */
            v = (src[1] - '0') << 6 | (src[2] - '0') << 3 | (src[3] - '0');
            src += 4;
        }
        else if (src[1] == '\\') {
            /* escaped backslash */
            v = '\\';
            src += 2;
        }
        else {
            /* Should be impossible, but compiler complains */
            PyErr_SetString(PoqueError, "Invalid escaped bytea value");
            return -1;
        }
        *dest++ = v;
    }
    return 0;
}


static PyObject *
bytea_strval(data_crs *curs)
{
    /* converts the textual representation of a bytea value to a Python
     * bytes value
     */
    int bytea_len;
    char *data, *dest, *end;
    PyObject *bytea = NULL;
    int (*fill_func)(char *, char *, char *);

    data = advance_cursor(curs);
    if (data == NULL)
        return NULL;
    end = curs->data;

    /* determine number of bytes and parse function based on format */
    if (strncmp(data, "\\x", 2) == 0) {
        /* hexadecimal format */
        bytea_len = (curs->item_len - 2) / 2;
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
            else if ((src[1] >= '0' && src[1] <= '3') &&
                     (src[2] >= '0' && src[2] <= '7') &&
                     (src[3] >= '0' && src[3] <= '7'))
                /* octal value */
                src += 4;
            else if (src[1] == '\\')
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

static PyObject *bytea_binval(data_crs* curs) {
    char *data;

    data = advance_cursor(curs);
    if (data == NULL)
        return NULL;
    return PyBytes_FromStringAndSize(data, curs->item_len);
}


static PyObject *char_binval(data_crs* curs) {
    char *data;

    data = advance_cursor_len(curs, 1);
    if (data == NULL)
        return NULL;
    return PyBytes_FromStringAndSize(data, 1);
}


static PyObject *
text_val(data_crs* curs) {
    char *data;

    data = advance_cursor(curs);
    if (data == NULL)
        return NULL;
    return PyUnicode_FromStringAndSize(data, curs->item_len);
}

static pq_read
get_read_func(Oid oid, int format);


static PyObject *
read_value(char *data, int len, pq_read read_func) {
    data_crs curs;
    PyObject *val;

    curs.data = data;
    curs.len = len;
    curs.item_len = len;
    val = read_func(&curs);
    if (val == NULL)
        return NULL;
    if (curs.len != 0) {
        /* read function has not consumed all the data */
        PyErr_SetString(PoqueError, "Invalid data format");
        return NULL;
    }
    return val;
}


static PyObject *
get_arr_value(data_crs *curs, PY_INT32_T *arraydims, pq_read read_func) {
    /* Get multidimensional array as a nested list of item values
     * curs: The data cursor
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
        if (read_int4_binval(curs, &item_len) < 0) {
            return NULL;
        }
        /* -1 indicates NULL value */
        if (item_len == -1)
            Py_RETURN_NONE;

        /* advance cursor past item */
        data = advance_cursor_len(curs, item_len);
        if (data == NULL)
            return NULL;

        /* read the item, this will use its own cursor */
        return read_value(data, item_len, read_func);
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
        arraydims += 1;
        for (i = 0; i < dim; i++) {
            new_item = get_arr_value(curs, arraydims, read_func);
            if (new_item == NULL) {
                Py_DECREF(lst);
                return NULL;
            }
            PyList_SET_ITEM(lst, i, new_item);
        }

        return lst;
    }
    return NULL;
}


static PyObject *
array_binval(data_crs *curs) {
    int i;
    PY_UINT32_T dims;
    PY_INT32_T flags, arraydims[7];
    Oid elem_type;
    pq_read read_func;

    /* get number of dimensions */
    if (read_int4_binval(curs, (PY_INT32_T *)&dims) < 0)
        return NULL;
    if (dims > 6) {
        PyErr_SetString(PoqueError, "Number of dimensions exceeded");
        return NULL;
    }

    /* get flags */
    if (read_int4_binval(curs, &flags) < 0)
        return NULL;
    if ((flags & 1) != flags) {
        PyErr_SetString(PoqueError, "Invalid value for array flags");
        return NULL;
    }

    /* get the element datatype */
    if (read_uint4_binval(curs, &elem_type) < 0)
        return NULL;

    /* zero dimension array, just return an empty list */
    if (dims == 0)
        return PyList_New(0);

    /* find corresponding read function */
    read_func = get_read_func(elem_type, 1);
    if (read_func == NULL)
        return NULL;

    /* fill array with dimension lengths */
    for (i = 0; i < dims; i++) {
        int arraydim;

        if (read_int4_binval(curs, &arraydim) < 0)
            return NULL;
        if (arraydim < 0) {
            PyErr_SetString(PoqueError, "Negative number of items");
            return NULL;
        }
        arraydims[i] = arraydim;

        advance_cursor_len(curs, 4); /* skip lower bounds */
    }
    arraydims[i] = -1;  /* terminate array */

    /* actually get the array */
    return get_arr_value(curs, arraydims, read_func);
}


static PyObject *
tid_binval(data_crs *curs)
{
    PY_UINT32_T block_num;
    uint16_t offset;
    PyObject *tid, *tmp;

    if (read_uint4_binval(curs, &block_num) < 0)
        return NULL;
    if (read_uint2_binval(curs, &offset) < 0)
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
tid_strval(data_crs *curs)
{
    PyObject *tid, *bl_num;
    char *dt, *bl_data;

    dt = advance_cursor_len(curs, 1);
    if (dt == NULL)
        return NULL;
    if (dt[0] != '(' || curs->data[curs->len - 1] != ')') {
        PyErr_SetString(PoqueError, "Invalid tid value");
        return NULL;
    }
    bl_data = curs->data;
    while (1) {
        dt = advance_cursor_len(curs, 1);
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
    bl_num = PyLong_FromString(bl_data, NULL, 10);
    dt[0] = ',';
    if (bl_num == NULL) {
        Py_DECREF(tid);
        return NULL;
    }
    PyTuple_SET_ITEM(tid, 0, bl_num);
    curs->data[curs->len - 1] = '\0';
    bl_num = PyLong_FromString(dt + 1, NULL, 10);
    curs->data[curs->len - 1] = ')';
    advance_cursor_len(curs, curs->len);
    if (bl_num == NULL) {
        Py_DECREF(tid);
        return NULL;
    }
    PyTuple_SET_ITEM(tid, 1, bl_num);
    return tid;
}


static PyObject *
float8_binval(data_crs *curs)
{
    union {
        PY_UINT64_T int_val;
        double dbl_val;
    } value;
    if (read_uint8_binval(curs, &value.int_val) < 0)
        return NULL;
    return PyFloat_FromDouble(value.dbl_val);
}


static PyObject *
float4_binval(data_crs *curs)
{
    union {
        PY_UINT32_T int_val;
        float dbl_val;
    } value;
    if (read_uint4_binval(curs, &value.int_val) < 0)
        return NULL;
    return PyFloat_FromDouble(value.dbl_val);
}


static PyObject *
float_strval(data_crs *curs)
{
    PyObject *float_str, *value;

    float_str = text_val(curs);
    if (float_str == NULL)
        return NULL;
    value = PyFloat_FromString(float_str);
    Py_DECREF(float_str);
    return value;
}


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


static PyObject *
json_val(data_crs *curs)
{
    char *data;
    PyObject *json_loads, *ret;

    data = advance_cursor(curs);
    if (data == NULL)
        return NULL;
    json_loads = load_python_object("json", "loads");
    if (json_loads == NULL) {
        return NULL;
    }
    ret = PyObject_CallFunction(json_loads, "s", data);
    Py_DECREF(json_loads);
    return ret;
}


static PyObject *
uuid_binval(data_crs *curs) {
    char *data;
    PyObject *uuid_cls, *uuid;

    data = advance_cursor_len(curs, 16);
    if (data == NULL)
        return NULL;
    uuid_cls = load_python_object("uuid", "UUID");
    if (uuid_cls == NULL)
        return NULL;
    uuid = PyObject_CallFunction(uuid_cls, "sy", NULL, data);
    Py_DECREF(uuid_cls);
    return uuid;
}


static PyObject *
uuid_strval(data_crs *curs)
{
    char *data;
    PyObject *uuid_cls, *uuid;

    data = advance_cursor(curs);
    if (data == NULL)
        return NULL;
    uuid_cls = load_python_object("uuid", "UUID");
    if (uuid_cls == NULL)
        return NULL;
    uuid = PyObject_CallFunction(uuid_cls, "s", data);
    Py_DECREF(uuid_cls);
    return uuid;
}


static PyObject *
point_binval(data_crs *curs) {
    PyObject *float_val, *point;

    point = PyTuple_New(2);
    if (point == NULL)
        return NULL;
    float_val = float8_binval(curs);
    if (float_val == NULL) {
        Py_DECREF(point);
        return NULL;
    }
    PyTuple_SET_ITEM(point, 0, float_val);
    float_val = float8_binval(curs);
    if (float_val == NULL) {
        Py_DECREF(point);
        return NULL;
    }
    PyTuple_SET_ITEM(point, 1, float_val);
    return point;
}


static PyObject *
lseg_binval(data_crs *curs)
{
    PyObject *point, *lseg;
    int i;

    lseg = PyTuple_New(2);
    if (lseg == NULL)
        return NULL;
    for (i = 0; i < 2; i++) {
        point = point_binval(curs);
        if (point == NULL) {
            Py_DECREF(lseg);
            return NULL;
        }
        PyTuple_SET_ITEM(lseg, i, point);
    }
    return lseg;
}


static PyObject *
polygon_binval(data_crs *curs) {
    PyObject *points;
    PY_INT32_T npoints, i;

    if (read_int4_binval(curs, &npoints) < 0) {
        return NULL;
    }
    if (npoints < 0) {
        PyErr_SetString(PoqueError, "Path length can not be less than zero");
        return NULL;
    }
    points = PyList_New(npoints);
    for (i = 0; i < npoints; i++) {
        PyObject *point;

        point = point_binval(curs);
        if (point == NULL) {
            Py_DECREF(points);
            return NULL;
        }
        PyList_SET_ITEM(points, i, point);
    }
    return points;
}


static PyObject *
path_binval(data_crs *curs)
{
    PyObject *path, *points, *closed;

    closed = bool_binval(curs);
    if (closed == NULL)
        return NULL;

    points = polygon_binval(curs);
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
abstime_binval(data_crs *curs)
{
    PyObject *abstime, *seconds, *args;
    PY_INT32_T value;

    if (read_int4_binval(curs, &value) < 0)
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
reltime_binval(data_crs *curs)
{
    PY_INT32_T value;

    if (read_int4_binval(curs, &value) < 0)
        return NULL;
    return PyDelta_FromDSU(0, value, 0);
}


static PyObject *
tinterval_binval(data_crs *curs) {
    PyObject *tinterval, *abstime;
    int i;

    advance_cursor_len(curs, 4);
    tinterval = PyTuple_New(2);
    if (tinterval == NULL)
        return NULL;
    for (i = 0; i < 2; i++) {
        abstime = abstime_binval(curs);
        if (abstime == NULL) {
            Py_DECREF(tinterval);
            return NULL;
        }
        PyTuple_SET_ITEM(tinterval, i, abstime);
    }
    return tinterval;
}


static PyObject *
circle_binval(data_crs *curs) {
    PyObject *circle, *center, *value;
    int i;

    center = PyTuple_New(2);
    if (center == NULL)
        return NULL;
    for (i = 0; i < 2; i++) {
        value = float8_binval(curs);
        if (value == NULL) {
            Py_DECREF(center);
            return NULL;
        }
        PyTuple_SET_ITEM(center, i, value);
    }
    circle = PyTuple_New(2);
    if (circle == NULL) {
        Py_DECREF(center);
        return NULL;
    }
    PyTuple_SET_ITEM(circle, 0, center);
    value = float8_binval(curs);
    if (value == NULL) {
        Py_DECREF(circle);
    }
    PyTuple_SET_ITEM(circle, 1, value);
    return circle;
}


static PyObject *
mac_binval(data_crs *curs)
{
    uint16_t first;
    PY_UINT32_T second;

    if (read_uint2_binval(curs, &first) < 0)
        return NULL;
    if (read_uint4_binval(curs, &second) < 0)
        return NULL;
    return PyLong_FromLongLong(((PY_UINT64_T)first << 32) + second);
}


static pq_read
get_read_func(Oid oid, int format) {

    static PoqueTypeDef typedefs[] = {{
            INT4OID,
            int4_binval,
            int_strval
        }, {
            INT2OID,
            int2_binval,
            int_strval
        }, {
            BOOLOID,
            bool_binval,
            bool_strval
        }, {
            BYTEAOID,
            bytea_binval,
            bytea_strval
        }, {
            CHAROID,
            char_binval,
            char_binval
        }, {
            NAMEOID,
            text_val,
            text_val
        }, {
            INT8OID,
            int8_binval,
            int_strval
        }, {
            INT2VECTOROID,
            array_binval,
            NULL
        }, {
            REGPROCOID,
            uint32_binval,
            text_val
        }, {
            TEXTOID,
            text_val,
            text_val
        }, {
            OIDOID,
            uint32_binval,
            int_strval
        }, {
            TIDOID,
            tid_binval,
            tid_strval
        }, {
            XIDOID,
            uint32_binval,
            int_strval
        }, {
            CIDOID,
            uint32_binval,
            int_strval
        }, {
            OIDVECTOROID,
            array_binval,
            NULL
        }, {
            JSONOID,
            json_val,
            json_val
        }, {
            XMLOID,
            text_val,
            text_val
        }, {
            XMLARRAYOID,
            array_binval,
            NULL
        }, {
            JSONARRAYOID,
            array_binval,
            NULL
        }, {
            POINTOID,
            point_binval,
            NULL
        }, {
            LSEGOID,
            lseg_binval,
            NULL
        }, {
            PATHOID,
            path_binval,
            NULL
        }, {
            BOXOID,
            lseg_binval,
            NULL
        }, {
            POLYGONOID,
            polygon_binval,
            NULL
        }, {
            ABSTIMEOID,
            abstime_binval,
            NULL
        }, {
            RELTIMEOID,
            reltime_binval,
            NULL
        }, {
            TINTERVALOID,
            tinterval_binval,
            NULL
        }, {
            UNKNOWNOID,
            text_val,
            NULL
        }, {
            CIRCLEOID,
            circle_binval,
            NULL
        }, {
            CIRCLEARRAYOID,
            array_binval,
            NULL
        }, {
            CASHOID,
            int8_binval,
            NULL
        }, {
            CASHARRAYOID,
            array_binval,
            NULL
        }, {
            MACADDROID,
            mac_binval,
            NULL
        }, {
            FLOAT8OID,
            float8_binval,
            float_strval,
        }, {
            FLOAT4OID,
            float4_binval,
            float_strval,
        }, {
            INT4ARRAYOID,
            array_binval,
            NULL
        }, {
            UUIDOID,
            uuid_binval,
            uuid_strval
        }, {
            0
    }};
    PoqueTypeDef *tdef = typedefs;
    pq_read read_func = NULL;

    while (tdef->oid) {
        if (tdef->oid != oid) {
            tdef++;
            continue;
        }
        if (format == 1) {
            read_func = tdef->binval;
        } else {
            read_func = tdef->strval;
        }
        break;
    }
    if (read_func == NULL) {
        if (format == 1) {
            read_func = bytea_binval;
        }
        else {
            read_func = text_val;
        }
    }
    return read_func;
}


PyObject *
Poque_value(Oid oid, int format, char *data, int len) {
    pq_read read_func;

    if (format == 0 && data[len] != '\0') {
        PyErr_SetString(PoqueError, "Invalid text format");
        return NULL;
    }

    read_func = get_read_func(oid, format);
    if (read_func == NULL)
        return NULL;

    return read_value(data, len, read_func);
}

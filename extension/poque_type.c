#include "poque.h"
#include <datetime.h>
#include <endian.h>


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
advance_cursor_len(data_crs *curs, int len)
{
    curs->item_len = len;
    return advance_cursor(curs);
}


static char *
advance_cursor_len_reset(data_crs *curs, int len)
{
    char *data;

    curs->item_len = len;
    data = advance_cursor(curs);
    curs->item_len = curs->len;
    return data;
}


static int
read_uint8_binval(data_crs *curs, unsigned char *value) {

    unsigned char *data;

    data = (unsigned char *)advance_cursor_len(curs, 1);
    if (data == NULL)
        return -1;
    *value = data[0];
    return 0;
}

static int
read_uint16_binval(data_crs *curs, uint16_t *value)
{
    unsigned char *data;

    data = (unsigned char *)advance_cursor_len(curs, 2);
    if (data == NULL)
        return -1;

    *value = ((uint16_t)data[0] << 8) + data[1];
    return 0;
}


static PyObject *
int16_binval(data_crs *curs)
{
    uint16_t value;
    if (read_uint16_binval(curs, &value) < 0)
        return NULL;
    return PyLong_FromLong((int16_t)value);
}


static int
read_uint32_binval(data_crs *curs, PY_UINT32_T *value)
{
    unsigned char *data;

    data = (unsigned char *)advance_cursor_len(curs, 4);
    if (data == NULL)
        return -1;

    *value = ((PY_UINT32_T)data[0] << 24) | ((PY_UINT32_T)data[1] << 16) |
             ((PY_UINT32_T)data[2] << 8) | data[3];

    return 0;
}


static PyObject *
uint32_binval(data_crs *curs)
{
    PY_UINT32_T value;
    if (read_uint32_binval(curs, &value) < 0)
        return NULL;
    return PyLong_FromLongLong(value);
}


static int
read_int32_binval(data_crs *curs, PY_INT32_T *value)
{
    return read_uint32_binval(curs, (PY_UINT32_T *)value);
}


static PyObject *
int32_binval(data_crs *curs)
{
    PY_INT32_T value;
    if (read_int32_binval(curs, &value) < 0)
        return NULL;
    return PyLong_FromLong(value);
}

static int
read_uint64_binval(data_crs *curs, PY_UINT64_T *value)
{
    char *data;

    data = advance_cursor_len(curs, 8);
    if (data == NULL)
        return -1;

    memcpy(value, data, 8);
    *value = be64toh(*value);
    return 0;
}


static int
read_int64_binval(data_crs *curs, PY_INT64_T *value)
{
    return read_uint64_binval(curs, (PY_UINT64_T *)value);
}


static PyObject *
int64_binval(data_crs *curs)
{
    PY_UINT64_T value;

    if (read_uint64_binval(curs, &value) < 0)
        return NULL;
    return PyLong_FromLongLong((PY_INT64_T)value);
}


static PyObject *int_strval(data_crs *curs)
{
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
        return NULL;
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

static PyObject *bytea_binval(data_crs* curs)
{
    char *data;

    data = advance_cursor(curs);
    if (data == NULL)
        return NULL;
    return PyBytes_FromStringAndSize(data, curs->item_len);
}


static PyObject *char_binval(data_crs* curs)
{
    char *data;

    data = advance_cursor_len(curs, 1);
    if (data == NULL)
        return NULL;
    return PyBytes_FromStringAndSize(data, 1);
}


static PyObject *
text_val(data_crs* curs)
{
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
    if (curs.len == 0) {
        return val;
    }
    /* read function has not consumed all the data */
    Py_DECREF(val);
    PyErr_SetString(PoqueError, "Invalid data format");
    return NULL;
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
        if (read_int32_binval(curs, &item_len) < 0) {
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
        for (i = 0; i < dim; i++) {
            new_item = get_arr_value(curs, arraydims + 1, read_func);
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
array_binval(data_crs *curs) {
    int i;
    PY_UINT32_T dims;
    PY_INT32_T flags, arraydims[7];
    Oid elem_type;
    pq_read read_func;

    /* get number of dimensions */
    if (read_uint32_binval(curs, &dims) < 0)
        return NULL;
    if (dims > 6) {
        PyErr_SetString(PoqueError, "Number of dimensions exceeded");
        return NULL;
    }

    /* get flags */
    if (read_int32_binval(curs, &flags) < 0)
        return NULL;
    if ((flags & 1) != flags) {
        PyErr_SetString(PoqueError, "Invalid value for array flags");
        return NULL;
    }

    /* get the element datatype */
    if (read_uint32_binval(curs, &elem_type) < 0)
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

        if (read_int32_binval(curs, &arraydim) < 0)
            return NULL;
        if (arraydim < 0) {
            PyErr_SetString(PoqueError, "Negative number of items");
            return NULL;
        }
        arraydims[i] = arraydim;

        advance_cursor_len(curs, 4); /* skip lower bounds */
    }
    arraydims[i] = -1;  /* terminate dimensions array */

    /* actually get the array */
    return get_arr_value(curs, arraydims, read_func);
}


static PyObject *
tid_binval(data_crs *curs)
{
    PY_UINT32_T block_num;
    uint16_t offset;
    PyObject *tid, *tmp;

    if (read_uint32_binval(curs, &block_num) < 0)
        return NULL;
    if (read_uint16_binval(curs, &offset) < 0)
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
float64_binval(data_crs *curs)
{
    union {
        PY_UINT64_T int_val;
        double dbl_val;
    } value;
    if (read_uint64_binval(curs, &value.int_val) < 0)
        return NULL;
    return PyFloat_FromDouble(value.dbl_val);
}


static PyObject *
float32_binval(data_crs *curs)
{
    union {
        PY_UINT32_T int_val;
        float dbl_val;
    } value;
    if (read_uint32_binval(curs, &value.int_val) < 0)
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
json_val(data_crs *curs)
{
    char *data;
    PyObject *json_loads, *ret;

    data = advance_cursor(curs);
    if (data == NULL)
        return NULL;
    json_loads = load_python_object("json", "loads");
    if (json_loads == NULL)
        return NULL;
    ret = PyObject_CallFunction(json_loads, "s", data);
    Py_DECREF(json_loads);
    return ret;
}


static PyObject *
jsonb_bin_val(data_crs *curs)
{
    char *version;

    version = advance_cursor_len_reset(curs, 1);
    if (version[0] != 1)
        PyErr_SetString(PoqueError, "Invalid jsonb version");
    return json_val(curs);
}

#define UUID_LEN    16


static PyObject *
uuid_binval(data_crs *curs) {
    char *data;
    PyObject *uuid_cls, *uuid;

    data = advance_cursor_len(curs, UUID_LEN);
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


static int
add_float_to_tuple(PyObject *tup, data_crs *curs, int idx)
{
    PyObject *float_val;

    float_val = float64_binval(curs);
    if (float_val == NULL) {
        Py_DECREF(tup);
        return -1;
    }
    PyTuple_SET_ITEM(tup, idx, float_val);
    return 0;
}


static PyObject *
float_tuple_binval(data_crs *curs, int len) {
    PyObject *tup;
    int i;

    tup = PyTuple_New(len);
    if (tup == NULL)
        return NULL;
    for (i=0; i < len; i++) {
        if (add_float_to_tuple(tup, curs, i) < 0)
            return NULL;
    }
    return tup;
}

static PyObject *
point_binval(data_crs *curs) {
    return float_tuple_binval(curs, 2);
/*
    PyObject *point;

    point = PyTuple_New(2);
    if (point == NULL)
        return NULL;
    if (add_float_to_tuple(point, curs, 0) < 0)
        return NULL;
    if (add_float_to_tuple(point, curs, 1) < 0)
        return NULL;
    return point; */
}


static PyObject *
line_binval(data_crs *curs) {
    return float_tuple_binval(curs, 3);
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

    if (read_int32_binval(curs, &npoints) < 0) {
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

    if (read_int32_binval(curs, &value) < 0)
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

    if (read_int32_binval(curs, &value) < 0)
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
    PyObject *circle, *center;

    circle = PyTuple_New(2);
    if (circle == NULL) {
        return NULL;
    }
    center = point_binval(curs);
    if (center == NULL) {
        Py_DECREF(circle);
        return NULL;
    }
    PyTuple_SET_ITEM(circle, 0, center);
    if (add_float_to_tuple(circle, curs, 1) < 0) {
        Py_DECREF(circle);
        return NULL;
    }
    return circle;
}


static PyObject *
mac_binval(data_crs *curs)
{
    uint16_t first;
    PY_UINT32_T second;

    if (read_uint16_binval(curs, &first) < 0)
        return NULL;
    if (read_uint32_binval(curs, &second) < 0)
        return NULL;
    return PyLong_FromLongLong(((PY_UINT64_T)first << 32) + second);
}


#define PGSQL_AF_INET 2
#define PGSQL_AF_INET6 3

static PyObject *
inet_binval(data_crs *curs)
{
    PyObject *addr_cls, *addr_str, *address=NULL;
    char *cls_name;
    unsigned char mask, *cr, family, size, is_cidr;

    cr = (unsigned char *)advance_cursor_len(curs, 4);
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
        cr = (unsigned char *)advance_cursor_len(curs, size);
        if (cr == NULL)
            return NULL;
        addr_str = PyUnicode_FromFormat(
            "%u.%u.%u.%u/%u", cr[0], cr[1], cr[2], cr[3], mask);
        if (is_cidr)
            cls_name = "IPv4Network";
        else
            cls_name = "IPv4Interface";
    } else if (family == PGSQL_AF_INET6) {
        uint16_t parts[8];
        int i;

        if (size != 16) {
            PyErr_SetString(PoqueError, "Invalid address size");
            return NULL;
        }
        for (i = 0; i < 8; i++) {
            if (read_uint16_binval(curs, parts + i) < 0)
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
date_binval(data_crs *curs)
{
    PY_INT32_T jd;
    int year, month, day;
    char *fmt;

    if (read_int32_binval(curs, &jd) < 0)
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
#define USECS_PER_HOUR      3600000000
#define USECS_PER_MINUTE    60000000
#define USECS_PER_SEC       1000000

static int
time_vals_from_int(PY_INT64_T tm, int *hour, int *minute, int *second,
                   int *usec)
{
    *hour = tm / USECS_PER_HOUR;
    if (tm < 0 || *hour > 23) {
        PyErr_SetString(PoqueError, "Invalid time value");
        return -1;
    }
    tm -= *hour * USECS_PER_HOUR;
    *minute = tm / USECS_PER_MINUTE;
    tm -= *minute * USECS_PER_MINUTE;
    *second = tm / USECS_PER_SEC;
    *usec = tm - *second * USECS_PER_SEC;
    return 0;
}


static PyObject *
time_binval(data_crs *curs)
{
    PY_INT64_T value;
    int hour, minute, second, usec;

    if (read_int64_binval(curs, &value) < 0)
        return NULL;
    if (time_vals_from_int(value, &hour, &minute, &second, &usec) < 0)
        return NULL;
    return PyTime_FromTime(hour, minute, second, usec);
}


static PyObject *
timestamp_binval(data_crs *curs)
{
    PY_INT64_T value, time;
    PY_INT32_T date;
    int year, month, day, hour, minute, second, usec;
    char *fmt;

    if (read_int64_binval(curs, &value) < 0)
        return NULL;
    date = value / USECS_PER_DAY;
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
        return PyDateTime_FromDateAndTime(year, month, day, hour, minute,
                                          second, usec);
    return PyUnicode_FromFormat(fmt, year, month, day, hour, minute, second,
                                usec);
}


static PyObject *
interval_binval(data_crs *curs)
{
    PY_INT64_T secs, usecs;
    PY_INT32_T days, months;
    PyObject *interval, *value;

    if (read_int64_binval(curs, &usecs) < 0)
        return NULL;
    if (read_int32_binval(curs, &days) < 0)
        return NULL;
    if (read_int32_binval(curs, &months) < 0)
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
static PyObject *PyDecimal;

static PyObject *
numeric_strval(data_crs *curs) {
    /* Create a Decimal from a text value */
    PyObject *strval, *args, *ret;

    /* create text argument tuple */
    args = PyTuple_New(1);
    if (args == NULL) {
        return NULL;
    }
    strval = text_val(curs);
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


static int
numeric_set_digit(PyObject *digit_tuple, uint16_t val, int *idx) {
    PyObject *digit;

    /* create the digit */
    digit = PyLong_FromLong(val);
    if (digit == NULL) {
        return -1;
    }

    /* Add it to the tuple */
    PyTuple_SET_ITEM(digit_tuple, *idx, digit);

   /* advance the digit counter */
   *idx += 1;

   return 0;
}


static int
numeric_set_digitn(PyObject *digit_tuple, uint16_t val, int ndigits, int *idx) {

    if (*idx == ndigits) {
        /* Check for ndigits because there might be more available
         * than indicated by the precision, and we don't want those superfluoues
         * zeroes */
        return 0;
    }
    return numeric_set_digit(digit_tuple, val, idx);
}


static PyObject *
numeric_binval(data_crs *curs) {
    /* Create a Decimal from a binary value */
    uint16_t npg_digits, sign, dscale;
    int16_t weight;
    int ndigits, i, j;
    PyObject *digits, *ret=NULL, *arg, *args, *pyval;

    /* Get the field values */
    if (read_uint16_binval(curs, &npg_digits) < 0)
        return NULL;
    if (read_uint16_binval(curs, (uint16_t *)&weight) < 0)
        return NULL;
    if (read_uint16_binval(curs, &sign) < 0)
        return NULL;
    if (read_uint16_binval(curs, &dscale) < 0)
        return NULL;
    /* TODO check valid scale like postgres does */

    /* Check sign */
    if (sign == NUMERIC_NAN) {
        /* We're done it's a NaN */
        return PyObject_CallFunction(PyDecimal, "s", "NaN");
    } else if (sign == NUMERIC_NEG) {
        sign = 1;
    } else if (sign != NUMERIC_POS) {
        PyErr_SetString(PoqueError, "Invalid value for numeric sign");
        return NULL;
    }

    /* calculate number of digits of the Python Decimal */
    ndigits = dscale + (weight + 1) * 4;

    /* arguments for Decimal constructor */
    args = PyTuple_New(1);
    if (args == NULL) {
        return NULL;
    }

    /* create a constructor argument for a Decimal: tuple(sign, digits, exp) */
    arg = PyTuple_New(3);
    if (arg == NULL) {
        goto end;
    }
    PyTuple_SET_ITEM(args, 0, arg);

    /* set the sign */
    pyval = PyLong_FromLong(sign);
    if (pyval == NULL) {
        goto end;
    }
    PyTuple_SET_ITEM(arg, 0, pyval);

    /* create a tuple to hold the digits */
    digits = PyTuple_New(ndigits);
    if (digits == NULL) {
        goto end;
    }
    PyTuple_SET_ITEM(arg, 1, digits);

    /* set the exponent */
    pyval = PyLong_FromLong(-dscale);
    if (pyval == NULL) {
        goto end;
    }
    PyTuple_SET_ITEM(arg, 2, pyval);

    /* fill the digits */
    j = 0;
    for (i = 0; i < npg_digits; i++) {
        /* fill from postgres digits. A postgres digit contains 4 decimal
         * digits */
        uint16_t pg_digit;

        if (read_uint16_binval(curs, &pg_digit) < 0)
            return NULL;
        /* TODO; check for less than 10000 */

        if (numeric_set_digitn(digits, pg_digit / 1000, ndigits, &j) < 0) {
            goto end;
        }
        if (numeric_set_digitn(
                digits, (pg_digit / 100) % 10, ndigits, &j) < 0) {
            goto end;
        }
        if (numeric_set_digitn(digits, (pg_digit / 10) % 10, ndigits, &j) < 0) {
            goto end;
        }
        if (numeric_set_digitn(digits, pg_digit % 10, ndigits, &j) < 0) {
            goto end;
        }
    }

    /* add extra zeroes if indicated by display scale */
    for (i = j; i < ndigits; i++) {
        if (numeric_set_digit(digits, 0, &j) < 0) {
            goto end;
        }
    }

    /* create the Decimal now */
    ret = PyObject_CallObject(PyDecimal, args);

end:
    Py_DECREF(args);
    return ret;
}

static PyObject *
bit_strval(data_crs *curs) {
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
    while (curs->len) {
        char byte;
        PyObject *new_val;

        /* new bit, shift the return value one bit to make space */
        new_val = PyNumber_InPlaceLshift(val, one);
        if (new_val == NULL)
            goto error;
        Py_DECREF(val);
        val = new_val;

        /* Get the new bit as character */
        if (read_uint8_binval(curs, (unsigned char*)&byte) < 0)
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
bit_binval(data_crs *curs) {
    /* Reads a bitstring as a Python integer

    Format:
       * signed int: number of bits (bit_len)
       * bytes: All the bits left aligned

    */
    int bit_len, quot, rest, byte_len, i;
    PyObject *val, *eight, *new_val;

    /* first get the number of bits in the bit string */
    if (read_int32_binval(curs, &bit_len) < 0)
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
        if (read_uint8_binval(curs, &byte) < 0)
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
    Py_DECREF(eight);
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
    struct _poqueTypeEntry *next;
} PoqueTypeEntry;

static PoqueTypeEntry type_table[] = {
    {INT4OID, int32_binval, int_strval, NULL},
    {VARCHAROID, text_val, NULL, NULL},
    {INT8OID, int64_binval, int_strval, NULL},
    {INT2OID, int16_binval, int_strval, NULL},
    {BOOLOID, bool_binval, bool_strval, NULL},
    {BYTEAOID, bytea_binval, bytea_strval, NULL},
    {CHAROID, char_binval, char_binval, NULL},
    {NAMEOID, text_val, NULL, NULL},
    {INT2VECTOROID, array_binval, NULL, NULL},
    {REGPROCOID, uint32_binval, NULL, NULL },
    {TEXTOID, text_val, NULL, NULL},
    {CSTRINGOID, text_val, NULL, NULL},
    {CSTRINGARRAYOID, array_binval, NULL, NULL},
    {BPCHAROID, text_val, NULL, NULL},
    {OIDOID, uint32_binval, int_strval, NULL},
    {TIDOID, tid_binval, tid_strval, NULL},
    {XIDOID, uint32_binval, int_strval, NULL},
    {CIDOID, uint32_binval, int_strval, NULL},
    {OIDVECTOROID, array_binval, NULL, NULL},
    {JSONOID, json_val, json_val, NULL},
    {JSONBOID, jsonb_bin_val, json_val, NULL},
    {XMLOID, text_val, text_val, NULL},
    {XMLARRAYOID, array_binval, NULL, NULL},
    {JSONARRAYOID, array_binval, NULL, NULL},
    {JSONBARRAYOID, array_binval, NULL, NULL},
    {POINTOID, point_binval, NULL, NULL},
    {LINEOID, line_binval, NULL, NULL},
    {LINEARRAYOID, array_binval, NULL, NULL},
    {LSEGOID, lseg_binval, NULL, NULL},
    {PATHOID, path_binval, NULL, NULL},
    {BOXOID, lseg_binval, NULL, NULL},
    {POLYGONOID, polygon_binval, NULL, NULL},
    {ABSTIMEOID, abstime_binval, NULL, NULL},
    {RELTIMEOID, reltime_binval, NULL, NULL},
    {TINTERVALOID, tinterval_binval, NULL, NULL},
    {UNKNOWNOID, text_val, NULL, NULL},
    {CIRCLEOID, circle_binval, NULL, NULL},
    {CIRCLEARRAYOID, array_binval, NULL, NULL},
    {CASHOID, int64_binval, NULL, NULL},
    {CASHARRAYOID, array_binval, NULL, NULL},
    {MACADDROID, mac_binval, NULL, NULL},
    {INETOID, inet_binval, NULL, NULL},
    {CIDROID, inet_binval, NULL, NULL},
    {BOOLARRAYOID, array_binval, NULL, NULL},
    {BYTEAARRAYOID, array_binval, NULL, NULL},
    {CHARARRAYOID, array_binval, NULL, NULL},
    {NAMEARRAYOID, array_binval, NULL, NULL},
    {INT2ARRAYOID, array_binval, NULL, NULL},
    {INT2VECTORARRAYOID, array_binval, NULL, NULL },
    {REGPROCARRAYOID, array_binval, NULL, NULL},
    {TEXTARRAYOID, array_binval, NULL, NULL},
    {OIDARRAYOID, array_binval, NULL, NULL},
    {TIDARRAYOID, array_binval, NULL, NULL},
    {XIDARRAYOID, array_binval, NULL, NULL},
    {CIDARRAYOID, array_binval, NULL, NULL},
    {BPCHARARRAYOID, array_binval, NULL, NULL},
    {VARCHARARRAYOID, array_binval, NULL, NULL},
    {OIDVECTORARRAYOID, array_binval, NULL, NULL},
    {INT8ARRAYOID, array_binval, NULL, NULL},
    {POINTARRAYOID, array_binval, NULL, NULL},
    {LSEGARRAYOID, array_binval, NULL, NULL},
    {PATHARRAYOID, array_binval, NULL, NULL},
    {BOXARRAYOID, array_binval, NULL, NULL},
    {FLOAT4ARRAYOID, array_binval, NULL, NULL},
    {FLOAT8ARRAYOID, array_binval, NULL, NULL},
    {ABSTIMEARRAYOID, array_binval, NULL, NULL},
    {RELTIMEARRAYOID, array_binval, NULL, NULL},
    {TINTERVALARRAYOID, array_binval, NULL, NULL},
    {POLYGONARRAYOID, array_binval, NULL, NULL},
    {MACADDRARRAYOID, array_binval, NULL, NULL},
    {INETARRAYOID, array_binval, NULL, NULL},
    {CIDRARRAYOID, array_binval, NULL, NULL},
    {FLOAT8OID, float64_binval, float_strval, NULL},
    {FLOAT4OID, float32_binval, float_strval, NULL},
    {INT4ARRAYOID, array_binval, NULL, NULL },
    {UUIDOID, uuid_binval, uuid_strval, NULL},
    {DATEOID, date_binval, NULL, NULL},
    {TIMEOID, time_binval, NULL, NULL},
    {TIMESTAMPOID, timestamp_binval, NULL, NULL},
    {TIMESTAMPTZOID, timestamp_binval, NULL, NULL},
    {DATEARRAYOID, array_binval, NULL, NULL},
    {TIMESTAMPARRAYOID, array_binval, NULL, NULL},
    {TIMESTAMPTZARRAYOID, array_binval, NULL, NULL},
    {TIMEARRAYOID, array_binval, NULL, NULL},
    {INTERVALOID, interval_binval, NULL, NULL},
    {INTERVALARRAYOID, array_binval, NULL, NULL},
    {NUMERICOID, numeric_binval, numeric_strval, NULL},
    {NUMERICARRAYOID, array_binval, NULL, NULL},
    {BITOID, bit_binval, bit_strval, NULL},
    {VARBITOID, bit_binval, bit_strval, NULL},
    {InvalidOid}
};

#define TYPEMAP_SIZE 128

static PoqueTypeEntry *type_map[TYPEMAP_SIZE];


int
init_type_map(void) {
    PoqueTypeEntry *entry;
//    int j = 0;

    PyDecimal = load_python_object("decimal", "Decimal");
    if (PyDecimal == NULL)
        return -1;

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
get_read_func(Oid oid, int format) {
    size_t idx;
    PoqueTypeEntry *entry;

    idx = oid % TYPEMAP_SIZE;
    for (entry = type_map[idx]; entry; entry = entry->next) {
        if (entry->oid != oid)
            continue;
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

    if (format == 0 && data[len] != '\0') {
        PyErr_SetString(PoqueError, "Invalid text format");
        return NULL;
    }

    read_func = get_read_func(oid, format);
    if (read_func == NULL) {
        return NULL;
    }

    return read_value(data, len, read_func);
}

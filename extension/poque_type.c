#include "poque_type.h"
#include "numeric.h"
#include "text.h"
#include "uuid.h"
#include "datetime.h"
#include "network.h"
#include "geometric.h"


/* ======= param handlers ====================================================
 *
 * Parameter handlers are responsible for converting and encoding Python values
 * into the pg wire protocol value.
 *
 * This happens in the following steps.
 * * The value is examined by the handler. The handler reports the size in bytes
 *   required to encode the value.
 * * The pg type oid is retrieved from the handler
 * * The value is encoded by the handler.
 *
 * Handlers are called from two places.
 * * By the Conn_exec_params function. All steps are executed once for a value.
 * * By the Array parameter handler, which uses the handler for the array
 *   elements. The examine step will be executed for all values first. After
 *   retrieving the oid, the encode step will take place for all values.
 *
 * Methods:
 * * examine:    first opportunity for a handler to do anything. It reports the
 *               encoded size of the value
 * * total_size: Reports the total encoded size of all values. Can be NULL. Will
 *               only be called from array parameter handler. Only necessary
 *               if earlier reported size by examine has changed. (see int
 *               parameter handler)
 * * encode:     Can be NULL. Returns a pointer to the encoded value. Not used
 *               by the array parameter handler. This is meant for Python values
 *               that give access to the raw pointer value without the need to
 *               allocate memory. (see text and bytes parameter handlers)
 * * encode_at:  Encodes the value at the specified location. Returns the size.
 * * free:       Deallocates the parameter handler. Can be NULL in case of
 *               statically allocated handlers without the need for state. (for
 *               example uuid handler)
 *
 * Properties:
 * * oid: the pg type oid.
 * * array_oid: the pg type oid of the corresponding array type
 * Both are evaluated after the examine step
 *
 *
 * Handlers are registered with the register_parameter_handler. It links a
 * constructor function to a Python type.
 */

param_handler *
new_param_handler(param_handler *def_handler, size_t handler_size) {
    /* Allocator for param handlers. Helper function for parameter handler
     * constructors.
     *
     * Copies the content of an existing (static) struct into the newly
     * created one.
     *
     */
    param_handler *handler; /* handler to create */

    /* allocate mem */
    handler = PyMem_Malloc(handler_size);
    if (handler == NULL) {
        return (param_handler *)PyErr_NoMemory();
    }

    /* initialize new handler with static content */
    memcpy(handler, def_handler, handler_size);
    return handler;
}

/* param handler table record */
typedef struct _param_handler_constructor {
    PyTypeObject *typ;
    ph_new constructor;
} param_handler_constructor;

/* param handler table */
static param_handler_constructor param_handler_constructors[15];
static int num_phcons = 0;


void
register_parameter_handler(PyTypeObject *typ, ph_new constructor) {
    /* registers a parameter handler for a Python type */
    param_handler_constructor *cons;

    cons = param_handler_constructors + num_phcons++;
    cons->typ = typ;
    cons->constructor = constructor;
}


ph_new
get_param_handler_constructor(PyTypeObject *typ) {
    /* Returns the appropriate param handler for a Python type from the
     * registered handlers.
     *
     * The text parameter handler is the fallback
     */
    int i;
    param_handler_constructor *cons;

    for (i = 0; i < num_phcons; i++) {
        cons = param_handler_constructors + i;
        if (typ == cons->typ) {
            return cons->constructor;
        }
    }
    return new_object_param_handler;
}


/* compatible param type record */
typedef struct _compatible_type {
    PyTypeObject *typ1;
    PyTypeObject *typ2;
} compatible_type;

/* compatible param type table */
static compatible_type compatible_types[2];
static int comp_types = 0;


void
register_compatible_param(PyTypeObject *typ1, PyTypeObject *typ2) {
    compatible_type *ct;

    ct = compatible_types + comp_types++;
    ct->typ1 = typ1;
    ct->typ2 = typ2;
}

int
is_compatible(PyTypeObject *typ1, PyTypeObject *typ2) {
    int i;
    compatible_type *ct;

    for (i = 0; i < comp_types; i++) {
        ct = compatible_types + i;
        if (ct->typ1 == typ1 && ct->typ2 == typ2)
            return 1;
        if (ct->typ2 == typ1 && ct->typ1 == typ2)
            return 1;
    }
    return 0;
}


void
write_uint16(char **p, poque_uint16 val) {
    unsigned char *q = (unsigned char *)*p;

    *(q + 1) = (unsigned char)(val & 0xff);
    val >>= 8;
    *q = (unsigned char)val;

    *p += 2;
}


void
write_uint64(char **p, PY_UINT64_T val) {
    int i;
    unsigned char *q = (unsigned char *)*p;

    for (i = 7; i >=0; i--) {
        *(q + i) = (unsigned char)(val & 0xffL);
         val >>= 8;
    }
    *p += 8;
}

/* ====== Array parameter handler =========================================== */

/* This handler is used to convert (nested) Python lists into PostgreSQL arrays.
 *
 * It uses element parameter handlers to encode the elements of the list
 * hierarchy
 */
typedef struct _ArrayParamHandler {
    param_handler handler;      /* base handler */
    param_handler *el_handler;  /* param handler of elements */
    PyTypeObject *el_type;      /* Python type of elements */
    int has_null;               /* Are there None/NULL values */
    int item_depth;             /* depth at which items are found */
    int num_items;              /* number of non NULL items */
    unsigned int num_dims;      /* number of dimensions */
    int dims[6];                /* sizes of dimensions */
} ArrayParamHandler;


static int array_examine_list(
        ArrayParamHandler *handler, PyObject *param, int depth) {
    /* Determine dimension sizes, existence of NULL values and python type.
     *
     * A nested list is not the same as a multidimensional array. Therefore
     * some checks to make sure all lists of a certain dimension have the
     * same length.
     *
     * Also check if all non NULL values are of the same Python type and if the
     * maximum number of dimensions is not exceeded.
     */
    int curr_length, i;
    PyObject *item;
    PyTypeObject *item_type;
    Py_ssize_t list_length;

    /* size of current dimension */
    curr_length = handler->dims[depth];

    /* size of current list */
    list_length = PyList_GET_SIZE(param);

    if (curr_length == -1) {
        /* never been this deep in list hierarchy so initialize dimension */
        handler->dims[depth] = (int)list_length;
        handler->num_dims++;
    }
    else if (list_length != curr_length) {
        /* lists at the same level should all have same length */
        PyErr_SetString(PyExc_ValueError, "Invalid list length");
        return -1;
    }

    /* loop over all elements in the list */
    for (i = 0; i < list_length; i++) {
        item = PyList_GET_ITEM(param, i);

        if (PyList_CheckExact(item)) {
            /* a child list */

            if (depth == handler->item_depth) {
                /* we already found non list values at this depth. a list
                 * must not appear here */
                PyErr_SetString(PyExc_ValueError, "Invalid nesting");
                return -1;
            }

            /* postgres supports up to 6 dimensions */
            if (depth == 5) {
                PyErr_SetString(PyExc_ValueError, "Too deeply nested");
                return -1;
            }

            /* recursively call ourselves for the child list */
            if (array_examine_list(handler, item, depth + 1) < 0) {
                return -1;
            }
        }
        else {
            /* non list item */
            if (handler->item_depth == -1) {
                /* set the depth at which non list values are found */
                handler->item_depth = depth;
            }
            else if (handler->item_depth != depth) {
                /* all non list values must be found at the same depth */
                PyErr_SetString(PyExc_ValueError, "Invalid nesting");
                return -1;
            }

            if (item == Py_None) {
                handler->has_null = 1;
            }
            else {
                item_type = Py_TYPE(item);
                if (handler->el_type == NULL) {
                    /* set the Python type */
                    handler->el_type = item_type;
                }
                else {
                    /* check the Python type */
                    if (handler->el_type != item_type &&
                            !is_compatible(handler->el_type, item_type)) {
                        /* all items must be of the same type or None */
                        PyErr_SetString(PyExc_ValueError, "Can not mix types");
                        return -1;
                    }
                }
                /* count the number of not None values */
                handler->num_items++;
            }
        }
    }
    return 0;
}


static int array_examine_items(ArrayParamHandler *handler, PyObject *param) {
    /* Now we have a parameter handler to examine all the child items and
     * calculate their size
     */
    Py_ssize_t list_length, i;
    PyObject *item;
    int size = 0, item_size;

    list_length = PyList_GET_SIZE(param);
    for (i = 0; i < list_length; i++) {
        item = PyList_GET_ITEM(param, i);
        if (PyList_CheckExact(item)) {
            /* it's another list */
            item_size = array_examine_items(handler, item);
        }
        else {
            if (item == Py_None) {
                /* don't need to examine None values */
                continue;
            }
            item_size = PH_Examine(handler->el_handler, item);
        }
        if (item_size < 0) {
            return -1;
        }
        size += item_size;
    }
    return size;
}


static int
array_examine(ArrayParamHandler *handler, PyObject *param) {
    int size, total_items;
    unsigned int i;

    /* First examine the list */
    if (array_examine_list(handler, param, 0) < 0) {
        return -1;
    }

    /* Python type and number of items is known. Now we can set the element
     * parameter handler */
    handler->el_handler = get_param_handler_constructor(handler->el_type)(
                              handler->num_items);

    /* examine the elements to get total element size */
    size = array_examine_items(handler, param);
    if (size < 0) {
        return -1;
    }
    /* Size is computed above as the sum of the size of examining all elements.
     * In the case of an int parameter handler this can be wrong, if the pg type
     * has changed while examining the values. To overcome this, there is the
     * total_size method. If available call it to get correct size.
     */
    if (PH_HasTotalSize(handler->el_handler)) {
        size = PH_TotalSize(handler->el_handler);
    }

    /* Now calculate the total size, 12 for the header, 8 for each dimension,
     * 4 for each item and the size of the non NULL items calculated above */
    total_items = 1;
    for (i = 0; i < handler->num_dims; i++) {
        total_items *= handler->dims[i];
    }
    handler->handler.oid = handler->el_handler->array_oid;
    return 12 + handler->num_dims * 8 + total_items * 4 + size;
}

void
write_uint32(char **p, PY_UINT32_T val) {
    /* writes a 4 byte unsigned integer in network order at the provided
     * location
     */
    int i;
    unsigned char *q = (unsigned char *)*p;

    for (i = 3; i >=0; i--) {
        q[i] = (unsigned char)(val & 0xffL);
        val >>= 8;
    }
    *p += 4;
}


static int
array_write_values(ArrayParamHandler *handler, PyObject *param, char **loc) {
    Py_ssize_t list_length, i;
    PyObject *item;
    int item_size;

    list_length = PyList_GET_SIZE(param);
    for (i = 0; i < list_length; i++) {
        item = PyList_GET_ITEM(param, i);
        if (PyList_CheckExact(item)) {
            if (array_write_values(handler, item, loc) < 0) {
                return -1;
            }
        }
        else if (item == Py_None) {
                /* NULL value is just a -1 for length */
                write_uint32(loc, -1);
        }
        else {
            /* write the value and get the size */
            item_size = PH_EncodeValueAt(handler->el_handler, item, *loc + 4);
            if (item_size < 0) {
                return -1;
            }
            /* prefix value with size */
            write_uint32(loc, item_size);

            /* advance past value */
            *loc += item_size;
        }
    }
    return 0;
}


static int
array_encode_at(ArrayParamHandler *handler, PyObject *param, char *loc) {
    unsigned int i;

    /* write array header */
    write_uint32(&loc, handler->num_dims);
    write_uint32(&loc, handler->has_null);
    write_uint32(&loc, handler->el_handler->oid);

    /* write dimension headers */
    for(i = 0; i < handler->num_dims; i++) {
        write_uint32(&loc, handler->dims[i]);
        write_uint32(&loc, 1);
    }

    /* write values */
    i = array_write_values(handler, param, &loc);
    return i;
}


static void
array_free(ArrayParamHandler *handler) {
    /* Array parameter handler itself needs no more than to free itself.
     * The element handler might need to be freed though.
     */
    param_handler *el_handler;

    el_handler = handler->el_handler;
    if (el_handler && PH_HasFree(el_handler)) {
        PH_Free(el_handler);
    }
    PyMem_Free(handler);
}


static param_handler *
new_array_param_handler(int num_param) {
    /* array parameter handler constructor */

    static ArrayParamHandler def_handler = {{
            (ph_examine)array_examine,      /* examine */
            NULL,                           /* total_size */
            NULL,                           /* encode */
            (ph_encode_at)array_encode_at,  /* encode_at */
            (ph_free)array_free,            /* free */
            TEXTARRAYOID,                   /* oid */
            InvalidOid                      /* array oid */
        },
        NULL,                               /* el_handler */
        NULL,                               /* el_type */
        0,                                  /* has_null */
        -1,                                 /* item_depth */
        0,                                  /* num_items */
        0,                                  /* num_dims */
        {-1, -1, -1, -1, -1, -1}            /* dims */
    }; /* static initialized handler */

    return new_param_handler((param_handler *)&def_handler,
                             sizeof(ArrayParamHandler));
}


PyObject *
load_python_object(const char *module_name, const char *obj_name) {
    PyObject *module, *obj;

    module = PyImport_ImportModule(module_name);
    if (module == NULL)
        return NULL;

    obj = PyObject_GetAttrString(module, obj_name);
    Py_DECREF(module);
    return obj;
}


int
pyobj_long_attr(PyObject *mod, const char *attr, long *value) {
    PyObject *py_value;

    py_value = PyObject_GetAttrString(mod, attr);
    if (py_value == NULL) {
        return -1;
    }
    *value = PyLong_AsLong(py_value);
    Py_DECREF(py_value);
    if (*value == -1 && PyErr_Occurred()) {
        return -1;
    }
    return 0;
}


typedef struct {
    char *data;
    int len;
} data_crs;


static PyObject *
get_arr_value(data_crs *crs, PY_INT32_T *arraydims, pq_read read_func,
              Oid el_oid, PoqueResult *result)
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
        PyObject *val;

        /* get item length */
        CHECK_LENGTH_LT(crs->len, 4, "array", NULL);
        item_len = read_int32(crs->data);
        ADVANCE_DATA(crs->data, crs->len, 4);

        /* -1 indicates NULL value */
        if (item_len == -1)
            Py_RETURN_NONE;

        if (item_len < 0) {
            PyErr_SetString(PoqueError, "Invalid length");
            return NULL;
        }

        CHECK_LENGTH_LT(crs->len, item_len, "array", NULL);
        val = read_func(result, crs->data, item_len, el_oid);
        ADVANCE_DATA(crs->data, crs->len, item_len);
        return val;
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
        arraydims++;
        for (i = 0; i < dim; i++) {
            new_item = get_arr_value(
                crs, arraydims, read_func, el_oid, result);
            if (new_item == NULL) {
                Py_DECREF(lst);
                return NULL;
            }
            PyList_SET_ITEM(lst, i, new_item);
        }
        return lst;
    }
}


PyObject *
array_binval(PoqueResult *result, char *data, int len, Oid el_oid) {
    unsigned int i;
    PY_UINT32_T dims;
    PY_INT32_T flags, arraydims[7];
    Oid elem_type, sub_elem_type=InvalidOid;
    pq_read read_func;
    PyObject *val;
    data_crs crs;

    CHECK_LENGTH_LT(len, 12, "array", NULL);

    /* read header values */
    dims = read_uint32(data);
    flags = read_int32(data + 4);
    elem_type = read_uint32(data + 8);

    /* check header values */
    if (dims > 6) {
        PyErr_SetString(PoqueError, "Number of dimensions exceeded");
        return NULL;
    }
    if ((flags & 1) != flags) {
        PyErr_SetString(PoqueError, "Invalid value for array flags");
        return NULL;
    }
    if (elem_type != el_oid) {
        PyErr_SetString(PoqueError, "Unexpected element type");
        return NULL;
    }

    /* zero dimension array, just return an empty list */
    if (dims == 0) {
        CHECK_LENGTH_EQ(len, 12, "array", NULL);
        return PyList_New(0);
    }

    /* find corresponding read function */
    read_func = get_read_func(elem_type, 1, &sub_elem_type);
    if (read_func == NULL)
        return NULL;

    ADVANCE_DATA(data, len, 12);
    CHECK_LENGTH_LT(len, (int)dims * 8, "array", NULL);

    /* fill array with dimension lengths */
    for (i = 0; i < dims; i++) {
        int arraydim;

        arraydim = read_int32(data);
        if (arraydim < 0) {
            PyErr_SetString(PoqueError, "Negative number of items");
            return NULL;
        }
        arraydims[i] = arraydim;
        ADVANCE_DATA(data, len, 8); /* skip lower bounds */
    }
    arraydims[i] = -1;  /* terminate dimensions array */

    /* actually get the array */
    crs.data = data;
    crs.len = len;
    val = get_arr_value(
        &crs, arraydims, read_func, sub_elem_type, result);
    if (crs.len != 0) {
        Py_DECREF(val);
        PyErr_SetString(PoqueError, "Invalid data format");
        return NULL;
    }
    return val;
}


static PyObject *
tid_binval(PoqueResult *result, char *data, int len, Oid el_oid)
{
    PY_UINT32_T block_num;
    poque_uint16 offset;

    if (len != 6) {
        PyErr_SetString(PoqueError, "Invalid data format");
        return NULL;
    }

    block_num = read_uint32(data);
    offset = read_uint16(data + 4);

    return Py_BuildValue("IH", block_num, offset);
}


static PyObject *
tid_strval(PoqueResult *result, char *data, int len, Oid el_oid)
{
    PyObject *tid, *bl_num;
    char *pend, *tpos;

    if (len < 5) {
        PyErr_SetString(PoqueError, "Invalid data format");
        return NULL;
    }

    if (data[0] != '(' || data[len - 1] != ')') {
        PyErr_SetString(PoqueError, "Invalid tid value");
        return NULL;
    }
    tpos = strstr(data, ",");
    if (tpos == NULL) {
        PyErr_SetString(PoqueError, "Invalid data format");
        return NULL;
    }
    tpos[0] = '\0';
    data[len - 1] = '\0';

    tid = PyTuple_New(2);
    if (tid == NULL)
        return NULL;

    bl_num = PyLong_FromString(data + 1, &pend, 10);
    if (bl_num == NULL) {
        Py_DECREF(tid);
        return NULL;
    }
    if (pend != tpos) {
        PyErr_SetString(PoqueError, "Invalid data format");
        Py_DECREF(tid);
        return NULL;
    }
    PyTuple_SET_ITEM(tid, 0, bl_num);

    bl_num = PyLong_FromString(tpos + 1, &pend, 10);
    if (bl_num == NULL) {
        Py_DECREF(tid);
        return NULL;
    }
    if (pend != data + len - 1) {
        PyErr_SetString(PoqueError, "Invalid tid value");
        Py_DECREF(tid);
        return NULL;
    }
    PyTuple_SET_ITEM(tid, 1, bl_num);
    return tid;
}


static PyObject *json_loads;

static PyObject *
json_val(PoqueResult *result, char *data, int len, Oid el_oid)
{
    return PyObject_CallFunction(json_loads, "s#", data, len);
}


static PyObject *
jsonb_bin_val(PoqueResult *result, char *data, int len, Oid el_oid)
{
    if (data[0] != 1)
        PyErr_SetString(PoqueError, "Invalid jsonb version");
    return json_val(result, data + 1, len - 1, el_oid);
}




static PyObject *
inplace_op(PyObject *(*op)(PyObject *, PyObject *),
           PyObject *val, PyObject *arg)
{
    /* Helper function for in place operations. Steals a reference to val.
     * Returns a new reference
     */
    PyObject *new_val;

    new_val = op(val, arg);
    Py_DECREF(val);
    return new_val;
}


static PyObject *
bit_strval(PoqueResult *result, char *data, int len, Oid el_oid) {
    PyObject *val, *one=NULL;
    char *end;

    /* initialize return value */
    val = PyLong_FromLong(0);
    if (val == NULL) {
        return NULL;
    }

    /* initialize Python integer with value 1 */
    one = PyLong_FromLong(1);
    if (one == NULL) {
        Py_DECREF(val);
        return NULL;
    }

    /* interpret characters as bits */
    end = data + len;
    while (data < end) {
        char byte;

        /* new bit, shift the return value one bit to make space */
        val = inplace_op(PyNumber_InPlaceLshift, val, one);
        if (val == NULL) {
            goto error;
        }

        byte = *data;

        /* interpret bit */
        if (byte == '1') {
            /* add the bit to the return value */
            val = inplace_op(PyNumber_InPlaceOr, val, one);
            if (val == NULL) {
                goto error;
            }
        }
        else if (byte != '0') {
            PyErr_SetString(PoqueError, "Invalid character in bit string");
            goto error;
        }
        data++;
    }
    Py_DECREF(one);
    return val;

error:
    Py_DECREF(one);
    Py_XDECREF(val);
    return NULL;
}


static PyObject *
bit_binval(PoqueResult *result, char *data, int len, Oid el_oid) {
    /* Reads a bitstring as a Python integer

    Format:
       * signed int: number of bits (bit_len)
       * bytes: All the bits left aligned

    */
    int bit_len, quot, rest, byte_len;
    char *end;
    PyObject *val, *eight;

    /* first get the number of bits in the bit string */
    if (len < 4) {
        PyErr_SetString(PoqueError,
                        "Invalid binary bit string");
        return NULL;
    }
    bit_len = read_int32((unsigned char *)data);
    if (bit_len < 0) {
        PyErr_SetString(PoqueError,
                        "Invalid length value in binary bit string");
        return NULL;
    }

    quot = bit_len / 8;  /* number of bytes completely filled */
    rest = bit_len % 8;  /* number of bits in remaining byte */
    byte_len = quot + (rest > 0); /* total number of data bytes */
    if (len != byte_len + 4) {
        PyErr_SetString(PoqueError,
                        "Invalid binary bit string");
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

    /* add the value byte by byte, python ints have no upper limit, so this
     * works even for bitstrings longer than 64 bits */
    end = data + len;
    for (data += 4; data < end; data++) {
        unsigned char byte;
        PyObject *byte_val;

        /* new byte, first shift the return value one byte to the left, to make
         * space */
        val = inplace_op(PyNumber_InPlaceLshift, val, eight);
        if (val == NULL) {
            goto error;
        }

        /* read the new byte */
        byte = (unsigned char)*data;
        byte_val = PyLong_FromLong(byte);
        if (byte_val == NULL) {
            goto error;
        }

        /* add the new byte to the return value */
        val = inplace_op(PyNumber_InPlaceOr, val, byte_val);
        Py_DECREF(byte_val);
        if (val == NULL) {
            goto error;
        }
    }
    if (rest) {
        /* correct for the fact that the pg bitstring is left aligned */
        PyObject *shift_val;

        /* get the number of bits to shift the entire value */
        shift_val = PyLong_FromLong(8 - rest);
        if (shift_val == NULL) {
            goto error;
        }

        /* shift the value */
        val = inplace_op(PyNumber_InPlaceRshift, val, shift_val);
        Py_DECREF(shift_val);
        if (val == NULL) {
            goto error;
        }
    }
    Py_DECREF(eight);
    return val;

error:
    Py_DECREF(eight);
    Py_XDECREF(val);
    return NULL;
}


/* static definition of types with value converters */
static PoqueTypeEntry type_table[] = {
    {INT2VECTOROID, array_binval, NULL, INT2OID, NULL},
    {TIDOID, tid_binval, tid_strval, InvalidOid, NULL},
    {OIDVECTOROID, array_binval, NULL, OIDOID, NULL},
    {JSONOID, json_val, json_val, InvalidOid, NULL},
    {JSONBOID, jsonb_bin_val, json_val, InvalidOid, NULL},
    {JSONARRAYOID, array_binval, NULL, JSONOID, NULL},
    {JSONBARRAYOID, array_binval, NULL, JSONBOID, NULL},
    {INT2VECTORARRAYOID, array_binval, NULL, INT2VECTOROID, NULL},
    {TIDARRAYOID, array_binval, NULL, TIDOID, NULL},
    {OIDVECTORARRAYOID, array_binval, NULL, OIDVECTOROID, NULL},
    {BITOID, bit_binval, bit_strval, InvalidOid, NULL},
    {BITARRAYOID, array_binval, NULL, BITOID, NULL},
    {VARBITOID, bit_binval, bit_strval, InvalidOid, NULL},
    {VARBITARRAYOID, array_binval, NULL, VARBITOID, NULL},
    {InvalidOid}
};

#define TYPEMAP_SIZE 128

/* hash table of value converters */
static PoqueTypeEntry *type_map[TYPEMAP_SIZE];


static void
register_value_handler(PoqueTypeEntry *entry)
{
    size_t idx = entry->oid % TYPEMAP_SIZE;
    PoqueTypeEntry *prev = type_map[idx];
    if (prev == NULL) {
        type_map[idx] = entry;
    }
    else {
        /* append to linked list */
        while (prev->next) {
            prev = prev->next;
        }
        prev->next = entry;
    }
}


void
register_value_handler_table(PoqueTypeEntry *table) {
    PoqueTypeEntry *entry;

    entry = table;
    while (entry->oid != InvalidOid) {
        register_value_handler(entry);
        entry++;
    }
}


int
init_type_map(void) {

    if (init_numeric() < 0) {
        return -1;
    }
    if (init_text() < 0) {
        return -1;
    }
    if (init_datetime() < 0) {
        return -1;
    }
    if (init_uuid() < 0) {
        return -1;
    }
    if (init_network() < 0) {
        return -1;
    }
    if (init_geometric() < 0) {
        return -1;
    }

    register_parameter_handler(&PyList_Type, new_array_param_handler);

    json_loads = load_python_object("json", "loads");
    if (json_loads == NULL)
        return -1;

    /* initialize hash table of value converters */
    register_value_handler_table(type_table);
    return 0;
}


pq_read
get_read_func(Oid oid, int format, Oid *el_oid)
{
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
    return format ? bytea_binval : text_val;
}

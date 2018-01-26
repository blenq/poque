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
 * This happens in two steps.
 * * The value is examined by the handler. The handler reports the size in bytes
 *   required to encode the value.
 * * The value is encoded by the handler.
 *
 * Handlers are called from two places.
 * * By the Conn_exec_params function. Both steps are executed once for a value.
 * * By the Array parameter handler, which use the handler for the array
 *   elements. The examine step will be executed for all values first. Then the
 *   encode step will take place for all values.
 *
 * Methods:
 * * examine:    first opportunity for a handler to do anything. It reports the
 *               encoded size of the value
 * * total_size: Reports the total encoded size of all values. Can be NULL. Will
 *               only be called from array parameter handler. Only necessary
 *               if earlier reported size by examine has changed. (see int
 *               parameter handler)
 * * encode:     Returns a pointer to the encoded value. Can be NULL. Not used
 *               by array parameter handler. This is meant for Python values
 *               that give access to the pointer value without the need to
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
 * Handlers are registered
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
        PyErr_SetNone(PyExc_MemoryError);
        return NULL;
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
static param_handler_constructor param_handler_constructors[8];
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
    return new_text_param_handler;
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
        handler->dims[depth] = list_length;
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
                    if (handler->el_type != item_type) {
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
    /* Now we know the number of items, so we can initialize a parameter
     * handler to examine all the child items and calculate their size
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
    int size, total_items, i;

    /* First examine the list */
    if (array_examine_list(handler, param, 0) < 0) {
        return -1;
    }

    /* Python type is known. Now we can set the element parameter handler */
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
    int i;

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


PyObject *
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
    return format ? bytea_binval : text_val;
}


PyObject *
Poque_value(Oid oid, int format, char *data, int len) {
    pq_read read_func;
    Oid el_oid = InvalidOid;

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

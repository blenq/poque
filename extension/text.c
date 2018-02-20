#include "poque_type.h"


/* ======== pg bytea type =================================================== */

static char
hex_to_char(char hex) {
    char c = -1;
    static const char hex_vals[] = {
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
        0, 1, 2, 3, 4, 5, 6, 7, 8, 9, -1, -1, -1, -1, -1, -1,
        -1, 10, 11, 12, 13, 14, 15, -1, -1, -1, -1, -1, -1, -1, -1, -1,
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
        -1, 10, 11, 12, 13, 14, 15
    };

    if (hex >= 0x30 && hex <= 0x66)
        c = hex_vals[(size_t)hex];
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
bytea_strval(ValueCursor *crs)
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


PyObject *
bytea_binval(ValueCursor* crs)
{
    char *data;
    int len;

    len = crs_remaining(crs);
    data = crs_advance_end(crs);
    return Result_getview(crs->result, data, len);
}


static int
bytes_examine(param_handler *handler, PyObject *param) {
    return (int)PyBytes_GET_SIZE(param);
}

static int
bytes_encode(param_handler *handler, PyObject *param, char **loc) {
    *loc = PyBytes_AS_STRING(param);
    return 0;
}


static int
bytes_encode_at(param_handler *handler, PyObject *param, char *loc) {
    char *str;
    Py_ssize_t size;

    size = PyBytes_GET_SIZE(param);

#if SIZEOF_SIZE_T > SIZEOF_INT
    if (size > INT_MAX) {
        PyErr_SetString(PyExc_ValueError,
                        "Size of bytes value is too large");
        return -1;
    }
#endif

    str = PyBytes_AS_STRING(param);
    memcpy(loc, str, (int)size);

    return (int)size;
}


static param_handler bytes_param_handler = {
    bytes_examine,
    NULL,
    bytes_encode,
    bytes_encode_at,
    NULL,
    BYTEAOID,
    BYTEAARRAYOID
}; /* static initialized handler */


static param_handler *
new_bytes_param_handler(int num_param) {
    return &bytes_param_handler;
}


/* ======== pg text types =================================================== */

PyObject *
text_val(ValueCursor* crs)
{
    char *data;

    data = crs_advance_end(crs);
    return PyUnicode_FromStringAndSize(data, crs_len(crs));
}


typedef struct _TextParam {
    char *string;
    int size;
} TextParam;


typedef struct _TextParamHandler {
    param_handler handler;
    int num_params;
    int examine_pos;
    int encode_pos;
    TextParam params[];
} TextParamHandler;


static int
text_examine(TextParamHandler *handler, PyObject *param) {
    char *string;
    Py_ssize_t size;
    TextParam *tp;

    string = PyUnicode_AsUTF8AndSize(param, &size);
    if (string == NULL) {
        return -1;
    }
#if SIZEOF_SIZE_T > SIZEOF_INT
    if (size > INT32_MAX) {
        PyErr_SetString(PyExc_ValueError,
                        "String too long for postgresql");
        return -1;
    }
#endif
    tp = current_examine_param(handler);
    tp->size = (int)size;
    tp->string = string;
    return (int)size;
}


static int
text_encode(TextParamHandler *handler, PyObject *param, char **loc) {
    TextParam *tp;

    tp = current_encode_param(handler);
    *loc = tp->string;
    return 0;
}


static int
text_encode_at(TextParamHandler *handler, PyObject *param, char *loc) {
    int size;
    TextParam *tp;

    tp = current_encode_param(handler);
    size = (int)tp->size;
    memcpy(loc, tp->string, size);
    return size;
}


param_handler *
new_text_param_handler(int num_params) {
    static TextParamHandler def_handler = {
        {
            (ph_examine)text_examine,        /* examine */
            NULL,                           /* total_size */
            (ph_encode)text_encode,            /* encode */
            (ph_encode_at)text_encode_at,   /* encode_at */
            (ph_free)PyMem_Free,            /* free */
            TEXTOID,                        /* oid */
            TEXTARRAYOID,                    /* array_oid */
        },
        0
    }; /* static initialized handler */
    param_handler *handler;

    handler = new_param_handler(
        (param_handler *)&def_handler,
        sizeof(TextParamHandler) + num_params * sizeof(TextParam));
    if (handler == NULL) {
        return NULL;
    }
    return handler;
}


typedef struct _ObjectParam {
    char *string;
    int size;
    PyObject *ref;
} ObjectParam;


typedef struct _ObjectParamHandler {
    param_handler handler;
    int num_params;
    int examine_pos;
    int encode_pos;
    ObjectParam params[];
} ObjectParamHandler;


static int
object_examine(ObjectParamHandler *handler, PyObject *param)
{
    char *string;
    Py_ssize_t size;
    ObjectParam *tp;

	param = PyObject_Str(param);
	if (param == NULL) {
		return -1;
	}

    tp = current_examine_param(handler);
	tp->ref = param;

	string = PyUnicode_AsUTF8AndSize(param, &size);
    if (string == NULL) {
        return -1;
    }

#if SIZEOF_SIZE_T > SIZEOF_INT
    if (size > INT32_MAX) {
        PyErr_SetString(PyExc_ValueError,
                        "String too long for postgresql");
        return -1;
    }
#endif

    tp->size = (int)size;
    tp->string = string;
    return (int)size;
}


static int
object_encode(ObjectParamHandler *handler, PyObject *param, char **loc) {
    ObjectParam *tp;

    tp = current_encode_param(handler);
    *loc = tp->string;
    return 0;
}


static int
object_encode_at(ObjectParamHandler *handler, PyObject *param, char *loc) {
    int size;
    ObjectParam *tp;

    tp = current_encode_param(handler);
    size = (int)tp->size;
    memcpy(loc, tp->string, size);
    return size;
}


static void
object_handler_free(ObjectParamHandler *handler) {
    int i;
    ObjectParam *tp;

    for (i = 0; i < handler->examine_pos; i++) {
        tp = &handler->params[i];
        Py_DECREF(tp->ref);
    }
    PyMem_Free(handler);
}


param_handler *
new_object_param_handler(int num_params) {
    static ObjectParamHandler def_handler = {
        {
            (ph_examine)object_examine,        /* examine */
            NULL,                              /* total_size */
            (ph_encode)object_encode,            /* encode */
            (ph_encode_at)object_encode_at,      /* encode_at */
            (ph_free)object_handler_free,        /* free */
            TEXTOID,                           /* oid */
            TEXTARRAYOID,                      /* array_oid */
        },
        0
    }; /* static initialized handler */
    param_handler *handler;

    handler = new_param_handler(
        (param_handler *)&def_handler,
        sizeof(ObjectParamHandler) + num_params * sizeof(ObjectParam));
    if (handler == NULL) {
        return NULL;
    }
    return handler;
}


/* ======== pg char type ==================================================== */

static PyObject *
char_binval(ValueCursor* crs)
{
    char data;

    if (crs_read_char(crs, &data) < 0)
        return NULL;
    return PyBytes_FromStringAndSize(&data, 1);
}


/* ======== initialization ================================================== */

static PoqueTypeEntry text_value_handlers[] = {
    {VARCHAROID, text_val, NULL, InvalidOid, NULL},
    {TEXTOID, text_val, NULL, InvalidOid, NULL},
    {BYTEAOID, bytea_binval, bytea_strval, InvalidOid, NULL},
    {XMLOID, text_val, text_val, InvalidOid, NULL},
    {NAMEOID, text_val, NULL, InvalidOid, NULL},
    {CHAROID, char_binval, char_binval, InvalidOid, NULL},
    {CSTRINGOID, text_val, NULL, InvalidOid, NULL},
    {BPCHAROID, text_val, NULL, InvalidOid, NULL},

    {VARCHARARRAYOID, array_binval, NULL, VARCHAROID, NULL},
    {TEXTARRAYOID, array_binval, NULL, TEXTOID, NULL},
    {BYTEAARRAYOID, array_binval, NULL, BYTEAOID, NULL},
    {XMLARRAYOID, array_binval, NULL, XMLOID, NULL},
    {NAMEARRAYOID, array_binval, NULL, NAMEOID, NULL},
    {CHARARRAYOID, array_binval, NULL, CHAROID, NULL},
    {CSTRINGARRAYOID, array_binval, NULL, CSTRINGOID, NULL},
    {BPCHARARRAYOID, array_binval, NULL, BPCHAROID, NULL},

    {InvalidOid}
};


int
init_text(void)
{
    register_value_handler_table(text_value_handlers);
    register_parameter_handler(&PyUnicode_Type, new_text_param_handler);
    register_parameter_handler(&PyBytes_Type, new_bytes_param_handler);
    return 0;
};

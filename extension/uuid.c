#include "poque_type.h"

#define UUID_LEN    16


static PyTypeObject *PyUUID_Type;


PyObject *
uuid_binval(data_crs *crs)
{
    char *data;

    data = crs_advance(crs, UUID_LEN);
    if (data == NULL)
        return NULL;
    return PyObject_CallFunction(
        (PyObject *)PyUUID_Type, "sy#", NULL, data, UUID_LEN);
}


PyObject *
uuid_strval(data_crs *crs)
{
    char *data;

    data = crs_advance_end(crs);
    return PyObject_CallFunction(
        (PyObject *)PyUUID_Type, "s#", data, crs_end(crs) - data);
}


static int
uuid_examine(param_handler *handler, PyObject *param) {
    return UUID_LEN;
}

static int
uuid_encode_at(
        param_handler *handler, PyObject *param, char *loc) {
    PyObject *bytes;

    bytes = PyObject_GetAttrString(param, "bytes");
    if (bytes == NULL) {
        return -1;
    }
    memcpy(loc, PyBytes_AS_STRING(bytes), UUID_LEN);
    Py_DECREF(bytes);
    return UUID_LEN;
}


static param_handler uuid_param_handler = {
    uuid_examine,       /* examine */
    NULL,               /* encode */
    uuid_encode_at,     /* encode_at */
    NULL,               /* free */
    UUIDOID,            /* oid */
    UUIDARRAYOID        /* array_oid */
}; /* static initialized handler */


static param_handler *
new_uuid_param_handler(int num_param) {
    return &uuid_param_handler;
}


static PoqueTypeEntry uuid_value_handlers[] = {
    {UUIDOID, uuid_binval, uuid_strval, InvalidOid, NULL},
    {UUIDARRAYOID, array_binval, NULL, UUIDOID, NULL},
    {InvalidOid}
};


int
init_uuid(void) {
//    PoqueTypeEntry *entry;

    PyUUID_Type = (PyTypeObject *)load_python_object("uuid", "UUID");
    if (PyUUID_Type == NULL) {
        return -1;
    }

    /* initialize hash table of value converters */
    register_value_handler_table(uuid_value_handlers);
//    entry = uuid_value_handlers;
//    while (entry->oid != InvalidOid) {
//        register_value_handler(entry);
//        entry++;
//    }

    register_parameter_handler(PyUUID_Type, new_uuid_param_handler);
    return 0;
}

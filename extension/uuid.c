#include "poque_type.h"
#include "text.h"

#define UUID_LEN    16


static PyTypeObject *PyUUID_Type;


PyObject *
uuid_binval(PoqueResult *result, char *data, int len, PoqueTypeEntry *entry)
{
    return PyObject_CallFunction(
        (PyObject *)PyUUID_Type, "sy#", NULL, data, len);
}


PyObject *
uuid_strval(PoqueResult *result, char *data, int len, PoqueTypeEntry *entry)
{
    return PyObject_CallFunction(
        (PyObject *)PyUUID_Type, "s#", data, len);
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
    NULL,               /* total_size */
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
    {UUIDOID, InvalidOid, ',', {uuid_strval, uuid_binval}, NULL},
    {UUIDARRAYOID, UUIDOID, ',', {array_strval, array_binval}, NULL},
    {InvalidOid}
};


int
init_uuid(void) {
    PyUUID_Type = (PyTypeObject *)load_python_object("uuid", "UUID");
    if (PyUUID_Type == NULL) {
        return -1;
    }
    register_value_handler_table(uuid_value_handlers);
    register_parameter_handler(PyUUID_Type, new_uuid_param_handler);
    return 0;
}

#include "poque_type.h"
#include "text.h"


static int
add_float_to_tuple(PyObject *tup, char *data, int idx)
{
    double val;
    PyObject *float_val;

    val = _PyFloat_Unpack8((unsigned char *)data, 0);
    if (val == -1.0 && PyErr_Occurred()) {
        return -1;
    }

    float_val = PyFloat_FromDouble(val);
    if (float_val == NULL) {
        return -1;
    }
    PyTuple_SET_ITEM(tup, idx, float_val);
    return 0;
}


static PyObject *
float_tuple_binval(char *data, int len, int n) {
    PyObject *tup;
    int i;

    if (len != 8 * n) {
        PyErr_SetString(PoqueError, "Invalid geometric value");
        return NULL;
    }

    tup = PyTuple_New(n);
    if (tup == NULL)
        return NULL;
    for (i=0; i < n; i++) {
        if (add_float_to_tuple(tup, data + 8 * i, i) < 0) {
            Py_DECREF(tup);
            return NULL;
        }
    }
    return tup;
}

static PyObject *
point_binval(PoqueResult *result, char *data, int len, PoqueValueHandler *unused)
{
    return float_tuple_binval(data, len, 2);
}


static PyObject *
line_binval(PoqueResult *result, char *data, int len, PoqueValueHandler *unused)
{
    return float_tuple_binval(data, len, 3);
}


static PyObject *
lseg_binval(PoqueResult *result, char *data, int len, PoqueValueHandler *unused)
{
    PyObject *point, *lseg;
    int i;

    if (len != 32) {
        PyErr_SetString(PoqueError, "Invalid lseg value");
        return NULL;
    }

    lseg = PyTuple_New(2);
    if (lseg == NULL)
        return NULL;
    for (i = 0; i < 2; i++) {
        point = float_tuple_binval(data + 16 * i, 16, 2);
        if (point == NULL) {
            Py_DECREF(lseg);
            return NULL;
        }
        PyTuple_SET_ITEM(lseg, i, point);
    }
    return lseg;
}


static PyObject *
polygon_binval(PoqueResult *result, char *data, int len, PoqueValueHandler *unused)
{
    PyObject *points;
    PY_INT32_T npoints, i;

    if (len < 4) {
        PyErr_SetString(PoqueError, "Invalid polygon value");
        return NULL;
    }

    npoints = read_int32(data);
    if (npoints < 0) {
        PyErr_SetString(PoqueError, "Path length can not be less than zero");
        return NULL;
    }
    if (len != 4 + npoints * 16) {
        PyErr_SetString(PoqueError, "Invalid polygon value");
        return NULL;
    }

    points = PyList_New(npoints);
    if (points == NULL) {
        return NULL;
    }
    data += 4;
    for (i = 0; i < npoints; i++) {
        PyObject *point;

        point = point_binval(result, data + i * 16, 16, NULL);
        if (point == NULL) {
            Py_DECREF(points);
            return NULL;
        }
        PyList_SET_ITEM(points, i, point);
    }
    return points;
}


static PyObject *
path_binval(PoqueResult *result, char *data, int len, PoqueValueHandler *unused)
{
    PyObject *path, *points, *closed;

    if (len == 0) {
        PyErr_SetString(PoqueError, "Invalid path value");
        return NULL;
    }
    closed = PyBool_FromLong(data[0]);

    points = polygon_binval(result, data + 1, len -1, NULL);
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
circle_binval(PoqueResult *result, char *data, int len, PoqueValueHandler *unused)
{
    PyObject *circle, *center;

    if (len != 24) {
        PyErr_SetString(PoqueError, "Invalid circle value");
        return NULL;
    }

    circle = PyTuple_New(2);
    if (circle == NULL) {
        return NULL;
    }
    center = point_binval(result, data, 16, NULL);
    if (center == NULL) {
        Py_DECREF(circle);
        return NULL;
    }
    PyTuple_SET_ITEM(circle, 0, center);
    if (add_float_to_tuple(circle, data + 16, 1) < 0) {
        Py_DECREF(circle);
        return NULL;
    }
    return circle;
}


PoqueValueHandler point_val_handler = {{text_val, point_binval}, ',', NULL};
PoqueValueHandler line_val_handler = {{text_val, line_binval}, ',', NULL};
PoqueValueHandler lseg_val_handler = {{text_val, lseg_binval}, ',', NULL};
PoqueValueHandler path_val_handler = {{text_val, path_binval}, ',', NULL};
PoqueValueHandler box_val_handler = {{text_val, lseg_binval}, ';', NULL};
PoqueValueHandler polygon_val_handler = {{text_val, polygon_binval}, ',', NULL};
PoqueValueHandler circle_val_handler = {{text_val, circle_binval}, ',', NULL};

PoqueValueHandler pointarray_val_handler = {
        {array_strval, array_binval}, ',', &point_val_handler};
PoqueValueHandler linearray_val_handler = {
        {array_strval, array_binval}, ',', &line_val_handler};
PoqueValueHandler lsegarray_val_handler = {
        {array_strval, array_binval}, ',', &lseg_val_handler};
PoqueValueHandler patharray_val_handler = {
        {array_strval, array_binval}, ',', &path_val_handler};
PoqueValueHandler boxarray_val_handler = {
        {array_strval, array_binval}, ';', &box_val_handler};
PoqueValueHandler polygonarray_val_handler = {
        {array_strval, array_binval}, ',', &polygon_val_handler};
PoqueValueHandler circlearray_val_handler = {
        {array_strval, array_binval}, ',', &circle_val_handler};

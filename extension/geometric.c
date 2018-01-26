#include "poque_type.h"


static int
add_float_to_tuple(PyObject *tup, data_crs *crs, int idx)
{
    double val;
    PyObject *float_val;

    if (crs_read_double(crs, &val) < 0)
        return -1;
    float_val = PyFloat_FromDouble(val);
    if (float_val == NULL) {
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
        if (add_float_to_tuple(tup, crs, i) < 0) {
            Py_DECREF(tup);
            return NULL;
        }
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
        point = float_tuple_binval(crs, 2);
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
    char data;

    if (crs_read_char(crs, &data) < 0)
        return NULL;
    closed = PyBool_FromLong(data);

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


static PoqueTypeEntry geometric_value_handlers[] = {
    {POINTOID, point_binval, NULL, InvalidOid, NULL},
    {LINEOID, line_binval, NULL, InvalidOid, NULL},
    {LSEGOID, lseg_binval, NULL, InvalidOid, NULL},
    {PATHOID, path_binval, NULL, InvalidOid, NULL},
    {BOXOID, lseg_binval, NULL, InvalidOid, NULL},
    {POLYGONOID, polygon_binval, NULL, InvalidOid, NULL},
    {CIRCLEOID, circle_binval, NULL, InvalidOid, NULL},

    {POINTARRAYOID, array_binval, NULL, POINTOID, NULL},
    {LSEGARRAYOID, array_binval, NULL, LSEGOID, NULL},
    {PATHARRAYOID, array_binval, NULL, PATHOID, NULL},
    {BOXARRAYOID, array_binval, NULL, BOXOID, NULL},
    {POLYGONARRAYOID, array_binval, NULL, POLYGONOID, NULL},
    {LINEARRAYOID, array_binval, NULL, LINEOID, NULL},
    {CIRCLEARRAYOID, array_binval, NULL, CIRCLEOID, NULL},

    {InvalidOid}
};


int init_geometric(void) {
    register_value_handler_table(geometric_value_handlers);
    return 0;
}

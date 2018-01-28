#include "poque_type.h"
#include <datetime.h>

static long min_year;
static long max_year;

static PyObject *utc;


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


#define POSTGRES_EPOCH_JDATE    2451545
#define DATE_OFFSET             730120

#define USECS_PER_DAY       86400000000
#define USECS_PER_HOUR      Py_LL(3600000000)
#define USECS_PER_MINUTE    60000000
#define USECS_PER_SEC       1000000


/* ======= parameter helper functions ======================================= */

static int
date_pgordinal(PyObject *param, int *ordinal)
{
    PyObject *py_ordinal;

    /* get the Python ordinal */
    py_ordinal = PyObject_CallMethod(param, "toordinal", NULL);
    if (py_ordinal == NULL) {
        return -1;
    }

    /* convert to PG ordinal */
    *ordinal = (int) (PyLong_AsLong(py_ordinal) - DATE_OFFSET);
    Py_DECREF(py_ordinal);
    return 0;
}

/* ======= date parameter handler =========================================== */

static int
date_examine(param_handler *handler, PyObject *param) {
    return 4;
}

static int
date_encode_at(
        param_handler *handler, PyObject *param, char *loc) {
    /* PG uses a 32 bit integer which is just the day number, i.e. the ordinal.
     * Only difference with python ordinal is a different offset
     */
    int ordinal;

    if (date_pgordinal(param, &ordinal) < 0) {
        return -1;
    }

    /* write ordinal as parameter value */
    write_uint32(&loc, ordinal);

    /* 4 bytes further */
    return 4;
}


static param_handler date_param_handler = {
    date_examine,       /* examine */
    NULL,               /* total_size */
    NULL,               /* encode */
    date_encode_at,     /* encode_at */
    NULL,               /* free */
    DATEOID,            /* oid */
    DATEARRAYOID        /* array_oid */
}; /* static initialized handler */


static param_handler *
new_date_param_handler(int num_param) {
    /* date parameter constructor */
    return &date_param_handler;
}


/* ==== datetime parameter handler ========================================== */


static int
datetime_encode_at(
        param_handler *handler, PyObject *param, char *loc) {
    int ordinal;
    PY_INT64_T val;

    if (date_pgordinal(param, &ordinal) < 0) {
        return -1;
    }
    val = ordinal * USECS_PER_DAY;
    val += (PyDateTime_DATE_GET_HOUR(param)  * USECS_PER_HOUR +
            PyDateTime_DATE_GET_MINUTE(param) * USECS_PER_MINUTE +
            PyDateTime_DATE_GET_SECOND(param) * USECS_PER_SEC +
            PyDateTime_DATE_GET_MICROSECOND(param));
    write_uint64(&loc, val);
    return 8;
}


static int
datetimetz_encode_at(
        param_handler *handler, PyObject *param, char *loc) {
    PyObject *utc_param;
    int ret;

    utc_param = PyObject_CallMethod(param, "astimezone", "O", utc);
    if (utc_param == NULL) {
        return -1;
    }
    ret = datetime_encode_at(handler, utc_param, loc);
    Py_DECREF(utc_param);
    return ret;

}


static int
datetime_examine(param_handler *handler, PyObject *param) {
    PyObject *tz;
    int has_tz;

    /* Presence of timezone must be the same for all items in an datetime array.
     */
    tz = PyObject_GetAttrString(param, "tzinfo");
    if (tz == NULL) {
        return -1;
    }
    has_tz = tz != Py_None;
    Py_DECREF(tz);
    if (handler->oid == InvalidOid) {
        /* first time, adjust the handler appropriately */
        if (has_tz) {
            handler->oid = TIMESTAMPTZOID;
            handler->array_oid = TIMESTAMPTZARRAYOID;
            handler->encode_at = datetimetz_encode_at;
        }
        else {
            handler->oid = TIMESTAMPOID;
            handler->array_oid = TIMESTAMPARRAYOID;
        }
    }
    else if (has_tz != (handler->oid == TIMESTAMPTZOID)) {
        PyErr_SetString(PyExc_ValueError,
                        "Can not mix naive and aware datetimes");
        return -1;
    }
    return 8;
}


static param_handler *
new_datetime_param_handler(int num_param) {
    static param_handler def_handler = {
        datetime_examine,               /* examine */
        NULL,                           /* total_size */
        NULL,                           /* encode */
        datetime_encode_at,             /* encode_at */
        (ph_free)PyMem_Free,            /* free */
        InvalidOid,                     /* oid */
        TIMESTAMPARRAYOID               /* array_oid */
    };
    return new_param_handler(&def_handler, sizeof(param_handler));
}


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

static PoqueTypeEntry dt_value_handlers[] = {
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
    {ABSTIMEOID, abstime_binval, NULL, InvalidOid, NULL},
    {RELTIMEOID, reltime_binval, NULL, InvalidOid, NULL},
    {TINTERVALOID, tinterval_binval, NULL, InvalidOid, NULL},
    {ABSTIMEARRAYOID, array_binval, NULL, ABSTIMEOID, NULL},
    {RELTIMEARRAYOID, array_binval, NULL, RELTIMEOID, NULL},
    {TINTERVALARRAYOID, array_binval, NULL, TINTERVALOID, NULL},
    {InvalidOid}
};


int
init_datetime(void)
{   /* Initializes datetime API and get min and max year */

    PyObject *datetime_module;
    PyObject *tz;

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

    tz = load_python_object("datetime", "timezone");
    if (tz == NULL) {
        return -1;
    }
    utc = PyObject_GetAttrString(tz, "utc");
    Py_DECREF(tz);
    if (utc == NULL) {
        return -1;
    }

    /* initialize hash table of value converters */
    register_value_handler_table(dt_value_handlers);

    register_parameter_handler(PyDateTimeAPI->DateType, new_date_param_handler);
    register_parameter_handler(PyDateTimeAPI->DateTimeType,
                               new_datetime_param_handler);
    return 0;
}

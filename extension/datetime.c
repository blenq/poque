#include "poque_type.h"
#include <datetime.h>

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


#define POSTGRES_EPOCH_JDATE    2451545
#define DATE_OFFSET             730120


static int
date_examine(param_handler *handler, PyObject *param) {
    return 4;
}

static int
date_encode_at(
        param_handler *handler, PyObject *param, char *loc) {
    PyObject *py_ordinal;
    int ordinal;

    py_ordinal = PyObject_CallMethod(param, "toordinal", NULL);
    if (py_ordinal == NULL) {
        return -1;
    }
    ordinal = (int) PyLong_AsLong(py_ordinal) - DATE_OFFSET;
    write_uint32(&loc, ordinal);
    Py_DECREF(py_ordinal);
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
    return &date_param_handler;
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


#define USECS_PER_DAY       86400000000
#define USECS_PER_HOUR      Py_LL(3600000000) /* this might become a uint32 */
#define USECS_PER_MINUTE    60000000
#define USECS_PER_SEC       1000000

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
get_utc(void) {

    static PyObject *utc;
    PyObject *tz;

    if (utc == NULL) {
        tz = load_python_object("datetime", "timezone");
        if (tz == NULL)
            return NULL;
        utc = PyObject_GetAttrString(tz, "utc");
        Py_DECREF(tz);
    }
    return utc;
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
    PyObject *utc;

    utc = get_utc();
    if (utc == NULL)
        return NULL;
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


    /* initialize hash table of value converters */
    register_value_handler_table(dt_value_handlers);

    register_parameter_handler(PyDateTimeAPI->DateType, new_date_param_handler);
    return 0;
}

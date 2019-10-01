#include "poque_type.h"
#include "text.h"
#include <datetime.h>

static long min_year;
static long max_year;

static PyObject *utc;


#define POSTGRES_EPOCH_JDATE    2451545
#define DATE_OFFSET             730120

#define USECS_PER_DAY       Py_LL(86400000000)
#define USECS_PER_HOUR      Py_LL(3600000000)
#define USECS_PER_MINUTE    Py_LL(60000000)
#define USECS_PER_SEC       Py_LL(1000000)


/* ======= parameter helper functions ======================================= */

static int
date_pgordinal(PyObject *param, int *ordinal)
{
    PyObject *py_ordinal;
    _Py_IDENTIFIER(toordinal);

    /* get the Python ordinal */
    py_ordinal = _PyObject_CallMethodIdObjArgs(param, &PyId_toordinal, NULL);
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
    write_uint32(&loc, (PY_UINT32_T)ordinal);

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


/* ==== time parameter handler ============================================== */


static int
time_examine(param_handler *handler, PyObject *param) {
    return 8;
}


static int
time_encode_at(param_handler *handler, PyObject *param, char *loc) {
    PY_INT64_T val;

    val = (PyDateTime_TIME_GET_HOUR(param)  * USECS_PER_HOUR +
           PyDateTime_TIME_GET_MINUTE(param) * USECS_PER_MINUTE +
           PyDateTime_TIME_GET_SECOND(param) * USECS_PER_SEC +
           PyDateTime_TIME_GET_MICROSECOND(param));
    write_uint64(&loc, val);
    return 8;
}


static param_handler time_param_handler = {
    time_examine,       /* examine */
    NULL,               /* total_size */
    NULL,               /* encode */
    time_encode_at,     /* encode_at */
    NULL,               /* free */
    TIMEOID,            /* oid */
    TIMEARRAYOID        /* array_oid */
}; /* static initialized handler */


static param_handler *
new_time_param_handler(int num_param) {
    /* date parameter constructor */
    return &time_param_handler;
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
    val = (ordinal * USECS_PER_DAY +
           PyDateTime_DATE_GET_HOUR(param)  * USECS_PER_HOUR +
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
    _Py_IDENTIFIER(astimezone);

    /* convert datetime object to UTC */
    utc_param = _PyObject_CallMethodIdObjArgs(
        param, &PyId_astimezone, utc, NULL);
    if (utc_param == NULL) {
        return -1;
    }

    /* Use the UTC datetime for serialization */
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


/* ====== datetime retrieval funcs ========================================== */

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
date_fromymd(int year, int month, int day) {
    char *fmt;

    /* if outside python date range convert to a string */
    if (year < min_year || year > max_year) {
        if (year > 0) {
            fmt = "%i-%02i-%02i";
        }
        else {
            fmt = "%04i-%02i-%02i BC";
            year = -1 * (year - 1);
        }
        return PyUnicode_FromFormat(fmt, year, month, day);
    }
    return PyDate_FromDate(year, month, day);
}


static PyObject *
date_binval(PoqueResult *result, char *data, int len, PoqueValueHandler *unused)
{
    PY_INT32_T jd;
    int year, month, day;
    unsigned char *cr;

	if (len != 4) {
        PyErr_SetString(PoqueError, "Invalid int4 value");
        return NULL;
	}
	cr = (unsigned char *)data;
    jd = read_int32(cr);

    if (jd == 0x7FFFFFFF) {
        return PyUnicode_FromString("infinity");
    }
    if (jd == -0x80000000) {
        return PyUnicode_FromString("-infinity");
    }
    date_vals_from_int(jd, &year, &month, &day);

    return date_fromymd(year, month, day);
}


static PyObject *
date_strval(PoqueResult *result, char *data, int len, PoqueValueHandler *unused)
{
    int count, year, month, day, pos;

    // special values
    if (strncmp(data, "infinity", len) == 0 ||
            strncmp(data, "-infinity", len) == 0) {
        return PyUnicode_FromStringAndSize(data, len);
    }

    count = sscanf(data, "%7d-%2d-%2d%n", &year, &month, &day, &pos);
    if (count != 3) {
        printf("%s\n", PQparameterStatus(result->conn->conn, "DateStyle"));
        PyErr_SetString(PoqueError, "Invalid date value");
        return NULL;
    }

    if (pos < len) {
        if (strcmp(" BC", data + pos) != 0) {
            PyErr_SetString(PoqueError, "Invalid date value");
            return NULL;
        }
        return PyUnicode_FromStringAndSize(data, len);
    }
    return date_fromymd(year, month, day);
}


static int
time_vals_from_int(PY_INT64_T tm, int *hour, int *minute, int *second,
                   int *usec)
{
    PY_INT64_T hr;

    hr = (int)(tm / USECS_PER_HOUR);
    if (tm < 0 || hr > 24) {
        PyErr_SetString(PoqueError, "Invalid time value");
        return -1;
    }
    *hour = (int)hr % 24;
    tm -= hr * USECS_PER_HOUR;
    *minute = (int)(tm / USECS_PER_MINUTE);
    tm -= *minute * USECS_PER_MINUTE;
    *second = (int)(tm / USECS_PER_SEC);
    *usec = (int)(tm - *second * USECS_PER_SEC);
    return 0;
}


static PyObject *
time_binval(PoqueResult *result, char *data, int len, PoqueValueHandler *unused)
{
    int hour, minute, second, usec;
    PY_INT64_T value;

	if (len != 8) {
        PyErr_SetString(PoqueError, "Invalid time value");
        return NULL;
	}
    value = read_int64(data);

    if (time_vals_from_int(value, &hour, &minute, &second, &usec) < 0)
        return NULL;
    return PyDateTimeAPI->Time_FromTime(hour, minute, second, usec, Py_None,
                                       PyDateTimeAPI->TimeType);
}


static PyObject *
time_strval(PoqueResult *result, char *data, int len, PoqueValueHandler *unused)
{
    int count, pos;
    unsigned int hour, minute, second, usec=0;
    char *end;
    PyObject *tz_type, *tz, *time_val;

    count = sscanf(data, "%2u:%2u:%2u%n", &hour, &minute, &second, &pos);
    if (pos != 8 || count != 3 || hour > 24 || minute > 59 || second > 59) {
        PyErr_SetString(PoqueError, "Invalid time value");
        return NULL;
    }

    end = data + len;

    data += pos;

    if (data[0] == '.') {
        // read microseconds (max 6 digits)
        count = sscanf(data, ".%6u%n", &usec, &pos);
        if (count != 1) {
            PyErr_SetString(PoqueError, "Invalid time value");
            return NULL;
        }
        for (int i = pos; i < 7; i++) {
            // multiply by 10 for every missing digit
            usec *= 10;
        }
        data += pos;
    }

    if (data[0] == '+' || data[0] == '-') {
        // read timezone
        unsigned int tz_hour, tz_minute=0, tz_second=0;

        data++;

        PyObject *tz_offset;
        count = sscanf(
                data, "%2u:%2u:%2u%n", &tz_hour, &tz_minute, &tz_second, &pos);
        if (count == 0 || tz_hour > 24 || tz_minute > 59 || tz_second > 59) {
            PyErr_SetString(PoqueError, "Invalid time value");
            return NULL;
        }
        tz_second = tz_hour * 3600 + tz_minute * 60 + tz_second;
        if (data[0] == '+') {
            tz_second = -(tz_second);
        }
        tz_offset = PyDelta_FromDSU(0, tz_second, 0);
        if (tz_offset == NULL)
            return NULL;
        tz_type = load_python_object("datetime", "timezone");
        if (tz_type == NULL) {
            Py_DECREF(tz_offset);
            return NULL;
        }
        tz = PyObject_CallFunctionObjArgs(tz_type, tz_offset, NULL);
        Py_DECREF(tz_offset);
        Py_DECREF(tz_type);
        if (tz == NULL)
            return NULL;
        data += pos;
    }
    else {
        tz = Py_None;
        Py_INCREF(tz);
    }

    if (end != data) {
        PyErr_SetString(PoqueError, "Invalid time value");
        Py_DECREF(tz);
        return NULL;
    }
    hour = hour % 24;
    time_val = PyDateTimeAPI->Time_FromTime(hour, minute, second, usec, tz,
                                            PyDateTimeAPI->TimeType);
    Py_DECREF(tz);
    return time_val;
}


static PyObject *
timetz_binval(PoqueResult *result, char *data, int len, PoqueValueHandler *el_handler)
{
    int hour, minute, second, usec;
    PyObject *tz, *ret, *offset, *tzone;
    PY_INT64_T value;
    int tz_seconds;

	if (len != 12) {
        PyErr_SetString(PoqueError, "Invalid timetz value");
        return NULL;
	}

	value = read_int64(data);
    if (time_vals_from_int(value, &hour, &minute, &second, &usec) < 0) {
        return NULL;
    }

	tz_seconds = read_int32(data + 8);
	if (tz_seconds % 60 != 0) {
	    // Python timezone only accepts offset in whole minutes, Return string
	    // instead
	    char usec_str[8] = "";
	    int tz_hour, tz_sign;

	    if (usec) {
	        while (usec % 10 == 0) {
	            usec /= 10;
	        }
	        sprintf(usec_str, ".%d", usec);
	    }
	    tz_sign = (tz_seconds < 0) ? '+': '-';
	    tz_seconds = abs(tz_seconds);
	    tz_hour = tz_seconds / 3600;
        if (tz_hour > 23) {
            PyErr_SetString(PoqueError, "Invalid time value");
            return NULL;
        }
	    tz_seconds %= 3600;
        return PyUnicode_FromFormat(
            "%02i:%02i:%02i%s%c%02i:%02i:%02i", hour, minute, second, usec_str,
            tz_sign, tz_hour, tz_seconds / 60, tz_seconds % 60);
	}

    offset = PyDelta_FromDSU(0, -tz_seconds, 0);
    if (offset == NULL)
        return NULL;

    tz = load_python_object("datetime", "timezone");
    if (tz == NULL) {
        Py_DECREF(offset);
        return NULL;
    }
    tzone = PyObject_CallFunctionObjArgs(tz, offset, NULL);
    Py_DECREF(offset);
    Py_DECREF(tz);
    if (tzone == NULL)
        return NULL;

    ret = PyDateTimeAPI->Time_FromTime(hour, minute, second, usec, tzone,
                                       PyDateTimeAPI->TimeType);
    Py_DECREF(tzone);
    return ret;
}


static PyObject *
_timestamp_binval(char *data, int len, PyObject *tz)
{
    PY_INT64_T value, time;
    PY_INT32_T date;
    int year, month, day, hour, minute, second, usec;
    char usec_str[8], *bc_str, *tz_str;

	if (len != 8) {
        PyErr_SetString(PoqueError, "Invalid timestamp value");
        return NULL;
	}

	value = read_int64(data);

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
        bc_str = "";
    }
    else if (year < min_year) {
        year = -1 * (year - 1);  /* There is no year zero */
        bc_str = " BC";
    }
    else {
        // Timestamp falls within Python datetime range, return datetime
        return PyDateTimeAPI->DateTime_FromDateAndTime(
            year, month, day, hour, minute, second, usec, tz,
            PyDateTimeAPI->DateTimeType);
    }

    // Timestamp is outside Python datetime range. Create string similar to
    // postgres.

    // strip trailing millisecond zeroes
    while (usec && usec % 10 == 0) {
        usec = usec / 10;
    }

    if (usec)
        sprintf(usec_str, ".%i", usec);
    else
        usec_str[0] = '\0';

    if (tz == Py_None) {
        tz_str = "";
    }
    else {
        tz_str = "+00";
    }

    return PyUnicode_FromFormat(
        "%04i-%02i-%02i %02i:%02i:%02i%s%s%s", year, month, day, hour, minute,
        second, usec_str, tz_str, bc_str);
}


static PyObject *
timestamp_binval(
    PoqueResult *result, char *data, int len, PoqueValueHandler *el_handler)
{
    return _timestamp_binval(data, len, Py_None);
}


static PyObject *
timestamptz_binval(
    PoqueResult *result, char *data, int len, PoqueValueHandler *el_handler)
{
    return _timestamp_binval(data, len, utc);
}


static PyObject *
interval_binval(PoqueResult *result, char *data, int len, PoqueValueHandler *el_handler)
{
    PY_INT64_T secs, usecs;
    PY_INT32_T days, months;
    PyObject *interval, *value;

	if (len != 16) {
        PyErr_SetString(PoqueError, "Invalid interval value");
        return NULL;
	}
	usecs = read_int64(data);
	days = read_int32(data + 8);
	months = read_int32(data + 12);

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
abstime_binval(PoqueResult *result, char *data, int len, PoqueValueHandler *el_handler)
{
    PyObject *abstime, *seconds, *args;
    PY_INT32_T value;

    if (len != 4) {
        PyErr_SetString(PoqueError, "Invalid abstime value");
        return NULL;
    }
    value = read_int32(data);
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
reltime_binval(PoqueResult *result, char *data, int len, PoqueValueHandler *el_handler)
{
    if (len != 4) {
        PyErr_SetString(PoqueError, "Invalid reltime value");
        return NULL;
    }
    return PyDelta_FromDSU(0, read_int32(data), 0);
}


static PyObject *
tinterval_binval(
        PoqueResult *result, char *data, int len, PoqueValueHandler *el_handler)
{
    PyObject *tinterval, *abstime;
    int i;

    if (len != 12) {
        PyErr_SetString(PoqueError, "Invalid tinterval value");
        return NULL;
    }

    data += 4;
    tinterval = PyTuple_New(2);
    if (tinterval == NULL)
        return NULL;
    for (i = 0; i < 2; i++) {
        abstime = abstime_binval(result, data + i * 4, 4, NULL);
        if (abstime == NULL) {
            Py_DECREF(tinterval);
            return NULL;
        }
        PyTuple_SET_ITEM(tinterval, i, abstime);
    }
    return tinterval;
}

PoqueValueHandler date_val_handler = {{date_strval, date_binval}, ',', NULL};
PoqueValueHandler time_val_handler = {{time_strval, time_binval}, ',', NULL};
PoqueValueHandler timetz_val_handler = {{text_val, timetz_binval}, ',', NULL};
PoqueValueHandler timestamp_val_handler = {
        {text_val, timestamp_binval}, ',', NULL};
PoqueValueHandler timestamptz_val_handler = {
        {text_val, timestamptz_binval}, ',', NULL};
PoqueValueHandler interval_val_handler = {
        {text_val, interval_binval}, ',', NULL};
PoqueValueHandler abstime_val_handler = {
        {text_val, abstime_binval}, ',', NULL};
PoqueValueHandler reltime_val_handler = {
        {text_val, reltime_binval}, ',', NULL};
PoqueValueHandler tinterval_val_handler = {
        {text_val, tinterval_binval}, ',', NULL};

PoqueValueHandler datearray_val_handler = {
        {array_strval, array_binval}, ',', &date_val_handler};
PoqueValueHandler timearray_val_handler = {
        {array_strval, array_binval}, ',', &time_val_handler};
PoqueValueHandler timetzarray_val_handler = {
        {text_val, array_binval}, ',', &timetz_val_handler};
PoqueValueHandler timestamparray_val_handler = {
        {text_val, array_binval}, ',', &timestamp_val_handler};
PoqueValueHandler timestamptzarray_val_handler = {
        {text_val, array_binval}, ',', &timestamptz_val_handler};
PoqueValueHandler intervalarray_val_handler = {
        {text_val, array_binval}, ',', &interval_val_handler};
PoqueValueHandler abstimearray_val_handler = {
        {text_val, array_binval}, ',', &abstime_val_handler};
PoqueValueHandler reltimearray_val_handler = {
        {text_val, array_binval}, ',', &reltime_val_handler};
PoqueValueHandler tintervalarray_val_handler = {
        {text_val, array_binval}, ',', &tinterval_val_handler};


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
    if ((pyobj_long_attr(datetime_module, "MINYEAR", &min_year) != 0) ||
            (pyobj_long_attr(datetime_module, "MAXYEAR", &max_year) != 0)) {
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

    register_parameter_handler(PyDateTimeAPI->DateType, new_date_param_handler);
    register_parameter_handler(PyDateTimeAPI->DateTimeType,
                               new_datetime_param_handler);
    register_parameter_handler(PyDateTimeAPI->TimeType, new_time_param_handler);
    return 0;
}

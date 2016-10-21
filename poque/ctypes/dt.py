from ctypes import create_string_buffer, cast, c_char_p
from datetime import (
    datetime, date, time, timedelta, MAXYEAR, MINYEAR, timezone)
from struct import pack_into

from .lib import Error
from .constants import (
    ABSTIMEOID, TINTERVALOID, RELTIMEOID, DATEOID, TIMEOID, TIMESTAMPOID,
    TIMESTAMPTZOID, INTERVALOID, TIMETZOID, FORMAT_BINARY)

INVALID_ABSTIME = 0x7FFFFFFE


def read_abstime_bin(crs, length=None):
    seconds = crs.advance_single("!i", length)
    return datetime.fromtimestamp(seconds)


def read_tinterval_bin(crs, length=None):

    status, dt1, dt2 = crs.advance_struct_format("!3i", length)
    if dt1 == INVALID_ABSTIME or dt2 == INVALID_ABSTIME:
        st = 0
    else:
        st = 1
    if st != status:
        raise Error("Invalid value")
    return (datetime.fromtimestamp(dt1),
            datetime.fromtimestamp(dt2))


def read_reltime_bin(crs, length=None):
    return timedelta(seconds=crs.advance_single("!i", length))


USECS_PER_SEC = 1000000
USECS_PER_MINUTE = 60 * USECS_PER_SEC
USECS_PER_HOUR = 60 * USECS_PER_MINUTE
USECS_PER_DAY = 24 * USECS_PER_HOUR

POSTGRES_EPOCH_JDATE = 2451545


def _date_vals_from_int(jd):

    # julian day magic to retrieve day, month and year, shamelessly copied
    # from postgres server code
    julian = jd + POSTGRES_EPOCH_JDATE + 32044
    quad, extra = divmod(julian, 146097)
    extra = extra * 4 + 3
    julian += 60 + quad * 3 + extra // 146097
    quad, julian = divmod(julian, 1461)
    y = julian * 4 // 1461
    julian = ((julian + 305) % 365 if y else (julian + 306) % 366) + 123
    y += quad * 4
    year = y - 4800
    quad = julian * 2141 // 65536
    day = julian - 7834 * quad // 256
    month = (quad + 10) % 12 + 1
    return year, month, day


def _time_vals_from_int(tm):
    hour, tm = divmod(tm, USECS_PER_HOUR)
    if tm < 0 or hour > 23:
        raise Error("Invalid time value")
    minute, tm = divmod(tm, USECS_PER_MINUTE)
    second, usec = divmod(tm, USECS_PER_SEC)
    return hour, minute, second, usec

DATE_OFFSET = 730120
MIN_ORDINAL = date.min.toordinal()
MAX_ORDINAL = date.max.toordinal()


def read_date_bin(crs, length=None):
    jd = crs.advance_single("!i", length)

    ordinal = jd + DATE_OFFSET
    if ordinal >= MIN_ORDINAL and ordinal <= MAX_ORDINAL:
        return date.fromordinal(ordinal)

    year, month, day = _date_vals_from_int(jd)

    # if outside python date range convert to a string
    if ordinal > MAX_ORDINAL:
        fmt = "{0}-{1:02}-{2:02}"
    elif ordinal < MIN_ORDINAL:
        fmt = "{0:04}-{1:02}-{2:02} BC"
        year = -1 * (year - 1)
    return fmt.format(year, month, day)


def read_time_bin(crs, length=None):
    return time(*_time_vals_from_int(crs.advance_single("!q", length=length)))


def read_timetz_bin(crs, length=None):
    jd, seconds = crs.advance_struct_format("!qi", length=length)
    args = _time_vals_from_int(jd)
    tzinfo = timezone(timedelta(seconds=-seconds))
    return time(*(args + (tzinfo,)))


def read_timestamp_bin(crs, length=None):
    value = crs.advance_single("!q", length=length)
    dt, tm = divmod(value, USECS_PER_DAY)
    if tm < 0:
        tm += USECS_PER_DAY
        dt -= 1

    year, month, day = _date_vals_from_int(dt)
    time_vals = _time_vals_from_int(tm)
    if year > MAXYEAR:
        fmt = "{0}-{1:02}-{2:02} {3:02}:{4:02}:{5:02}.{6:06}"
    elif year < MINYEAR:
        year = -1 * (year - 1)  # There is no year zero
        fmt = "{0:04}-{1:02}-{2:02} {3:02}:{4:02}:{5:02}.{6:06} BC"
    else:
        return datetime(year, month, day, *time_vals)
    return fmt.format(year, month, day, *time_vals)


def read_timestamptz_bin(crs, length=None):
    return datetime.replace(
        read_timestamp_bin(crs, length), tzinfo=timezone.utc)


def read_interval_bin(crs, length=None):
    usecs, days, months = crs.advance_struct_format("!qii", length)
    value = timedelta(days, *divmod(usecs, USECS_PER_SEC))
    return months, value


def get_date_time_converters():
    return {
        ABSTIMEOID: (None, read_abstime_bin),
        TINTERVALOID: (None, read_tinterval_bin),
        RELTIMEOID: (None, read_reltime_bin),
        DATEOID: (None, read_date_bin),
        TIMEOID: (None, read_time_bin),
        TIMETZOID: (None, read_timetz_bin),
        TIMESTAMPOID: (None, read_timestamp_bin),
        TIMESTAMPTZOID: (None, read_timestamptz_bin),
        INTERVALOID: (None, read_interval_bin),
    }


def write_date_bin(val):
    ret = create_string_buffer(4)
    pack_into("!i", ret, 0, val.toordinal() - DATE_OFFSET)

    return DATEOID, cast(ret, c_char_p), 4, FORMAT_BINARY


def write_time_bin(val):
    val = (val.hour * USECS_PER_HOUR + val.minute * USECS_PER_MINUTE +
           val.second * USECS_PER_SEC + val.microsecond)

    ret = create_string_buffer(8)
    pack_into("!q", ret, 0, val)
    return TIMEOID, cast(ret, c_char_p), 8, FORMAT_BINARY


def write_datetime_bin(val):
    if val.tzinfo:
        val = val.astimezone(timezone.utc)
        oid = TIMESTAMPTZOID
    else:
        oid = TIMESTAMPOID
    val = ((val.toordinal() - DATE_OFFSET) * USECS_PER_DAY +
           val.hour * USECS_PER_HOUR + val.minute * USECS_PER_MINUTE +
           val.second * USECS_PER_SEC + val.microsecond)
    ret = create_string_buffer(8)
    pack_into("!q", ret, 0, val)
    return oid, cast(ret, c_char_p), 8, FORMAT_BINARY


def get_date_time_param_converters():
    return {
        date: write_date_bin,
        time: write_time_bin,
        datetime: write_datetime_bin,
    }

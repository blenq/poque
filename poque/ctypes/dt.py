from datetime import (
    datetime, date, time, timedelta, MAXYEAR, MINYEAR, timezone)

from .lib import Error
from .common import BaseParameterHandler
from .constants import (
    ABSTIMEOID, TINTERVALOID, RELTIMEOID, DATEOID, DATEARRAYOID, TIMEOID,
    TIMEARRAYOID, TIMESTAMPOID, TIMESTAMPTZOID, INTERVALOID, TIMETZOID)
from poque._poque import TIMESTAMPARRAYOID
from poque.ctypes.constants import TIMESTAMPTZARRAYOID

INVALID_ABSTIME = 0x7FFFFFFE


def read_abstime_bin(crs):
    seconds = crs.advance_single("!i")
    return datetime.fromtimestamp(seconds)


def read_tinterval_bin(crs):

    status, dt1, dt2 = crs.advance_struct_format("!3i")
    if dt1 == INVALID_ABSTIME or dt2 == INVALID_ABSTIME:
        st = 0
    else:
        st = 1
    if st != status:
        raise Error("Invalid value")
    return (datetime.fromtimestamp(dt1),
            datetime.fromtimestamp(dt2))


def read_reltime_bin(crs):
    return timedelta(seconds=crs.advance_single("!i"))


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

DATE_OFFSET = 730120  # Postgres date offset
MIN_ORDINAL = date.min.toordinal()
MAX_ORDINAL = date.max.toordinal()


def read_date_bin(crs):
    jd = crs.advance_single("!i")

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


def read_time_bin(crs):
    return time(*_time_vals_from_int(crs.advance_single("!q")))


def read_timetz_bin(crs):
    jd, seconds = crs.advance_struct_format("!qi")
    args = _time_vals_from_int(jd)
    tzinfo = timezone(timedelta(seconds=-seconds))
    return time(*(args + (tzinfo,)))


def read_timestamp_bin(crs):
    value = crs.advance_single("!q")
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


def read_timestamptz_bin(crs):
    return datetime.replace(
        read_timestamp_bin(crs), tzinfo=timezone.utc)


def read_interval_bin(crs):
    usecs, days, months = crs.advance_struct_format("!qii")
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


def date_ordinal(val):
    return val.toordinal() - DATE_OFFSET


class DateParameterHandler(BaseParameterHandler):

    oid = DATEOID
    array_oid = DATEARRAYOID
    fmt = "i"

    def binary_value(self, val):
        return date_ordinal(val)


def time_ordinal(val):
    return (val.hour * USECS_PER_HOUR + val.minute * USECS_PER_MINUTE +
            val.second * USECS_PER_SEC + val.microsecond)


class TimeParameterHandler(BaseParameterHandler):

    oid = TIMEOID
    array_oid = TIMEARRAYOID
    fmt = "q"

    def binary_value(self, val):
        return time_ordinal(val)


class DateTimeParameterHandler(BaseParameterHandler):

    oid = TIMESTAMPOID
    array_oid = TIMESTAMPARRAYOID
    fmt = "q"
    has_tz = None

    def check_value(self, val):
        has_tz = val.tzinfo is not None
        if self.has_tz is None:
            self.has_tz = has_tz
            if has_tz:
                self.oid = TIMESTAMPTZOID
                self.array_oid = TIMESTAMPTZARRAYOID
        elif self.has_tz != has_tz:
            raise ValueError("Can not mix naive and aware datetimes")
        super(DateTimeParameterHandler, self).check_value(val)

    def binary_value(self, val):
        if val.tzinfo:
            val = val.astimezone(timezone.utc)
        return date_ordinal(val) * USECS_PER_DAY + time_ordinal(val)

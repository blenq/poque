from datetime import (
    datetime, date, time, timedelta, timezone, MAXYEAR, MINYEAR)

from .lib import Error
from .common import BaseParameterHandler
from .constants import *  # noqa
from _datetime import MINYEAR

INVALID_ABSTIME = 0x7FFFFFFE


def read_abstime_bin(crs):
    seconds = crs.advance_single("i")
    return datetime.fromtimestamp(seconds)


def read_tinterval_bin(crs):

    status, dt1, dt2 = crs.advance_struct_format("3i")
    if status != (dt1 != INVALID_ABSTIME and dt2 != INVALID_ABSTIME):
        raise Error("Invalid value")
    return (datetime.fromtimestamp(dt1),
            datetime.fromtimestamp(dt2))


def read_reltime_bin(crs):
    return timedelta(seconds=crs.advance_single("i"))


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
    if tm < 0 or hour > 24:
        raise Error("Invalid time value")
    hour = hour % 24
    minute, tm = divmod(tm, USECS_PER_MINUTE)
    second, usec = divmod(tm, USECS_PER_SEC)
    return hour, minute, second, usec


DATE_OFFSET = 730120  # Postgres date offset
MIN_ORDINAL = date.min.toordinal()
MAX_ORDINAL = date.max.toordinal()


def date_from_pgordinal(pgordinal):
    """ returns Python date if postgres date is within Python date range """

    ordinal = pgordinal + DATE_OFFSET
    if ordinal >= MIN_ORDINAL and ordinal <= MAX_ORDINAL:
        return date.fromordinal(ordinal)
    return None


def read_date_bin(crs):
    jd = crs.advance_single("i")

    if jd == 0x7FFFFFFF:
        return "infinity"
    if jd == -0x80000000:
        return "-infinity"

    dt = date_from_pgordinal(jd)
    if dt is not None:
        return dt

    year, month, day = _date_vals_from_int(jd)

    # if outside python date range convert to a string
    if year > MAXYEAR:
        fmt = "{0}-{1:02}-{2:02}"
    elif year < MINYEAR:
        fmt = "{0:04}-{1:02}-{2:02} BC"
        year = -1 * (year - 1)
    return fmt.format(year, month, day)


def read_time_bin(crs):
    return time(*_time_vals_from_int(crs.advance_single("q")))


def read_timetz_bin(crs):
    jd, tz_seconds = crs.advance_struct_format("qi")
    hour, minute, second, usec = _time_vals_from_int(jd)
    if tz_seconds % 60 != 0:
        tz_sign = "+" if tz_seconds < 0 else "-"
        tz_seconds = abs(tz_seconds)
        if usec:
            while usec % 10 == 0:
                usec = usec // 10
            usec = ".{}".format(usec)
        else:
            usec = ""
        tz_hour, tz_seconds = divmod(tz_seconds, 3600)
        tz_minute, tz_seconds = divmod(tz_seconds, 60)
        return "{:02}:{:02}:{:02}{}{}{:02}:{:02}:{:02}".format(
            hour, minute, second, usec, tz_sign, tz_hour, tz_minute, tz_seconds
        )

    tzinfo = timezone(timedelta(seconds=-tz_seconds))
    return time(hour, minute, second, usec, tzinfo=tzinfo)


def read_timestamp_bin(crs, display_tz=False):
    value = crs.advance_single("q")

    # special values
    if value == 0x7FFFFFFFFFFFFFFF:
        return 'infinity'
    if value == -0x8000000000000000:
        return '-infinity'

    # calculate timestamp components
    jd, tm = divmod(value, USECS_PER_DAY)
    hour, minute, sec, usec = _time_vals_from_int(tm)

    # calculate datetime within Python range
    dt = date_from_pgordinal(jd)
    if dt is not None:
        return datetime.combine(dt, time(hour, minute, sec, usec))

    # Timestamp is outside Python datetime range. Create string similar to
    # postgres.

    # calculate date components
    year, month, day = _date_vals_from_int(jd)

    if year < MINYEAR:
        # display value of negative year including correction for non
        # existing year 0
        year = -1 * year + 1
        bc_suffix = " BC"
    else:
        bc_suffix = ""

    # strip trailing millisecond zeroes
    while usec and usec % 10 == 0:
        usec = usec // 10

    if usec:
        usec = ".{}".format(usec)
    else:
        usec = ""
    if display_tz:
        tz = '+00'
    else:
        tz = ''

    return "{0:04}-{1:02}-{2:02} {3:02}:{4:02}:{5:02}{6}{7}{8}".format(
        year, month, day, hour, minute, sec, usec, tz, bc_suffix)


def read_timestamptz_bin(crs):
    dt = read_timestamp_bin(crs, True)
    if isinstance(dt, datetime):
        return datetime.replace(dt, tzinfo=timezone.utc)
    return dt


def read_interval_bin(crs):
    usecs, days, months = crs.advance_struct_format("qii")
    value = timedelta(days, *divmod(usecs, USECS_PER_SEC))
    return months, value


def get_date_time_converters():
    return [
        (ABSTIMEOID, ABSTIMEARRAYOID, None, read_abstime_bin),
        (TINTERVALOID, TINTERVALARRAYOID, None, read_tinterval_bin),
        (RELTIMEOID, RELTIMEARRAYOID, None, read_reltime_bin),
        (DATEOID, DATEARRAYOID, None, read_date_bin),
        (TIMEOID, TIMEARRAYOID, None, read_time_bin),
        (TIMETZOID, TIMETZARRAYOID, None, read_timetz_bin),
        (TIMESTAMPOID, TIMESTAMPARRAYOID, None, read_timestamp_bin),
        (TIMESTAMPTZOID, TIMESTAMPTZARRAYOID, None, read_timestamptz_bin),
        (INTERVALOID, INTERVALARRAYOID, None, read_interval_bin),
    ]


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

    def examine(self, val):
        has_tz = val.tzinfo is not None
        if self.has_tz is None:
            self.has_tz = has_tz
            if has_tz:
                self.oid = TIMESTAMPTZOID
                self.array_oid = TIMESTAMPTZARRAYOID
                self.binary_value = self.binary_value_tz
        elif self.has_tz != has_tz:
            raise ValueError("Can not mix naive and aware datetimes")
        return super(DateTimeParameterHandler, self).examine(val)

    def binary_value(self, val):
        return date_ordinal(val) * USECS_PER_DAY + time_ordinal(val)

    def binary_value_tz(self, val):
        val = val.astimezone(timezone.utc)
        return date_ordinal(val) * USECS_PER_DAY + time_ordinal(val)

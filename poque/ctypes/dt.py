from datetime import (
    datetime, date, time, timedelta, timezone, MAXYEAR, MINYEAR)

from .lib import Error
from .common import BaseParameterHandler, get_array_bin_reader
from . import constants

INVALID_ABSTIME = 0x7FFFFFFE


def read_abstime_bin(crs):
    seconds = crs.advance_single("i")
    return datetime.fromtimestamp(seconds)


def read_tinterval_bin(crs):

    status, dt1, dt2 = crs.advance_struct_format("3i")
    if dt1 == INVALID_ABSTIME or dt2 == INVALID_ABSTIME:
        st = 0
    else:
        st = 1
    if st != status:
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
    if tm < 0 or hour > 23:
        raise Error("Invalid time value")
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
    jd, seconds = crs.advance_struct_format("qi")
    args = _time_vals_from_int(jd)
    tzinfo = timezone(timedelta(seconds=-seconds))
    return time(*(args + (tzinfo,)))


def read_timestamp_bin(crs):
    value = crs.advance_single("q")
    if value == 0x7FFFFFFFFFFFFFFF:
        return 'infinity'
    if value == -0x8000000000000000:
        return '-infinity'
    jd, tm = divmod(value, USECS_PER_DAY)
    time_vals = _time_vals_from_int(tm)

    dt = date_from_pgordinal(jd)
    if dt is not None:
        return datetime.combine(dt, time(*time_vals))

    year, month, day = _date_vals_from_int(jd)

    if year > MAXYEAR:
        fmt = "{0}-{1:02}-{2:02} {3:02}:{4:02}:{5:02}.{6:06}"
    elif year < MINYEAR:
        year = -1 * (year - 1)  # There is no year zero
        fmt = "{0:04}-{1:02}-{2:02} {3:02}:{4:02}:{5:02}.{6:06} BC"
    return fmt.format(year, month, day, *time_vals)


def read_timestamptz_bin(crs):
    return datetime.replace(
        read_timestamp_bin(crs), tzinfo=timezone.utc)


def read_interval_bin(crs):
    usecs, days, months = crs.advance_struct_format("qii")
    value = timedelta(days, *divmod(usecs, USECS_PER_SEC))
    return months, value


def get_date_time_converters():
    return {
        constants.ABSTIMEOID: (None, read_abstime_bin),
        constants.TINTERVALOID: (None, read_tinterval_bin),
        constants.RELTIMEOID: (None, read_reltime_bin),
        constants.DATEOID: (None, read_date_bin),
        constants.TIMEOID: (None, read_time_bin),
        constants.TIMETZOID: (None, read_timetz_bin),
        constants.TIMESTAMPOID: (None, read_timestamp_bin),
        constants.TIMESTAMPTZOID: (None, read_timestamptz_bin),
        constants.INTERVALOID: (None, read_interval_bin),
        constants.ABSTIMEARRAYOID: (
            None, get_array_bin_reader(constants.ABSTIMEOID)),
        constants.TINTERVALARRAYOID: (
            None, get_array_bin_reader(constants.TINTERVALOID)),
        constants.RELTIMEARRAYOID: (
            None, get_array_bin_reader(constants.RELTIMEOID)),
        constants.DATEARRAYOID: (
            None, get_array_bin_reader(constants.DATEOID)),
        constants.TIMEARRAYOID: (
            None, get_array_bin_reader(constants.TIMEOID)),
        constants.TIMESTAMPARRAYOID: (
            None, get_array_bin_reader(constants.TIMESTAMPOID)),
        constants.TIMESTAMPTZARRAYOID: (
            None, get_array_bin_reader(constants.TIMESTAMPTZOID)),
        constants.INTERVALARRAYOID: (
            None, get_array_bin_reader(constants.INTERVALOID)),
    }


def date_ordinal(val):
    return val.toordinal() - DATE_OFFSET


class DateParameterHandler(BaseParameterHandler):

    oid = constants.DATEOID
    array_oid = constants.DATEARRAYOID
    fmt = "i"

    def binary_value(self, val):
        return date_ordinal(val)


def time_ordinal(val):
    return (val.hour * USECS_PER_HOUR + val.minute * USECS_PER_MINUTE +
            val.second * USECS_PER_SEC + val.microsecond)


class TimeParameterHandler(BaseParameterHandler):

    oid = constants.TIMEOID
    array_oid = constants.TIMEARRAYOID
    fmt = "q"

    def binary_value(self, val):
        return time_ordinal(val)


class DateTimeParameterHandler(BaseParameterHandler):

    oid = constants.TIMESTAMPOID
    array_oid = constants.TIMESTAMPARRAYOID
    fmt = "q"
    has_tz = None

    def examine(self, val):
        has_tz = val.tzinfo is not None
        if self.has_tz is None:
            self.has_tz = has_tz
            if has_tz:
                self.oid = constants.TIMESTAMPTZOID
                self.array_oid = constants.TIMESTAMPTZARRAYOID
                self.binary_value = self.binary_value_tz
        elif self.has_tz != has_tz:
            raise ValueError("Can not mix naive and aware datetimes")
        super(DateTimeParameterHandler, self).examine(val)

    def binary_value(self, val):
        return date_ordinal(val) * USECS_PER_DAY + time_ordinal(val)

    def binary_value_tz(self, val):
        val = val.astimezone(timezone.utc)
        return date_ordinal(val) * USECS_PER_DAY + time_ordinal(val)

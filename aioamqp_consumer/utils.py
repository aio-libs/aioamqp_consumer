import calendar
import datetime
import time

utc_tz = datetime.timezone.utc


def utc_now():
    return datetime.datetime.now(utc_tz)


def datetime_to_timestamp(value):
    if not isinstance(value, datetime.datetime):
        raise ValueError('Need datetime object')

    if value.tzinfo is None:
        return int(time.mktime(value.timetuple()))

    if value.tzinfo != utc_tz:
        value = value.astimezone(utc_tz)

    return calendar.timegm(value.timetuple())


def timestamp_to_datetime(value, tz=utc_tz):
    if not isinstance(value, int):
        try:
            value = int(value)
        except (TypeError, ValueError):
            raise ValueError('Can not convert to int')

    try:
        value = datetime.datetime.fromtimestamp(value, tz=tz)
    except OSError as exc:
        raise ValueError from exc

    if value.tzinfo != utc_tz:
        value = value.astimezone(utc_tz)

    return value

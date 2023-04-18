import datetime


def timestamp_to_YYYYMMDD_HHMMSS(epoch):
    return datetime.datetime.utcfromtimestamp(int(epoch)).strftime("%Y-%m-%d %H:%M:%S")


MONTH_STR_TO_INT = {
    "Jan": "01",
    "Feb": "02",
    "Mar": "03",
    "Apr": "04",
    "May": "05",
    "Jun": "06",
    "Jul": "07",
    "Aug": "08",
    "Sep": "09",
    "Oct": "10",
    "Nov": "11",
    "Dec": "12",
}


def twitter_date_to_YYYYMMDD_HHMMSS(date_time):
    """
    convert from 'Tue Apr 04 17:08:57 +0000 2023' to '2023-04-04 17:08:57'
    """
    d = date_time.split()
    year = d[5]
    month = MONTH_STR_TO_INT[d[1]]
    date = d[2]
    time = d[3]
    return f"{year}-{month}-{date} {time}"


def facebook_date_to_YYYYMMDD_HHMMSS(date_time):
    """
    convert from '2023-04-04T17:09:02+0000' to '2023-04-04 17:09:02'
    """
    d = date_time.split("T")
    date = d[0]
    time = d[1][:-5]
    return f"{date} {time}"


def youtube_date_to_YYYYMMDD_HHMMSS(date_time):
    """
    convert from '2023-04-04T17:09:21Z' to '2023-04-04 17:09:21'
    """
    d = date_time.split("T")
    date = d[0]
    time = d[1][:-1]
    return f"{date} {time}"


def round_time_five_minute(date_time, round=5 * 60):
    """
    convert from '2023-04-04 17:09:21' to '2023-04-04 17:05:00'
    """
    dt = datetime.datetime.strptime(date_time, "%Y-%m-%d %H:%M:%S")
    seconds = (dt.replace(tzinfo=None) - dt.min).seconds
    rounding = ((seconds) // round) * round
    rounded_dt = dt + datetime.timedelta(0, rounding - seconds, -dt.microsecond)
    return rounded_dt.strftime("%Y-%m-%d %H:%M:%S")

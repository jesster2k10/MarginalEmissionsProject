import datetime

def monthrange(
        start_date: datetime.datetime | datetime.date,
        end_date: datetime.datetime | datetime.date):
    """Creates a loop for a date range by the month"""
    current_month = start_date.month
    current_year = start_date.year

    while datetime.datetime(current_year, current_month, 1) < end_date:
        yield datetime.datetime(current_year, current_month, 1)
        if current_month == 12:
            current_month = 1
            current_year += 1
        else:
            current_month += 1


def next_month(date: datetime.datetime) -> datetime.datetime:
    return datetime.datetime(date.year + int(date.month / 12), ((date.month % 12) + 1), 1)

def format_datettime(date: datetime.datetime) -> str:
    return date.strftime('%d-%m-%Y+%H')

def format_date(date: datetime.date) -> str:
    return date.strftime('%d-%m-%Y')

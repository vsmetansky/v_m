from datetime import timedelta, date


def iso_year_start(iso_year):
    """The gregorian calendar date of the first day of the given ISO year"""
    fourth_jan = date(iso_year, 1, 4)
    delta = timedelta(fourth_jan.isoweekday() - 1)
    return fourth_jan - delta


def iso_to_gregorian(iso_year, iso_week, iso_day):
    """Gregorian calendar date for the given ISO year, week and day"""
    year_start = iso_year_start(iso_year)
    return year_start + timedelta(days=iso_day - 1, weeks=iso_week - 1)


def year_week_to_date(x):
    year, week = int(str(x)[:4]), int(str(x)[-2:])
    dt = iso_to_gregorian(year, week, 1)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + 'Z'


def date_to_year_week(date_: date) -> str:
    year, week, _ = date_.isocalendar()
    year, week = str(year), str(week).zfill(2)
    return f'{year}{week}'

import datetime

import pandas as pd
import requests
import ujson
from prefect import task

from dask import dataframe as dd
from v_m.constants import flu_a as const
from v_m.commons.database import ElasticsearchConnector


def iso_year_start(iso_year):
    """The gregorian calendar date of the first day of the given ISO year"""
    fourth_jan = datetime.date(iso_year, 1, 4)
    delta = datetime.timedelta(fourth_jan.isoweekday() - 1)
    return fourth_jan - delta


def iso_to_gregorian(iso_year, iso_week, iso_day):
    """Gregorian calendar date for the given ISO year, week and day"""
    year_start = iso_year_start(iso_year)
    return year_start + datetime.timedelta(days=iso_day - 1, weeks=iso_week - 1)


def process(x):
    year, week = int(str(x)[:4]), int(str(x)[-2:])
    dt = iso_to_gregorian(year, week, 1)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + 'Z'


@task()
def extract(timestamp):
    url = const.BASE_URL.format(','.join(const.STATE_CODES), timestamp)
    response = requests.get(url)
    data = ujson.loads(response.text).get('epidata')
    return dd.from_pandas(pd.DataFrame.from_records(data), npartitions=5)


@task()
def transform(df):
    df.region = df.region.str.upper()
    df['timestamp'] = df.epiweek.map(process)
    df = df.drop(['issue', 'release_date'], axis=1)
    df.region = 'US-' + df.region
    df['id_'] = df['region'] + df['timestamp']
    return df


@task()
def load(df):
    ElasticsearchConnector.dump(df, const.INDEX_NAME)

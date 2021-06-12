from datetime import date

import pandas as pd
import requests
import ujson
from prefect import task
from dask import dataframe as dd

from v_m.commons.timeseries import date_to_year_week, year_week_to_date
from v_m.commons.validation import ValidationException, validate_date_range
from v_m.constants import flu_a as const
from v_m.commons.database import ElasticsearchConnector


@task()
def process_parameters(start, stop):
    try:
        start, stop = date.fromisoformat(start), date.fromisoformat(stop)
    except (TypeError, ValueError) as e:
        raise ValidationException(e)
    validate_date_range(start, stop)
    print(date_to_year_week(start), date_to_year_week(stop))
    return f'{date_to_year_week(start)}-{date_to_year_week(stop)}'


@task()
def extract(date_range):
    url = const.BASE_URL.format(','.join(const.STATE_CODES), date_range)
    response = requests.get(url)
    data = ujson.loads(response.text).get('epidata')
    if not data:
        return pd.DataFrame()
    return dd.from_pandas(pd.DataFrame.from_records(data), npartitions=5)


@task()
def transform(df):
    if len(df.columns) == 0:
        return df
    df.region = df.region.str.upper()
    df['timestamp'] = df.epiweek.map(year_week_to_date)
    df = df.drop(['issue', 'release_date'], axis=1)
    df.region = 'US-' + df.region
    df['id_'] = df['region'] + df['timestamp']
    return df


@task()
def load(df):
    ElasticsearchConnector.dump(df, const.INDEX_NAME)

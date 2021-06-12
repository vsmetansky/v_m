from datetime import date

import pandas as pd
import requests
import ujson
from prefect import task
from dask import dataframe as dd

from v_m.commons.timeseries import date_to_year_week, year_week_to_date
from v_m.commons.validation import ValidationException, validate_date_range
from v_m.constants import flu_b as const
from v_m.commons.database import ElasticsearchConnector


@task()
def process_parameters(start, stop):
    try:
        start, stop = date.fromisoformat(start), date.fromisoformat(stop)
    except (TypeError, ValueError) as e:
        raise ValidationException(e)
    validate_date_range(start, stop)
    return f'{date_to_year_week(start)}-{date_to_year_week(stop)}'


@task()
def extract_epidemiological(date_range):
    url = const.BASE_EPIDEMIOLOGICAL_URL.format(','.join(const.STATE_CODES), date_range)
    response = requests.get(url)
    data = ujson.loads(response.text).get('epidata')
    if not data:
        return pd.DataFrame()
    return dd.from_pandas(pd.DataFrame.from_records(data), npartitions=5)


@task()
def extract_population():
    return dd.read_csv(const.BASE_POPULATION_URL)


@task()
def transform_epidemiological(df):
    if len(df.columns) == 0:
        return df
    df['timestamp'] = df.epiweek.map(year_week_to_date)
    df = df.drop(['issue', 'release_date'], axis=1)
    df.location = 'US-' + df.location
    df['id_'] = df.location + df.timestamp

    return df.reset_index(drop=True)


@task()
def transform_population(df):
    df = df.rename(columns={'state/region': 'location'})
    df = df.loc[df.location.str.lower().isin(const.STATE_CODES)]
    df = df.loc[df.ages == 'total']
    df.location = 'US-' + df.location
    df = df.sort_values(by=['year'])
    df = df.tail(len(const.STATE_CODES))
    return df.drop(columns=['year', 'ages']).reset_index(drop=True)


@task()
def transform(e_df, p_df):
    df = e_df.merge(p_df, how='outer', on='location').fillna(0)
    df['num_patients'] = df.population * (df.rate_overall / 100)
    return df


@task()
def load(df):
    ElasticsearchConnector.dump(df, const.INDEX_NAME)

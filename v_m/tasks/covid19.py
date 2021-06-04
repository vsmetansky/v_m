from prefect import task
from dask import dataframe as dd
import pandas as pd

from v_m.constants import covid19 as const
from v_m.commons.database import ElasticsearchConnector


@task()
def extract_epidemiological_data(timestamp):
    df = dd.read_csv(
        const.BASE_EPIDEMIOLOGICAL_URL.format(timestamp.month, timestamp.day, timestamp.year),
        usecols=list(const.EPIDEMIOLOGICAL_COLUMNS_TYPES),
        dtype=const.EPIDEMIOLOGICAL_COLUMNS_TYPES,
    )
    timestamp_iso = timestamp.isoformat()
    return df.assign(timestamp=timestamp_iso)


@task()
def extract_restrictions_data(period):
    df = dd.from_pandas(pd.read_csv(
        const.BASE_RESTRICTIONS_URL,
        usecols=list(const.RESTRICTIONS_COLUMNS_TYPES),
        dtype=const.RESTRICTIONS_COLUMNS_TYPES,
    ), npartitions=10)
    df['timestamp'] = dd.to_datetime(df['Date'], format='%Y%m%d')
    df = df.drop(['Date'], axis=1)
    df = df[df['timestamp'].between(period[0], period[-1])]
    df['timestamp'] = df['timestamp'].map(lambda x: x.isoformat())

    return df


@task()
def extract_population_data():
    return dd.read_csv(
        const.BASE_POPULATION_URL,
        usecols=list(const.POPULATION_COLUMNS_TYPES),
        dtype=const.POPULATION_COLUMNS_TYPES,
    )


@task()
def transform_epidemiological_data(df):
    df = df.rename(columns={'Country_Region': 'CountryCode'})

    df['CountryCode'] = df['CountryCode'].map(const.COUNTRY_CODES_MAP, meta=('CountryCode', 'string'))
    df = df[df['CountryCode'].isin(const.COUNTRY_CODES)]

    df = df.assign(id_=df['CountryCode'] + df['timestamp'])

    df = df.groupby('id_').agg(const.EPIDEMIOLOGICAL_AGGREGATIONS).reset_index()

    df['location'] = df['Lat'].astype(str).str.cat(df['Long_'].astype(str), sep=',')
    df = df.drop(['Lat', 'Long_'], axis=1)

    return df


@task()
def transform_restrictions_data(df):
    df = df.fillna(method='pad')

    df = df[df['CountryCode'].isin(const.COUNTRY_CODES)]

    df = df.assign(id_=df['CountryCode'] + df['timestamp'])

    df = df.groupby('id_').agg(const.RESTRICTIONS_AGGREGATIONS).reset_index()

    return df


@task()
def transform_population_data(df):
    df = df.rename(columns={'Country Code': 'CountryCode', 'Value': 'Population'})

    df = df[df['CountryCode'].isin(const.COUNTRY_CODES)]

    df = df.groupby('CountryCode').agg(const.POPULATION_AGGREGATIONS)

    return df


@task()
def transform(e_df, r_df, p_df):
    return dd.multi.concat(e_df).merge(r_df, how='outer').merge(p_df, how='outer', on='CountryCode') \
        .dropna().rename(columns=const.COLUMNS_MAP)


@task()
def load(df):
    ElasticsearchConnector.dump(df, const.INDEX_NAME)

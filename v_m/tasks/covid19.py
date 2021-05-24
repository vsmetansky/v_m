from prefect import task
from dask import dataframe as dd

from v_m.constants import covid19 as const

import pandas as pd


@task
def extract_epidemiological_data(timestamp):
    df = dd.read_csv(
        const.BASE_EPIDEMIOLOGICAL_URL.format(timestamp.month, timestamp.day, timestamp.year),
        usecols=list(const.EPIDEMIOLOGICAL_COLUMNS_TYPES),
        dtype=const.EPIDEMIOLOGICAL_COLUMNS_TYPES,
    )
    timestamp_iso = timestamp.isoformat()
    return df.assign(timestamp=timestamp_iso)


@task
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


@task
def extract_population_data():
    return dd.read_csv(
        const.BASE_POPULATION_URL,
        usecols=list(const.POPULATION_COLUMNS_TYPES),
        dtype=const.POPULATION_COLUMNS_TYPES,
    )


@task
def transform_epidemiological_data(df):
    df = df.rename(columns={'Country_Region': 'CountryCode'})

    df['CountryCode'] = df['CountryCode'].map(const.COUNTRY_CODES_MAP, meta=('CountryCode', 'string'))
    df = df[df['CountryCode'].isin(const.COUNTRY_CODES)]

    df = df.assign(id_=df['CountryCode'] + df['timestamp'])

    df = df.groupby('id_').agg({
        'Active': 'sum',
        'Deaths': 'sum',
        'Confirmed': 'sum',
        'Recovered': 'sum',
        'CountryCode': 'last',
        'Lat': 'mean',
        'Long_': 'mean',
        'timestamp': 'last'
    })

    df['location'] = df['Lat'].astype(str).str.cat(df['Long_'].astype(str), sep=',')
    df = df.drop(['Lat', 'Long_'], axis=1)

    return df


@task
def transform_restrictions_data(df):
    df = df.fillna(method='pad')

    df = df[df['CountryCode'].isin(const.COUNTRY_CODES)]

    df = df.assign(id_=df['CountryCode'] + df['timestamp'])

    df = df.groupby('id_').agg({
        'C1_School closing': 'mean',
        'C2_Workplace closing': 'mean',
        'C3_Cancel public events': 'mean',
        'C4_Restrictions on gatherings': 'mean',
        'C5_Close public transport': 'mean',
        'C6_Stay at home requirements': 'mean',
        'C7_Restrictions on internal movement': 'mean',
        'C8_International travel controls': 'mean',
        'E4_International support': 'mean',
        'H1_Public information campaigns': 'mean',
        'H2_Testing policy': 'mean',
        'H3_Contact tracing': 'mean',
        'H5_Investment in vaccines': 'mean',
        'H6_Facial Coverings': 'mean',
        'H7_Vaccination policy': 'mean',
        'StringencyIndex': 'mean',
        'GovernmentResponseIndex': 'mean',
        'ContainmentHealthIndex': 'mean',
        'EconomicSupportIndex': 'mean',
        'CountryCode': 'last',
        'timestamp': 'last'
    })

    return df


@task
def transform_population_data(df):
    df = df.rename(columns={'Country Code': 'CountryCode', 'Value': 'Population'})

    df = df[df['CountryCode'].isin(const.COUNTRY_CODES)]

    df = df.groupby('CountryCode').agg({
        'Population': 'last'
    })

    return df


@task
def transform(e_df, r_df, p_df):
    return dd.multi.concat(e_df).merge(r_df, how='outer').merge(p_df, how='outer', on='CountryCode')


@task
def load(df):
    print(df.head())

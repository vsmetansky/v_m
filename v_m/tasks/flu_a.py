from prefect import task
from dask import dataframe as dd

from v_m.constants import flu_a as const
from v_m.commons.database import ElasticsearchConnector


@task()
def extract():
    return dd.read_csv(
        const.BASE_URL,
        usecols=list(const.COLUMNS_TYPES),
        dtype=const.COLUMNS_TYPES,
    )


@task()
def transform(df):
    return df


@task()
def load(df):
    ElasticsearchConnector.dump(df, const.INDEX_NAME)

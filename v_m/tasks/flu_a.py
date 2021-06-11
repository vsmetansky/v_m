import pandas as pd
import requests
import ujson
from prefect import task

from v_m.constants import flu as const
from v_m.commons.database import ElasticsearchConnector


@task()
def extract(timestamp):
    response = requests.get(const.BASE_URL.format(','.join(const.STATE_CODES), timestamp))
    data = ujson.loads(response.text).get('epidata')
    return pd.DataFrame.from_records(data)


@task()
def transform(df):
    df.region = df.region.str.upper()
    return df


@task()
def load(df):
    ElasticsearchConnector.dump(df, const.INDEX_NAME)

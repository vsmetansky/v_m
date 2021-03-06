from typing import Any, Dict

import pandas as pd
from dask import dataframe as dd
from dask import bag as db
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

import v_m.settings as settings


class ElasticsearchConnector:
    _es = Elasticsearch(
        http_auth=(settings.ES_USER, settings.ES_PASS),
        hosts=settings.ES_HOST,
        maxsize=settings.ES_CONNECTIONS_MAX_NUM
    )

    @classmethod
    def dump(cls, df: dd.DataFrame, index_name: str):
        if len(df.columns) == 0:
            return
        documents = df.map_partitions(pd.DataFrame.to_dict, orient='records').to_delayed()
        db.from_delayed(documents).map_partitions(cls._dump, index_name).compute()

    @classmethod
    def _dump(cls, actions, index_name: str):
        return bulk(
            cls._es,
            (cls._doc_from_action(a) for a in actions),
            index=index_name,
            stats_only=settings.ES_STATS_ONLY
        )

    @staticmethod
    def _doc_from_action(action: Dict[str, Any]) -> Dict[str, Any]:
        return dict(
            _id=action.get('id_'),
            _source=action
        )

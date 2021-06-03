import os

ES_HOST = os.getenv('ES_HOST', 'localhost')
ES_CONNECTIONS_MAX_NUM = os.getenv('ES_CONNECTIONS_MAX_NUM', 100)
ES_STATS_ONLY = os.getenv('ES_STATS_ONLY', True)

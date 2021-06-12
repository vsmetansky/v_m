INDEX_NAME = 'flu_b'

BASE_EPIDEMIOLOGICAL_URL = 'https://delphi.cmu.edu/epidata/api.php?endpoint=flusurv&locations={}&epiweeks={}'
BASE_POPULATION_URL = 'https://raw.githubusercontent.com/jakevdp/data-USstates/master/state-population.csv'

STATE_CODES = {
    'al', 'ak', 'az', 'ar', 'ca', 'co', 'ct', 'dc', 'de', 'fl', 'ga', 'hi', 'id', 'il', 'in', 'ia', 'ks',
    'ky', 'la', 'me', 'md', 'ma', 'mi', 'mn', 'ms', 'mo', 'mt', 'ne', 'nv', 'nh', 'nj', 'nm', 'ny', 'nc',
    'nd', 'oh', 'ok', 'or', 'pa', 'ri', 'sc', 'sd', 'tn', 'tx', 'ut', 'vt', 'va', 'wa', 'wv', 'wi', 'wy'
}

LOOKBACK = 100
INTERVAL = 1

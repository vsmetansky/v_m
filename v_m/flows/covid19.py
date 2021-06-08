import pandas as pd
from prefect import Flow, Parameter, task
from prefect.executors import LocalDaskExecutor

from v_m.tasks import covid19 as tasks

with Flow('covid19', executor=LocalDaskExecutor()) as flow:
    start, stop = Parameter('start', default='2020-04-20'), Parameter('stop', default='2021-02-01')
    period = task()(lambda x, y: pd.date_range(*[x, y]))(start, stop)

    e_df = tasks.extract_epidemiological_data.map(period)
    r_df = tasks.extract_restrictions_data(period)
    p_df = tasks.extract_population_data()

    e_df = tasks.transform_epidemiological_data.map(e_df)
    r_df = tasks.transform_restrictions_data(r_df)
    p_df = tasks.transform_population_data(p_df)

    df = tasks.transform(e_df, r_df, p_df)

    tasks.load(df)

if __name__ == '__main__':
    flow.run()

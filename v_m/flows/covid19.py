import pandas as pd
from prefect import Flow, Parameter
from prefect.executors import LocalDaskExecutor

from v_m.tasks import covid19 as tasks

with Flow('covid19', executor=LocalDaskExecutor()) as flow:
    period = pd.date_range(*['2021-02-01', '2021-02-01'])

    e_df = tasks.extract_epidemiological_data.map(period)
    r_df = tasks.extract_restrictions_data(period)
    p_df = tasks.extract_population_data()

    e_df = tasks.transform_epidemiological_data.map(e_df)
    r_df = tasks.transform_restrictions_data(r_df)
    p_df = tasks.transform_population_data(p_df)

    df = tasks.transform(e_df, r_df, p_df)

    _ = tasks.load(df)

if __name__ == '__main__':
    flow.run()
    # flow.register(project_name='v_m')

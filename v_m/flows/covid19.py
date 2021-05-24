import pandas as pd
from prefect import Flow
from prefect.executors import LocalDaskExecutor

from v_m.tasks import covid19 as cv

with Flow('covid19', executor=LocalDaskExecutor()) as flow:
    period = pd.date_range(*['2020-05-03', '2021-04-03'])
    e_df = cv.extract_epidemiological_data.map(timestamp=period)
    r_df = cv.extract_restrictions_data(period)
    p_df = cv.extract_population_data()

    e_df = cv.transform_epidemiological_data.map(df=e_df)
    r_df = cv.transform_restrictions_data(r_df)
    p_df = cv.transform_population_data(p_df)

    df = cv.transform(e_df, r_df, p_df)

if __name__ == '__main__':
    flow.register(project_name='v_m')

    flow.run()

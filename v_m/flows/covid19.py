from prefect import Flow
from prefect.executors import LocalDaskExecutor

from v_m.flows import init_dates, init_date_parameters, init_schedule
from v_m.tasks import covid19 as tasks
from v_m.constants import covid19 as const

dates = init_dates(days=const.LOOKBACK)
start, stop = init_date_parameters(dates)
schedule = init_schedule(start, stop, dates, days=const.INTERVAL)
executor = LocalDaskExecutor()

with Flow(
        name='covid19',
        executor=executor,
        schedule=schedule
) as flow:
    date_range = tasks.process_parameters(start, stop)

    e_df = tasks.extract_epidemiological_data.map(date_range)
    r_df = tasks.extract_restrictions_data(date_range)
    p_df = tasks.extract_population_data()

    e_df = tasks.transform_epidemiological_data.map(e_df)
    r_df = tasks.transform_restrictions_data(r_df)
    p_df = tasks.transform_population_data(p_df)

    df = tasks.transform(e_df, r_df, p_df)

    tasks.load(df)

if __name__ == '__main__':
    flow.run(run_on_schedule=False)

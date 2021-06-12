from prefect import Flow
from prefect.executors import LocalDaskExecutor

from v_m.flows import init_schedule, init_dates, init_date_parameters
from v_m.tasks import flu_b as tasks
from v_m.constants import flu_b as const

dates = init_dates(weeks=const.LOOKBACK)
start, stop = init_date_parameters(dates)
schedule = init_schedule(start, stop, dates, weeks=const.INTERVAL)
executor = LocalDaskExecutor()

with Flow(
        name='flu_b',
        executor=executor,
        schedule=schedule
) as flow:
    date_range = tasks.process_parameters(start, stop)
    e_df = tasks.extract_epidemiological(date_range)
    p_df = tasks.extract_population()

    e_df = tasks.transform_epidemiological(e_df)
    p_df = tasks.transform_population(p_df)
    df = tasks.transform(e_df, p_df)

    tasks.load(df)

if __name__ == '__main__':
    flow.run(run_on_schedule=False)

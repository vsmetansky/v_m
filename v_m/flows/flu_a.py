from prefect import Flow
from prefect.executors import LocalDaskExecutor

from v_m.flows import init_schedule, init_dates, init_date_parameters
from v_m.tasks import flu_a as tasks
from v_m.constants import flu_a as const

dates = init_dates(weeks=const.LOOKBACK)
start, stop = init_date_parameters(dates)
schedule = init_schedule(start, stop, dates, weeks=const.INTERVAL)
executor = LocalDaskExecutor()

with Flow(
        name='flu_a',
        executor=executor,
        schedule=schedule
) as flow:
    date_range = tasks.process_parameters(start, stop)
    df = tasks.extract(date_range)
    df = tasks.transform(df)
    tasks.load(df)

if __name__ == '__main__':
    flow.run()

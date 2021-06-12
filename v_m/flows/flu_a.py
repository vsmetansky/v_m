from datetime import datetime, timedelta

from prefect import Flow
from prefect.core.parameter import Parameter
from prefect.executors import LocalDaskExecutor
from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock

from v_m.tasks import flu_a as tasks
from v_m.constants import flu_a as const


datetime_now = datetime.utcnow()
date_stop = datetime_now.date()
date_start = date_stop - timedelta(weeks=const.WEEKS_LOOKBACK)
date_stop_serializable = date_stop.isoformat()
date_start_serializable = date_start.isoformat()

start = Parameter(name='start', default=date_start_serializable)
stop = Parameter(name='stop', default=date_stop_serializable)

clock = IntervalClock(
    start_date=datetime_now,
    interval=timedelta(weeks=1),
    parameter_defaults={
        start.name: date_start_serializable,
        stop.name: date_stop_serializable
    }
)

schedule = Schedule(clocks=[clock])
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

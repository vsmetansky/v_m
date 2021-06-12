from datetime import datetime, timedelta
from typing import Tuple

from prefect import Parameter
from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock


def init_dates(**lookback: int) -> Tuple:
    date_now = datetime.utcnow()
    date_stop = date_now.date()
    date_start = date_stop - timedelta(**lookback)
    date_stop_serializable = date_stop.isoformat()
    date_start_serializable = date_start.isoformat()
    return date_now, date_stop, date_start, date_stop_serializable, date_start_serializable


def init_date_parameters(
        dates: Tuple
) -> Tuple[Parameter, Parameter]:
    _, _, _, date_stop_serializable, date_start_serializable = dates
    return (
        Parameter(name='start', default=date_start_serializable),
        Parameter(name='stop', default=date_stop_serializable)
    )


def init_schedule(start, stop, dates: Tuple, **interval: int) -> Schedule:
    date_now, _, _, date_stop_serializable, date_start_serializable = dates
    clock = IntervalClock(
        start_date=date_now,
        interval=timedelta(**interval),
        parameter_defaults={
            start.name: date_start_serializable,
            stop.name: date_stop_serializable
        }
    )
    return Schedule(clocks=[clock])

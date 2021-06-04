from prefect import Flow, Parameter
from prefect.executors import LocalDaskExecutor

from v_m.tasks import flu_a as tasks

with Flow('flu_a', executor=LocalDaskExecutor()) as flow:
    df = tasks.extract()
    df = tasks.transform()
    tasks.load(df)

if __name__ == '__main__':
    flow.run()

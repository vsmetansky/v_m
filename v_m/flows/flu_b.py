from prefect import Flow, Parameter
from prefect.executors import LocalDaskExecutor

from v_m.tasks import flu_b as tasks

with Flow('flu_b', executor=LocalDaskExecutor()) as flow:
    df = tasks.extract()
    df = tasks.transform(df)
    tasks.load(df)

if __name__ == '__main__':
    flow.run()

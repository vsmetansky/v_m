from prefect import Flow, Parameter
from prefect.executors import LocalDaskExecutor

from v_m.tasks import flu as tasks

with Flow('flu', executor=LocalDaskExecutor()) as flow:
    df = tasks.extract('200501-202105')
    df = tasks.transform(df)
    # tasks.load(df)

if __name__ == '__main__':
    flow.run()

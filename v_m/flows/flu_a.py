from prefect import Flow, Parameter
from prefect.executors import LocalDaskExecutor

from v_m.tasks import flu_a as tasks

with Flow('flu_a', executor=LocalDaskExecutor()) as flow:
    years = [f'20{year}01-20{year+1}01' for year in range(10, 21)]
    dfs = tasks.extract.map(years)
    dfs = tasks.transform.map(dfs)
    tasks.load.map(dfs)

if __name__ == '__main__':
    flow.run()

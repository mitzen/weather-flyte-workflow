"""A weather forecasting system."""

import pandas as pd 
from flytekit import task, workflow, Resources
from sqlite3 import connect
from flytekitplugins.dask import Dask, WorkerGroup

# @task(
#     task_config=Dask(
#         workers=WorkerGroup(
#             number_of_workers=1,
#             limits=Resources(cpu="1", mem="1Gi"),
#         ),
#     ),
#     limits=Resources(cpu="1", mem="1Gi"),
#     cache_version="1",
#     cache=True,
# )

@task()
def processdata(weatherdata: pd.DataFrame, start: int = 0, end: int = 1) -> str: 
    print("exeecuting processdata")
    print(f"output the weather data value: {weatherdata.iloc[start:end]}")
    return "excellent!"

# @task(
#     task_config=Dask(
#         workers=WorkerGroup(
#             number_of_workers=1,
#             limits=Resources(cpu="1", mem="1Gi"),
#         ),
#     ),
#     limits=Resources(cpu="1", mem="1Gi"),
#     cache_version="1",
#     cache=True,
# )
@task
def processdata2(weatherdata: pd.DataFrame, start: int = 0, end: int = 1) -> str: 
    print("exeecuting processdata2")
    print(f"output the weather data value: {weatherdata.iloc[start:end]}")
    return "excellent!"

@task(
    task_config=Dask(
        workers=WorkerGroup(
            number_of_workers=1,
        ),
    )
)
def get_weather_data(name: str) -> pd.DataFrame:
    """A simple Flyte task to say "hello".

    The @task decorator allows Flyte to use this function as a Flyte task, which
    is executed as an isolated, containerized unit of compute.
    """
    try:

        import requests
        import io
        urlData = requests.get(name).content
        return pd.read_csv(io.StringIO(urlData.decode('utf-8')))
    
    except Exception as e: 
        print(e)

@workflow
def wf(name: str) -> pd.DataFrame:
  
    #url = f"https://www.ncei.noaa.gov/access/services/data/v1"
    weatherdata = get_weather_data(name="https://www.ncei.noaa.gov/access/services/data/v1?dataset=global-marine&dataTypes=WIND_DIR,WIND_SPEED&stations=AUCE&startDate=2016-01-01&endDate=2016-01-02&boundingBox=90,-180,-90,180")
    
    processdata(weatherdata=weatherdata, start=1, end=2)
    processdata(weatherdata=weatherdata, start=3, end=4)
    
    return weatherdata


if __name__ == "__main__":
    # Execute the workflow, simply by invoking it like a function and passing in
    # the necessary parameters
    print(f"Running wf() { wf(name='test') }")
 
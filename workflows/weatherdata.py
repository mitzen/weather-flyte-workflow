"""A weather forecasting system."""

import typing
import pandas as pd 
from flytekit import task, workflow

@task
def processdata(weatherdata: pd.DataFrame) -> None: 
    print(f"output the weather data value: {weatherdata}")
    for x in weatherdata: 
        print(x)

@task
def get_weather_data(name: str) -> pd.DataFrame:
    """A simple Flyte task to say "hello".

    The @task decorator allows Flyte to use this function as a Flyte task, which
    is executed as an isolated, containerized unit of compute.
    """

    import requests
    import io
    urlData = requests.get(name).content
    return pd.read_csv(io.StringIO(urlData.decode('utf-8')))

    #return f"hello {name}!"


@workflow
def wf(name: str) -> pd.DataFrame:
  
    #url = f"https://www.ncei.noaa.gov/access/services/data/v1"
    weatherdata = get_weather_data(name="https://www.ncei.noaa.gov/access/services/data/v1?dataset=global-marine&dataTypes=WIND_DIR,WIND_SPEED&stations=AUCE&startDate=2016-01-01&endDate=2016-01-02&boundingBox=90,-180,-90,180")
    
    processdata(weatherdata=weatherdata)
    return weatherdata


if __name__ == "__main__":
    # Execute the workflow, simply by invoking it like a function and passing in
    # the necessary parameters
    print(f"Running wf() { wf(name='test') }")
 
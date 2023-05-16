# weather-flyte-workflow

A template for the recommended layout of a Flyte enabled repository for code written in python using [flytekit](https://docs.flyte.org/projects/flytekit/en/latest/).

## Usage

To get up and running with your Flyte project, we recommend following the
[Flyte getting started guide](https://docs.flyte.org/en/latest/getting_started.html).

This project includes a script `docker_build.sh` that you can use to build a
Docker image for your Flyte project.

```
# help
./docker_build.sh -h

# build an image with defaults
./docker_build.sh

# build an image with custom values
./docker_build.sh -p weather-flyte-workflow -r <REGISTRY> -v <VERSION>
```

We recommend using a git repository to version this project, so that you can
use the git sha to version your Flyte workflows.


-------------------------

docker build --tag kepung/flyte-wfimage:dask  .

docker push

pyflyte run -i kepung/flyte-wfimage:dask --remote workflows/hellodask.py wf --name "Gilgamesh"
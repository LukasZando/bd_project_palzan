# Big Data Project : Analyze Parking Ticket Data

## Setup

### Spark, Elasticsearch, and Kibana

After cloning the repository, the stack has to be setup. This can be done by running the following command:

```bash
docker-compose up -d
```

This will start the Elasticsearch, Kibana, and Spark containers in the background.

### Python Environment

The interpreter has to be installed locally, whether it is Anaconda, a virtual environment, ... doesn't matter. 
The packages that are needed are listed in the `requirements.txt` file, which can be found in the `impl` directory.
To install the packages, run the following command:

```bash
cd impl
pip install -r requirements.txt
```

## Accessing Kibana

To access Kibana, open your web browser and go to the following URL:

```
http://localhost:5601
```

> **Note:** It can take some time for Kibana to start up. 
> So it can happen that the page will result in an error. 
> Just wait a few seconds and refresh the page.

## Executing a Spark Job

To execute a Spark job the script `run-spark.sh` can be used as follows:

```bash
./run-spark.sh <script_name> [container_name]
```

Where `<script_name.py>` is the name of the script that you want to run (path relative to `impl` directory) and `[container_name]` is the name of the container that you want to run the script in (defaulting to spark as required in this project).

If you don't want to use the script, the following command can be used to run a script in the Spark container:

```bash
docker exec -it <container_name> spark-submit --conf spark.jars.ivy=/opt/bitnami/spark-app/.ivy2 /opt/bitnami/spark-app/<script_name>
```

Where again `<script_name>` is the path to the script that you want to run (path relative to `impl` directory) and `<container_name>` is the name of the container that you want to run the script in.
#!/bin/bash

# Function to show usage and exit
usage() {
  echo "Usage: $0 <script_name.py> [container_name]"
  echo "  <script_name.py> : Required. Python script to run with spark-submit."
  echo "  [container_name] : Optional. Docker container name (default: spark)."
  exit 1
}

# Validate input
if [ $# -lt 1 ]; then
  echo "Error: Missing required script name."
  usage
fi

SCRIPT_NAME="$1"
CONTAINER_NAME="${2:-spark}"
SPARK_APP_PATH="/opt/bitnami/spark-app"
IVY_CACHE="$SPARK_APP_PATH/.ivy2"

# Check that the script exists inside the container
docker exec "$CONTAINER_NAME" test -f "$SPARK_APP_PATH/$SCRIPT_NAME"
if [ $? -ne 0 ]; then
  echo "Error: Script '$SCRIPT_NAME' not found in $SPARK_APP_PATH inside container '$CONTAINER_NAME'."
  exit 2
fi

# Execute the Spark job
docker exec -it "$CONTAINER_NAME" spark-submit \
  --conf spark.jars.ivy="$IVY_CACHE" \
  "$SPARK_APP_PATH/$SCRIPT_NAME"

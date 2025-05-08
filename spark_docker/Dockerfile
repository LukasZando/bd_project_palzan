FROM bitnami/spark:3.5.5

WORKDIR /opt/bitnami/spark

USER root

COPY jars/elasticsearch-spark-30_2.12-9.0.0.jar /opt/bitnami/spark/jars/

RUN chmod 644 /opt/bitnami/spark/jars/elasticsearch-spark-30_2.12-9.0.0.jar

USER 1001
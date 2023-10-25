FROM apache/airflow:2.0.0
USER root
RUN apt-get update \
  && pip install clickhouse-driver
USER airflow
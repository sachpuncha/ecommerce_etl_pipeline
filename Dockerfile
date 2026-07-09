FROM apache/airflow:2.8.1

USER root

RUN apt-get update && apt-get install -y build-essential

USER airflow

RUN pip install google-cloud-bigquery==3.11.0 dbt-bigquery==1.7.0 db-dtypes==1.1.1 pyarrow==12.0.0
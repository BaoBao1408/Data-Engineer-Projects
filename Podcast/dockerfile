FROM apache/airflow:latest-python3.11

USER root
RUN apt-get update && \
    apt-get install -y git && \
    apt-get clean

RUN pip install --no-cache-dir xmltodict


# Set the root password
USER airflow
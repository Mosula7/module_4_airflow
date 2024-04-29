FROM apache/airflow:2.9.0

USER root

RUN apt-get update && apt-get install libgomp1

COPY configure_database.py /configure_database.py

COPY requirements.txt /requirements.txt

USER airflow
RUN pip install --no-cache-dir -r /requirements.txt



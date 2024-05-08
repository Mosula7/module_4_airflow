FROM apache/airflow:2.9.0

RUN chmod -R a+rx /var/lib/apt/lists/ && apt-get update && apt-get install -y libgomp1

COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir -r /requirements.txt



FROM apache/airflow:2.7.1


USER root
COPY requirements.txt /opt/airflow/requirements.txt
RUN apt-get update && apt-get install -y build-essential g++
RUN chown airflow: /opt/airflow/requirements.txt
USER airflow
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt
RUN python -m spacy download en_core_web_sm
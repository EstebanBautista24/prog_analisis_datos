FROM apache/airflow:2.7.1

USER root

# Java para PySpark + build tools
RUN apt-get update && apt-get install -y \
    build-essential \
    g++ \
    default-jdk \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Java 11 (lo que instala default-jdk en Debian Bullseye)
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

COPY requirements.txt /opt/airflow/requirements.txt
RUN chown airflow: /opt/airflow/requirements.txt

USER airflow

RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt
RUN python -m spacy download en_core_web_sm
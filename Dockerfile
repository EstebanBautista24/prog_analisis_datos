FROM apache/airflow:2.7.1

# Copiamos requirements como root
USER root
COPY requirements.txt /opt/airflow/requirements.txt

# Cambiamos permisos (muy importante)
RUN chown airflow: /opt/airflow/requirements.txt

# 🔥 Instalamos como airflow
USER airflow
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt
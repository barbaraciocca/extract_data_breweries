# Dockerfile

FROM apache/airflow:2.11.0

# Muda para usuário airflow antes de instalar
USER airflow

# Instala provider Amazon (S3/MinIO)
RUN pip install --no-cache-dir apache-airflow-providers-amazon

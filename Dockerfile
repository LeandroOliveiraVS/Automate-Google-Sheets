ARG AIRFLOW_IMAGE_NAME=apache/airflow:3.0.2
FROM ${AIRFLOW_IMAGE_NAME}

# Usuário para instalar pacotes.
USER airflow

# Arquivo de dependências.
COPY requirements.txt /requirements.txt

# Instale as dependências
RUN pip install --no-cache-dir -r /requirements.txt


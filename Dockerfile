ARG AIRFLOW_IMAGE_NAME=apache/airflow:3.0.2
FROM ${AIRFLOW_IMAGE_NAME}

# Usuário para instalar pacotes.
USER airflow

# Arquivo de dependências.
COPY requirements.txt /requirements-dev.txt

# Instale as dependências
RUN pip install --no-cache-dir -r /requirements-dev.txt

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    gnupg \
    unixodbc-dev \
    && rm -rf /var/lib/apt/lists/*

# Adiciona a chave e o repositório de pacotes da Microsoft
RUN curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg
RUN curl -fsSL https://packages.microsoft.com/config/debian/12/prod.list > /etc/apt/sources.list.d/mssql-release.list



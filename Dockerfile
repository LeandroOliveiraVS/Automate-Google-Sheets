# Usa a mesma imagem base que está no seu docker-compose.yml
ARG AIRFLOW_IMAGE_NAME=apache/airflow:2.9.2
FROM ${AIRFLOW_IMAGE_NAME}

# Mude para o usuário root para todas as instalações
USER root

# Instala as dependências de sistema (drivers ODBC) primeiro
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    gnupg \
    unixodbc-dev \
    && rm -rf /var/lib/apt/lists/*

# Adiciona a chave e o repositório de pacotes da Microsoft
RUN curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg
RUN curl -fsSL https://packages.microsoft.com/config/debian/12/prod.list > /etc/apt/sources.list.d/mssql-release.list

# Instala o driver ODBC da Microsoft, aceitando o contrato de licença (EULA)
RUN apt-get update && ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql18

# Copia o arquivo de dependências Python
COPY requirements-dev.txt /requirements-dev.txt

# Usa o user airflow para o pip install
USER airflow

# Instala as dependências Python
RUN pip install --no-cache-dir -r /requirements-dev.txt





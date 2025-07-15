# Projeto de Pipeline de Dados com Airflow

Este projeto extrai dados do Google Sheets, os transforma e carrega em um banco de dados MySQL usando Apache Airflow e Docker.

## Configuração do Ambiente

Para rodar este projeto, siga os passos abaixo:

**Pré-requisitos:** Docker e Docker Compose instalados.

### 1. Preparar Arquivos de Configuração
Você precisará de alguns arquivos para configurar o ambiente. Crie a seguinte estrutura de pastas:

```
/servidor_airflow/
├── config/
│   └── sheets_config.json
├── secrets/
│   └── sua_credencial_google.json
│── dags/
│   └── seu_dag.py
├── src/
│   └── seu_script.py
├── docker-compose.yml
└── .env
```

* **`docker-compose.yml`**: Copie o conteúdo do arquivo `docker-compose.yml` deste repositório.
* **`secrets/sua_credencial_google.json`**: Coloque aqui seu arquivo de credenciais do Google.
* **`config/sheets_config.json`**: Crie este arquivo com base no `config/sheets_config.template.json` do repositório, preenchendo os IDs e nomes reais.
* **`.env`**: Crie este arquivo e adicione a seguinte linha (use `id -u` em um terminal Linux para encontrar seu ID):
  ```env
  AIRFLOW_UID=1000
  ```

### 2. Baixar a Imagem Docker Pronta
Faça o login no GitHub Container Registry e baixe a imagem mais recente.
```bash
# Faça o login com seu usuário do GitHub e um Personal Access Token
docker login ghcr.io -u SEU_USUARIO_GITHUB

# Baixe a imagem
docker pull ghcr.io/leandrooliveiravs/automate-google-sheets:latest
```
Comente a linha 'Build: .' e descomente a linha 'Image: ghcr.io/leandrooliveiravs/automate-google-sheets:latest' no arquivo `docker-compose.yml`.

### 3. Iniciar a Aplicação
Dentro da pasta `/servidor_airflow`, execute o comando:
```bash
docker compose up -d
```

### 4. Configurar Conexões no Airflow
Acesse a interface do Airflow (`http://localhost:8080`) e configure as conexões necessárias em **Admin -> Connections**, como `google_cloud_default` e a conexão para o seu SQL Server.
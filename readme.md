# Projeto de Pipeline de Dados com Airflow

Este projeto extrai dados do Google Sheets, os transforma e carrega em um banco de dados MySQL usando Apache Airflow e Docker.

## Configuração do Ambiente

Para rodar este projeto, siga os passos abaixo:

1.  **Clonar o Repositório:**
    ```bash
    git clone [https://github.com/LeandroOliveiraVS/Automate-Google-Sheets](https://github.com/seu-usuario/seu-repositorio.git)
    cd seu-repositorio
    ```

2.  **Configurar Credenciais do Google:**
    * Crie uma pasta `secrets/` na raiz do projeto.
    * Coloque seu arquivo de credenciais do Google (JSON) dentro desta pasta.

3.  **Configurar Planilhas:**
    * Na pasta `config/`, renomeie o arquivo `sheets_config.template.json` para `sheets_config.json`.
    * Edite o `sheets_config.json` e preencha os valores reais, como o `sheet_id`.

4.  **Configurar Variáveis de Ambiente:**
    * Crie um arquivo `.env` na raiz do projeto. Ele deve conter as seguintes variáveis:
      ```env
      # ID do usuário para evitar problemas de permissão no Linux
      AIRFLOW_UID=1000 

      # Dependências Python adicionais (se houver)
      # _PIP_ADDITIONAL_REQUIREMENTS=""
      ```
      *Para obter seu UID no Linux/macOS, use o comando `id -u`.*

5.  **Construir e Iniciar o Ambiente:**
    ```bash
    docker compose build
    docker compose up -d
    ```

6.  **Configurar Conexões no Airflow:**
    * Acesse a interface do Airflow em `http://localhost:8080`.
    * Vá em **Admin -> Connections** e crie as seguintes conexões:
        * **`google_cloud_default`**: Para o Google Cloud.
        * **`mysql_local`**: Para o seu banco de dados MySQL.

Agora o projeto está pronto para ser executado.
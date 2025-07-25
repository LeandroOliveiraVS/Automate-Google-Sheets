import logging
import pandas as pd
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.mysql.hooks.mysql import MySqlHook


# =================================================================================
#                      CARREGAR OS DADOS NA TABELA MSSQL                          #
# =================================================================================
def load_data_to_mssql(mssql_conn_id: str, sheet_config: dict, df_final: pd.DataFrame):

    table = sheet_config["table"]

    try:
        # 1. Conectar ao banco de dados.
        hook = MsSqlHook(mssql_conn_id=mssql_conn_id)

        # 2. Define os campos de destino a partir das colunas do DataFrame
        target_fields = df_final.columns.tolist()

        # 3. Converte o DataFrame em uma lista de tuplas/listas, substituindo nulos
        rows_to_insert = df_final.astype(object).replace({pd.NA: None}).values.tolist()

        # 4. Chame o método 'insert_rows' do Hook
        hook.insert_rows(table=table, rows=rows_to_insert, target_fields=target_fields)

        logging.info(f"Carga concluída com sucesso para a tabela '{table}'.")

    except Exception as e:
        logging.error(f"Erro durante a carga para a tabela '{table}': {e}")
        raise  # Re-lança a exceção para que o Airflow marque a tarefa como 'failed'


# =================================================================================
#                      CARREGAR OS DADOS NA TABELA MYSQL                          #
# =================================================================================
def load_data_to_mysql(mysql_conn_id: str, sheet_config: dict, df_final: pd.DataFrame):

    table = sheet_config["table"]

    try:
        # 1. Conectar ao banco de dados.
        hook = MySqlHook(mysql_conn_id=mysql_conn_id)

        # 2. Define os campos de destino a partir das colunas do DataFrame
        target_fields = df_final.columns.tolist()

        # 3. Converte o DataFrame em uma lista de tuplas/listas, substituindo nulos
        rows_to_insert = df_final.astype(object).replace({pd.NA: None}).values.tolist()

        # 4. Chame o método 'insert_rows' do Hook
        hook.insert_rows(table=table, rows=rows_to_insert, target_fields=target_fields)

        logging.info(f"Carga concluída com sucesso para a tabela '{table}'.")

    except Exception as e:
        logging.error(f"Erro durante a carga para a tabela '{table}': {e}")
        raise  # Re-lança a exceção para que o Airflow marque a tarefa como 'failed'

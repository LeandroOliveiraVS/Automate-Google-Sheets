import pandas as pd
import logging
import mysql.connector
from airflow.providers.mysql.hooks.mysql import MySqlHook

def load_data_to_mysql(mysql_conn_id: str, sheet_config: dict, df_final: pd.DataFrame):

    mysql_table = sheet_config['mysql_table']

    try:
        # 1. Conectar ao banco de dados.
        hook = MySqlHook(mysql_conn_id=mysql_conn_id)

        # 2. Define os campos de destino a partir das colunas do DataFrame
        target_fields = df_final.columns.tolist()
        
        # 3. Converte o DataFrame em uma lista de tuplas/listas, substituindo nulos
        rows_to_insert = df_final.replace({pd.NA: None}).values.tolist()

        # 4. Chame o método 'insert_rows' do Hook
        hook.insert_rows(
            table=mysql_table,
            rows=rows_to_insert,
            target_fields=target_fields
        )

        logging.info(f"Carga concluída com sucesso para a tabela '{mysql_table}'.")

    except Exception as e:
        logging.error(f"Erro durante a carga para a tabela '{mysql_table}': {e}")
        raise # Re-lança a exceção para que o Airflow marque a tarefa como 'failed'
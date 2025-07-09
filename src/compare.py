import logging
import pandas as pd
from airflow.providers.mysql.hooks.mysql import MySqlHook

def compare_data(mysql_conn_id: str, sheet_config:dict, df_transformado: pd.DataFrame) -> pd.DataFrame:

    # Defining variables
    mysql_table = sheet_config['mysql_table']
    key_column = sheet_config['key_column']

    try:
        logging.info(f"Comparing data for table: {mysql_table}, key: {key_column}")

        if df_transformado.empty:
            logging.info("DataFrame de entrada está vazio. Nenhuma comparação necessária.")
            return df_transformado

        if key_column not in df_transformado.columns:
            raise ValueError(f"Key column '{key_column}' not found in sheet data")
        
        # 1. Usar o Airflow Hook para interagir com o banco de dados
        mysql_hook = MySqlHook(mysql_conn_id=mysql_conn_id)
        
        # 2. Garantir que a coluna chave no DataFrame não tenha nulos
        df_transformado.dropna(subset=[key_column], inplace=True)
        
        # Pega todas as chaves únicas do DataFrame para consultar o banco
        keys_from_sheet = df_transformado[key_column].unique().tolist()
        
        if not keys_from_sheet:
            logging.info("Nenhuma chave válida para comparar após a limpeza.")
            return pd.DataFrame()
        
        sql_query = f"SELECT `{key_column}` FROM `{mysql_table}` WHERE `{key_column}` IN %s"
        
        existing_keys_tuples = mysql_hook.get_records(sql=sql_query, parameters=(keys_from_sheet,))

        # Converter para um conjunto (set) para uma busca ultra-rápida
        existing_keys_set = {key[0] for key in existing_keys_tuples}
        
        logging.info(f"Encontradas {len(existing_keys_set)} chaves correspondentes no banco de dados.")

        # 4. Filtrar o DataFrame para manter apenas as chaves que NÃO foram encontradas no banco
        missing_data = df_transformado[~df_transformado[key_column].isin(existing_keys_set)]
        return missing_data

    except ValueError as ve:
        logging.error(f"Value error during comparison: {ve}")
        raise
    except Exception as e:
        logging.error(f"Error comparing data: {e}")
        raise
    

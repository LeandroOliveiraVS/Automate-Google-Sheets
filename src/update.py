import pandas as pd
import logging
import mysql.connector
from mysql.connector import Error

def update_mysql_from_file(mysql_conn_id: str, sheet_config:dict, df_transformado: pd.DataFrame):
    """
    Função reutilizável que compara dados de um arquivo com uma tabela MySQL
    e atualiza múltiplas colunas nas linhas que foram alteradas.
    """
    # Variaveis do sheet.
    mysql_table = sheet['mysql_table']
    update_cols = sheet['update_cols'] 
    key_column = sheet['key_column']
    
    logging.info("Iniciando a tarefa de atualização do MySQL.")
    
    try:
        # Dados CSV que entram
        logging.info(f"Lendo arquivo de dados de entrada: {input_path}")
        df_new_data = pd.read_csv(input_path)
        
        # Checagem de colunas presentes.
        required_cols = [key_column] + update_cols
        if not all(col in df_new_data.columns for col in required_cols):
            raise ValueError(f"Colunas necessárias ({required_cols}) não encontradas no arquivo.")
        
        # Preencher espaços em branco para o banco MySQL.
        df_new_data.fillna('', inplace=True)

        # Conectando-se ao banco de dados.
        logging.info(f"Conectando ao MySQL para buscar dados da tabela `{mysql_table}`")
        conn = mysql.connector.connect(**mysql_config)
        
        # Query dinamica para selecionar as colunas e valores para atualização.
        cols_to_select = ", ".join([f"`{col}`" for col in required_cols])
        df_sql_current = pd.read_sql(f"SELECT {cols_to_select} FROM `{mysql_table}`", conn)

        # Transformando o nome das colunas dos dados antigos para COLUNA_old.
        logging.info("Comparando dados para encontrar diferenças...")
        rename_dict = {col: f'{col}_old' for col in update_cols}
        df_sql_current.rename(columns=rename_dict, inplace=True)
        
        # Transformar ambas as colunas chaves para o merge.
        df_new_data[key_column] = df_new_data[key_column].astype(str)
        if not df_sql_current.empty:
            df_sql_current[key_column] = df_sql_current[key_column].astype(str)

        # Merge para juntas as tabelas do Sheets e do mysql nas linhas de dados que tem a mesma coluna chave.
        merged_df = pd.merge(df_new_data, df_sql_current, on=key_column, how='inner')
        
        # Criação de duas novas tabelas com os valores velhos e novos a partir do merged_df.
        df_new_values = merged_df[update_cols]
        df_old_values = merged_df[[f'{col}_old' for col in update_cols]]
        df_old_values.columns = update_cols
        
        # Comparando as duas tabelas para achar dados diferentes e criando uma outra tabela somente com as linhas com os dados
        # para atualizar.
        diff_mask = (df_new_values.ne(df_old_values) | (df_new_values.notna() & df_old_values.isna())).any(axis=1)
        rows_to_update = merged_df[diff_mask]

        # Se estiver vazio retorna mensagem.
        if rows_to_update.empty:
            logging.info("Nenhuma mudança encontrada. O banco de dados já está atualizado. ✅")
            conn.close()
            return

        # Se houver dados a query para atualizar (UPDATE) é criada. 
        logging.info(f"Encontradas {len(rows_to_update)} linhas com alterações para atualizar.")
        cursor = conn.cursor()
        set_clause = ", ".join([f"`{col}` = %s" for col in update_cols])
        sql_update_query = f"UPDATE `{mysql_table}` SET {set_clause} WHERE `{key_column}` = %s"

        # Itera sobre as linhas de dados na tabela de dados para atualizar.
        for _, row in rows_to_update.iterrows():
            update_values = [row[col] for col in update_cols]
            key_value = row[key_column]
            data_tuple = tuple(update_values + [key_value])
            cursor.execute(sql_update_query, data_tuple)
        
        # Confirma as alterações e fecha a conexão com o banco de dados.
        conn.commit()
        cursor.close()
        conn.close()
        logging.info(f"Atualização concluída com sucesso! {len(rows_to_update)} linhas afetadas. 🚀")

    except Error as e:
        logging.error(f"Erro de Banco de Dados: {e}")
        if conn and conn.is_connected(): conn.rollback()
        raise # Re-lança a exceção para que o Airflow marque a tarefa como falha
    except FileNotFoundError:
        logging.error(f"Arquivo de entrada não encontrado em: {input_path}")
        raise
    except Exception as e:
        logging.error(f"Ocorreu um erro geral: {e}")
        raise
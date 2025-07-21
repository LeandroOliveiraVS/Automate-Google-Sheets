import logging

import pandas as pd
import pendulum
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook


# ========================================================================
#                    ATUALIZAR BANCO MSSQL                               #
# ========================================================================
def update_sql_mssql(
    mssql_conn_id: str, sheet_config: dict, df_transformado: pd.DataFrame
):

    # Variaveis do sheet.
    main_table = sheet_config["table"]
    key_column = sheet_config["key_column"]

    if df_transformado.empty:
        logging.info(
            f"DataFrame vazio. Nenhuma atualização para a tabela '{main_table}'."
        )
        return

    # Colunas para a SQL Query
    all_cols = df_transformado.columns.tolist()

    update_cols = [col for col in all_cols if col != key_column]

    # Usar um nome de tabela único com timestamp para evitar conflitos
    staging_table_name = f"staging_{main_table}_{int(pendulum.now().timestamp())}"

    hook = MsSqlHook(mssql_conn_id=mssql_conn_id)

    # O get_sqlalchemy_engine() é a forma mais fácil de usar o pandas.to_sql com o Hook
    engine = hook.get_sqlalchemy_engine(fast_executemany=True)

    logging.info(
        f"Iniciando atualização para {len(df_transformado)} registros na tabela '{main_table}' via tabela de staging '{staging_table_name}'."
    )

    # Usamos um bloco try/finally para garantir que a tabela de staging seja sempre apagada
    try:
        # --- Passo 1: Carregar os dados novos para a tabela de staging ---
        logging.info(f"Carregando dados para a tabela de staging: {staging_table_name}")
        # O método to_sql do pandas cria e carrega a tabela de uma vez só
        df_transformado.to_sql(
            staging_table_name,
            con=engine,
            index=False,
            if_exists="replace",
            chunksize=1000,
        )
        logging.info("Dados carregados para a tabela de staging com sucesso.")

        # --- Passo 2: Montar e executar a query de MERGE com JOIN ---

        # Cláusula de colunas para o INSERT: [col1], [col2], [col3]
        insert_cols_str = ", ".join([f"[{col}]" for col in all_cols])

        # Cláusula de valores para o INSERT: S.[col1], S.[col2], S.[col3]
        insert_values_str = ", ".join([f"S.[{col}]" for col in all_cols])

        # Cláusula SET para o UPDATE: T.[col1] = S.[col1], T.[col2] = S.[col2]
        update_set_clause = ", ".join([f"T.[{col}] = S.[{col}]" for col in update_cols])

        # A query junta a tabela principal (T) com a de staging (S) e atualiza os campos
        sql_update_query = f"""
            MERGE INTO [{main_table}] AS T
            USING [{staging_table_name}] AS S 
            ON (T.[{key_column}] = S.[{key_column}])
            WHEN MATCHED THEN
                UPDATE SET {update_set_clause}
            WHEN NOT MATCHED BY TARGET THEN
                INSERT ({insert_cols_str})
                VALUES ({insert_values_str});
        """

        logging.info("Executando a query de UPDATE a partir da tabela de staging...")
        # Usamos o método 'run' do hook para executar a atualização
        result = hook.run(sql_update_query)
        logging.info(f"Query de UPDATE concluída. Resultado: {result}")

    except Exception as e:
        logging.error(
            f"Erro durante a operação de atualização via tabela de staging: {e}"
        )
        raise  # Re-lança a exceção para que a tarefa falhe no Airflow

    finally:
        # --- Passo 3: Apagar a tabela de staging (MUITO IMPORTANTE) ---
        logging.info(f"Limpando e removendo a tabela de staging: {staging_table_name}")
        hook.run(f"DROP TABLE IF EXISTS [{staging_table_name}]")


# ========================================================================
#                      ATUALIZAR BANCO MYSQL                             #
# ========================================================================
def update_sql_mysql(
    mssql_conn_id: str, sheet_config: dict, df_transformado: pd.DataFrame
):
    # Variaveis do sheet.
    mysql_table = sheet_config["mysql_table"]
    update_cols = sheet_config["update_cols"]
    key_column = sheet_config["key_column"]

    # Pega todas as colunas do DataFrame para o insert
    all_cols = df_transformado.columns.tolist()

    if df_transformado.empty:
        logging.info(
            f"DataFrame vazio. Nenhuma operação de upsert para a tabela '{mysql_table}'."
        )
        return

    logging.info(
        f"Iniciando operação de UPSERT para {len(df_transformado)} registros na tabela '{mysql_table}'."
    )

    try:
        # Conexão
        hook = mssql_conn_id(mssql_conn_id=mssql_conn_id)

        # Prepara os dados para inserção, convertendo nulos do pandas para None do Python
        rows_to_insert = df_transformado.replace({pd.NA: None}).values.tolist()

        # --- Montagem da Query SQL ---

        # Cláusula de colunas para o INSERT: `(col1, col2, col3)`
        insert_clause = ", ".join([f"`{col}`" for col in all_cols])

        # Placeholders para os valores: `(%s, %s, %s)`
        value_placeholders = ", ".join(["%s"] * len(all_cols))

        # Cláusula de atualização para o ON DUPLICATE KEY: `col2 = VALUES(col2), col3 = VALUES(col3)`
        update_clause = ", ".join(
            [f"`{col}` = VALUES(`{col}`)" for col in update_cols if col != key_column]
        )

        # CORREÇÃO 2: Remover a key_column da cláusula ON DUPLICATE KEY
        sql_upsert_query = f"""
            INSERT INTO `{mysql_table}` ({insert_clause})
            VALUES ({value_placeholders})
            ON DUPLICATE KEY UPDATE {update_clause}
        """

        # Executa a query para todas as linhas de uma só vez
        hook.run(sql_upsert_query, parameters=rows_to_insert)

        logging.info(
            f"Operação de UPSERT concluída com sucesso para a tabela '{mysql_table}'."
        )

    except Exception as e:
        logging.error(
            f"Erro durante a operação de UPSERT para a tabela '{mysql_table}': {e}"
        )
        raise

import logging
import pandas as pd
import pendulum
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.mysql.hooks.mysql import MySQLHook


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
    mysql_conn_id: str, sheet_config: dict, df_transformado: pd.DataFrame
):
    # Variaveis do sheet.
    main_table = sheet_config["table"]
    key_column = sheet_config["key_column"]

    # Pega todas as colunas do DataFrame para o insert
    all_cols = df_transformado.columns.tolist()
    # Colunas a serem comparadas e atualizadas
    update_cols = [col for col in all_cols if col != key_column]

    if df_transformado.empty:
        logging.info(
            f"DataFrame vazio. Nenhuma operação de upsert para a tabela '{main_table}'."
        )
        return

    staging_table_name = f"staging_{main_table}_{int(pendulum.now().timestamp())}"
    hook = MySQLHook(mysql_conn_id=mysql_conn_id)
    engine = hook.get_sqlalchemy_engine()

    logging.info(
        f"Iniciando sincronização para {len(df_transformado)} registros na tabela '{main_table}' via staging."
    )

    try:
        # --- Passo 1: Carregar dados para a tabela de staging ---
        df_transformado.to_sql(
            staging_table_name,
            con=engine,
            index=False,
            if_exists="replace",
            chunksize=1000,
        )
        logging.info(
            f"Dados carregados para a tabela de staging '{staging_table_name}'."
        )

        # --- Passo 2: UPDATE Inteligente (apenas linhas que mudaram) ---

        # Cria a cláusula SET: T.`col1` = S.`col1`, T.`col2` = S.`col2`, ...
        update_set_clause = ", ".join([f"T.`{col}` = S.`{col}`" for col in update_cols])

        # Cria a cláusula WHERE para comparar cada coluna.
        where_clause = " OR ".join(
            [f"NOT (T.`{col}` <=> S.`{col}`)" for col in update_cols]
        )

        # --- QUERY DE UPDATE ---
        sql_update_query = f"""
            UPDATE `{main_table}` T JOIN `{staging_table_name}` S
            ON T.`{key_column}` = S.`{key_column}`
            SET {update_set_clause}
            WHERE {where_clause} """

        logging.info("Executando UPDATE para as linhas que foram alteradas...")
        hook.run(sql_update_query)

    except Exception as e:
        logging.error(f"Erro durante a sincronização para a tabela '{main_table}': {e}")
        raise
    finally:
        # --- Passo 4: Limpar a tabela de staging ---
        logging.info(f"Limpando e removendo a tabela de staging: {staging_table_name}")
        hook.run(f"DROP TABLE IF EXISTS `{staging_table_name}`")

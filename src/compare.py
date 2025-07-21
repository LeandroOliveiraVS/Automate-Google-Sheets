import logging

import pandas as pd
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook


# ===============================================================================================================================================================
#                                                       COMPARAR DADOS DO BANCO COM OS QUE VEM DA PLANILHA                                                     #
# ===============================================================================================================================================================
def compare_data(
    mssql_conn_id: str, sheet_config: dict, df_transformado: pd.DataFrame
) -> pd.DataFrame:

    table = sheet_config["table"]
    key_column = sheet_config["key_column"]

    try:

        hook = MsSqlHook(mssql_conn_id=mssql_conn_id)

        # 1. Captar a linha com a ultima entrada
        date_query = f"SELECT MAX({key_column}) FROM {table}"
        result = hook.get_first(sql=date_query)

        # 2. Tratar o caso da tabela estar vazia ou a consulta não retornar nada
        if not result or result[0] is None:
            logging.info(
                f"Tabela de destino '{table}' está vazia ou não contém registros. Processando todos os dados."
            )
            return df_transformado

        max_date_in_db = result[0]
        logging.info(f"Último registro no banco de dados é de: {max_date_in_db}")

        # 3. Garantir que a coluna de data no DataFrame tenha o tipo correto para comparação
        df_transformado[key_column] = pd.to_datetime(df_transformado[key_column])

        # 4. Corrigir o filtro: usar a coluna correta e atribuir o resultado a uma nova variável
        new_data_df = df_transformado[df_transformado[key_column] > max_date_in_db]

        logging.info(f"Encontrados {len(new_data_df)} novos registros para processar.")
        return new_data_df

    except Exception as e:
        logging.error(
            f"Erro durante a operação de transformar os dados para a tabela '{table}': {e}"
        )
        raise

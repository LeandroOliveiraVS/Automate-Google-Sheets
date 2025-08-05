import logging
import pandas as pd


# =================================================================================
#                     TRANSFORMAR OS DADOS DA PLANILHA                            #
# =================================================================================
def Transform_Data_1(df_extraido: pd.DataFrame, sheet_config: dict) -> pd.DataFrame:

    # Defining variables
    table = sheet_config["table"]
    key_column = sheet_config["key_column"]
    uncolumns = sheet_config["uncolumns"]

    # Garante que as colunas de data e hora existam na configuração
    date_cols = sheet_config.get("dates", [])
    hour_cols = sheet_config.get("hours", [])

    try:
        logging.info(f"Transforming data for table: {table}, key: {key_column}")
        df_sheets = df_extraido.copy()
        if key_column not in df_sheets.columns:
            raise ValueError(f"Key column '{key_column}' not found in sheet data")

        # Format date columns
        df_sheets[key_column] = pd.to_datetime(
            df_sheets[key_column], dayfirst=True, errors="coerce"
        )
        # Concatena as listas para uma verificação única
        all_datetime_cols = date_cols + hour_cols
        for col in all_datetime_cols:
            if col in df_sheets.columns:
                logging.info(f"Processando coluna de data/hora: {col}")

                # Guarda o número de linhas antes da limpeza
                rows_before = len(df_sheets)

                # Converte para datetime, forçando erros a se tornarem Nulos (NaT)
                df_sheets[col] = pd.to_datetime(
                    df_sheets[col], dayfirst=True, errors="coerce"
                )

                # --- Remove as linhas com datas/horas nulas ---
                df_sheets.dropna(subset=[col], inplace=True)

                rows_after = len(df_sheets)
                if rows_after < rows_before:
                    logging.warning(
                        f"Removidas {rows_before - rows_after} linhas com valores nulos/inválidos na coluna '{col}'."
                    )

                # Agora, formata para o tipo de dado final
                if col in date_cols:
                    df_sheets[col] = df_sheets[col].dt.strftime("%Y-%m-%d")
                elif col in hour_cols:
                    df_sheets[col] = df_sheets[col].dt.strftime("%H:%M:%S")

        # Remove specified uncolumns
        cols_to_drop = set(uncolumns) & set(df_sheets.columns)
        if cols_to_drop:
            df_sheets.drop(columns=list(cols_to_drop), inplace=True)
            logging.info(f"Removed columns: {list(cols_to_drop)}")

        original_row_count = len(df_sheets)
        df_sheets.dropna(subset=[key_column], inplace=True)
        if len(df_sheets) < original_row_count:
            logging.warning(
                f"Dropped {original_row_count - len(df_sheets)} rows due to missing key in '{key_column}'."
            )

        return df_sheets
    except ValueError as ve:
        logging.error(f"Value error during transformation: {ve}")
        raise
    except Exception as e:
        logging.error(f"Error transforming data: {e}")
        raise

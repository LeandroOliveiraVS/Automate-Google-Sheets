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

    try:
        logging.info(f"Transforming data for table: {table}, key: {key_column}")
        df_sheets = df_extraido.copy()
        if key_column not in df_sheets.columns:
            raise ValueError(f"Key column '{key_column}' not found in sheet data")

        # Format date columns to mysql database
        df_sheets[key_column] = pd.to_datetime(
            df_sheets[key_column], dayfirst=True, errors="coerce"
        )
        if sheet_config["dates"]:
            dates = sheet_config["dates"]
            for col in dates:
                df_sheets[col] = pd.to_datetime(
                    df_sheets[col], dayfirst=True, errors="coerce"
                ).dt.strftime("%Y-%m-%d")
                logging.info(
                    f"Transforming date columns for table: {table}, col: {col}"
                )
        # Format hours columns to mysql database
        if sheet_config["hours"]:
            hours = sheet_config["hours"]
            for col in hours:
                if col in df_sheets.columns:
                    df_sheets[col] = pd.to_datetime(
                        df_sheets[col], errors="coerce"
                    ).dt.strftime("%H:%M:%S")
                    logging.info(
                        f"Transforming hour columns for table: {table}, col: {col}"
                    )

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

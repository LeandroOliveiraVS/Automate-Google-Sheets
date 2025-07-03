import logging
import pandas as pd


def Transform_Data_1(input_path, key_column, mysql_table, output_path, uncolumns, dates, hours):
    try:
        logging.info(f"Transforming data for table: {mysql_table}, key: {key_column}")
        df_sheets = pd.read_csv(input_path)
        if key_column not in df_sheets.columns:
            raise ValueError(f"Key column '{key_column}' not found in sheet data")
        
        # Format date columns to mysql database
        df_sheets[key_column] = pd.to_datetime(df_sheets[key_column], dayfirst=True, errors='coerce')
        if dates:
            for col in dates:
                df_sheets[col] = pd.to_datetime(df_sheets[col], dayfirst=True, errors='coerce').dt.strftime('%Y-%m-%d')
                logging.info(f"Transforming date columns for table: {mysql_table}, col: {col}")
        # Format hours columns to mysql database
        if hours:
            for col in hours:
                if col in df_sheets.columns:
                    df_sheets[col] = pd.to_datetime(df_sheets[col], errors='coerce').dt.strftime('%H:%M:%S')
                    logging.info(f"Transforming hour columns for table: {mysql_table}, col: {col}")

        # Remove specified uncolumns
        if uncolumns:
            df_sheets = df_sheets.drop(columns=[col for col in df_sheets.columns if col in uncolumns])
            logging.info(f"Removed uncolumns: {uncolumns}")
            transformed_sheets = pd.DataFrame(df_sheets)
            transformed_sheets.to_csv(output_path, index=False)
            logging.info(f"Transformed data saved to {output_path}")
            return output_path
        else:
            logging.info("No uncolumns specified for removal.")
            transformed_sheets = pd.DataFrame(df_sheets)
            transformed_sheets.to_csv(output_path, index=False)
            return output_path
    except ValueError as ve:
        logging.error(f"Value error during transformation: {ve}")
        raise
    except Exception as e:
        logging.error(f"Error transforming data: {e}")
        raise 
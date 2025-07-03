import mysql.connector
from mysql.connector import Error
from datetime import datetime
import logging
import pandas as pd

def compare_data(mysql_config, input_path, key_column, mysql_table, output_path):
    try:
        logging.info(f"Comparing data for table: {mysql_table}, key: {key_column}")
        df_sheets = pd.read_csv(input_path)
        if key_column not in df_sheets.columns:
            raise ValueError(f"Key column '{key_column}' not found in sheet data")
        
        df_sheets[key_column] = pd.to_datetime(df_sheets[key_column], dayfirst=True, errors='coerce')
        null_rows = df_sheets[df_sheets[key_column].isnull()]
        if not null_rows.empty:
            logging.warning(f"{len(null_rows)} rows dropped due to missing '{key_column}'.")
        df_sheets = df_sheets[df_sheets[key_column].notnull()]

        conn = mysql.connector.connect(**mysql_config)
        cursor = conn.cursor(dictionary=True)
        cursor.execute(f"SELECT * FROM {mysql_table};")
        df_sql = pd.DataFrame(cursor.fetchall())

        if df_sql.empty:
            logging.info(f"No data found in MySQL table '{mysql_table}'.")
            cursor.close()
            conn.close()
            missing_data = df_sheets
            missing_data.to_csv(output_path, index=False)
            return output_path

        else: 
            if 'registro' not in df_sql.columns:
                raise ValueError(f"Column 'registro' not found in MySQL table {mysql_table}")
            else:
                logging.info(f"Column 'registro' found in MySQL table {mysql_table}")
                df_sql['registro'] = pd.to_datetime(df_sql['registro'], errors='coerce')
        
                missing_data = df_sheets[~df_sheets[key_column].isin(df_sql['registro'])]
                missing_data.to_csv(output_path, index=False)  
                
                cursor.close()
                conn.close()
                return output_path
    except ValueError as ve:
        logging.error(f"Value error during comparison: {ve}")
        raise
    except Exception as e:
        logging.error(f"Error comparing data: {e}")
        raise
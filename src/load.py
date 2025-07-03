import pandas as pd
import logging
import mysql.connector
from mysql.connector import MySQLConnection
from mysql.connector import Error

def load_data_to_mysql(mysql_config, input_path, sheet):

    # Defining variables
    mysql_table = sheet['mysql_table']
    columns = sheet['columns']
    
    try:
        logging.info(f"Loading data to MySQL table: {mysql_table}")
        df = pd.read_csv(input_path)
        if df.empty:
            logging.info(f"No new data to insert into {mysql_table}")
            return

        # Connect to MySQL
        conn = mysql.connector.connect(**mysql_config)
        cursor = conn.cursor(dictionary=True)

        # Prepare INSERT statement dynamically
        columns_str = ", ".join(columns)
        placeholders = ", ".join(["%s"] * len(columns))
        insert_query = f"INSERT INTO {mysql_table} ({columns_str}) VALUES ({placeholders})"

        # Prepare data for executemany
        data_to_insert = [tuple(row) for row in df.replace({pd.NA: None}).to_numpy()]
        if data_to_insert:
            cursor.executemany(insert_query, data_to_insert)
            conn.commit()
            logging.info(f"Inserted {cursor.rowcount} rows into {mysql_table}")
        else:
            logging.info(f"No data to insert into {mysql_table}")

        cursor.close()
        conn.close()
    except Exception as e:
        logging.error(f"Load error: {e}")
        raise
import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import logging
import mysql.connector
from mysql.connector import Error
from sqlalchemy import create_engine
import os

def fetch_data():
    try:
        BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
        CREDENTIALS_PATH = os.path.join(BASE_DIR,'formularios' ,'credentials', 'credentials.json')
        scope = ["https://www.googleapis.com/auth/spreadsheets"]
        
        creds = Credentials.from_service_account_file(CREDENTIALS_PATH, scopes=scope)
        client = gspread.authorize(creds)

        sheet = client.open_by_key("10KvoAnAGuDSTGPY_uwQrKzmzxK_ODQVyoqlx79Bn6cY").worksheet("main")
        data = sheet.get_all_records()
        df = pd.DataFrame(data)
        return df
    except gspread.exceptions.APIError as e:
        logging.error(f"Google Sheets API error: {e}")
        raise
    except Exception as e:
        print(f"Error fetching data: {e}")
        raise
print(fetch_data())

def Transform_Data_1():
    try:
        mysql_config = {
            'user': 'root',
            'password': '123',
            'host': 'localhost',
            'database': 'formularios'
            }
        

        conn = mysql.connector.connect(**mysql_config)
        cursor = conn.cursor(dictionary=True)
        cursor.execute(f"SELECT * FROM checklistveiculos;")
        """db = cursor.fetchall()
        columns = [column[0] for column in cursor.description]"""
        df_sql = pd.DataFrame(cursor.fetchall())

        if df_sql.empty:
            logging.info("No data found in MySQL table 'checklistveiculos'.")
            cursor.close()
            conn.close()
            return pd.DataFrame()
        else:
            cursor.close()
            conn.close()
            return df_sql
    except Exception as e:
        logging.error(f"Error transforming data: {e}")
        raise 

mysql_config = {
    'user': 'root',
    'password': '123',
    'host': 'localhost',
    'database': 'formularios'
}
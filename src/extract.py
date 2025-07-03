import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import logging

def fetch_data(credentials_path, sheet, output_path):

    # Defining variables
    sheet_id = sheet['sheet_id']
    worksheet_name = sheet['worksheet_name']
    
    try:
        logging.info(f"Extracting data from sheet ID: {sheet_id}, worksheet: {worksheet_name}")
        scope = ["https://www.googleapis.com/auth/spreadsheets"]
        creds = Credentials.from_service_account_file(credentials_path, scopes=scope)
        client = gspread.authorize(creds)

        sheet = client.open_by_key(sheet_id).worksheet(worksheet_name)
        data = sheet.get_all_records()
        df = pd.DataFrame(data)
        """df = df.drop(columns=[col for col in df.columns if col in uncolumns])"""
        df.to_csv(output_path, index=False)
        return output_path
    except gspread.exceptions.APIError as e:
        logging.error(f"Google Sheets API error: {e}")
        raise
    except Exception as e:
        print(f"Error fetching data: {e}")
        raise

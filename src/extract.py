import gspread
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
import pandas as pd
import logging


# ===============================================================================================================================================================
#                                                       CARREGAR OS DADOS DA PLANILHA                                                                          #
# ===============================================================================================================================================================
def fetch_data(gcp_conn_id: str, sheet_config: dict) -> pd.DataFrame:

    # Definir variaveis.
    sheet_id = sheet_config["sheet_id"]
    worksheet_name = sheet_config["worksheet_name"]

    try:
        # Conectar a Planilha Google
        logging.info(
            f"Obtendo credenciais do Google a partir da conexão: {gcp_conn_id}"
        )
        gcp_hook = GoogleBaseHook(gcp_conn_id=gcp_conn_id)
        creds = gcp_hook.get_credentials()
        client = gspread.authorize(creds)

        # Abrir a planilha ,a aba específica e transferir os dados para o formato CSV.
        logging.info(
            f"Extracting data from sheet ID: {sheet_id}, worksheet: {worksheet_name}"
        )
        sheet = client.open_by_key(sheet_id).worksheet(worksheet_name)
        data = sheet.get_all_records()
        df = pd.DataFrame(data)

        # Saída do resultado para o próximo passo.
        for col in df.columns:
            if df[col].dtype == "object":
                df[col] = (
                    df[col].astype(str).replace("nan", "")
                )  # Converte e limpa possíveis 'nan' de texto
        return df
    except gspread.exceptions.APIError as e:
        logging.error(f"Google Sheets API error: {e}")
        raise
    except Exception as e:
        print(f"Error fetching data: {e}")
        raise

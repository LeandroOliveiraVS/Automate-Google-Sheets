import logging

import gspread
import pandas as pd
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook


# ==================================================================
#                   CARREGAR OS DADOS DA PLANILHA                  #
# ==================================================================
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
        if not data:
            # Se não há dados, busca a primeira linha (cabeçalho)
            headers = sheet.row_values(1)
            logging.warning(
                f"A planilha '{worksheet_name}' não tem linhas de dados. Criando DataFrame vazio com os cabeçalhos: {headers}"
            )
            # Cria um DataFrame vazio, mas com as colunas corretas
            df = pd.DataFrame(columns=headers)
        else:
            # Se há dados, o comportamento continua o mesmo de antes
            df = pd.DataFrame(data)

        logging.info(f"Colunas: {df.columns}, linhas: {len(df)}")

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

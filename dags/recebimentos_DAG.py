import pendulum
import json
import pandas as pd
from airflow.decorators import dag, task

import extract
import transform
import load
import compare
#=======================================================================================================================================#

CONFIG_FILE_PATH = "/opt/airflow/config/sheets_config.json"

#=======================================================================================================================================#

@dag(
    dag_id='processar_recebimentos_final',
    description='Processa dados do Google Sheets para o MySQL',
    schedule='@daily',
    start_date=pendulum.datetime(2025, 7, 9, tz="America/Recife"),
    catchup=False,
    tags=['sheets', 'mysql']
)

def dag_processar_recebimentos():

    @task
    def get_sheets_config() -> dict:
        with open(CONFIG_FILE_PATH, 'r') as f:
            sheets_config = json.load(f)

        return sheets_config[0]

    @task
    def extract_data_task(sheet_config: dict) -> pd.DataFrame:
        df_extraido = extract.fetch_data(
            gcp_conn_id='google_cloud_default',
            sheet_config = sheet_config
        )
        
        return df_extraido
    
    @task
    def transform_data_task(sheet_config: dict, df_extraido: pd.DataFrame) -> pd.DataFrame:
        df_transformado = transform.Transform_Data_1(
            sheet_config = sheet_config,
            df_extraido = df_extraido
        )

        return df_transformado
    
    @task
    def compare_data_task(mssql_conn_id: str, sheet_config: dict, df_transformado: pd.DataFrame) -> pd.DataFrame:
        df_final = compare.compare_data(
            mssql_conn_id = mssql_conn_id,
            sheet_config = sheet_config,
            df_transformado = df_transformado
        )

        return df_final
    
    @task
    def load_data_task(mssql_conn_id: str, sheet_config: dict, df_final: pd.DataFrame):
        load.load_data_to_mssql(
            mssql_conn_id = mssql_conn_id,
            sheet_config = sheet_config,
            df_final = df_final
        )

# --- ORQUESTRAÇÃO DAS TAREFAS ---
    config = get_sheets_config()
    extracted_df = extract_data_task(sheet_config=config)
    transformed_df = transform_data_task(df_extraido=extracted_df, sheet_config=config)
    final_df = compare_data_task(
        mssql_conn_id="mssql_default", 
        sheet_config=config,
        df_transformado=transformed_df
    )
    load_data_task(
        mssql_conn_id="mssql_default", 
        sheet_config=config,
        df_final=final_df
    )

# Instancia a DAG para que o Airflow possa encontrá-la
dag_processar_recebimentos()
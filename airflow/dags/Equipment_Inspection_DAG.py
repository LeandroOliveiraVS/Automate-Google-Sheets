from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import json
import os
from src.extract import fetch_data
from src.transform import Transform_Data_1
from src.load import load_data_to_mysql
from src.compare import compare_data
#=======================================================================================================================================#
default_args = {
    'owner': 'user',
    'depends_on_past': False,
    'email_on_failure': False,
    'email': ['your_email@email.com']
}

# MySQL configuration
mysql_config = {
    'host': 'localhost',
    'user': 'root',
    'password': 'your_password',
    'database': 'your_database'
}

# Load Configuration
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
CONFIG_PATH = os.path.join(BASE_DIR, 'config', 'sheets_config.json')
CREDENTIALS_PATH = os.path.join(BASE_DIR, 'credentials', 'credentials.json')
TEMP_DIR = os.path.join(BASE_DIR, 'tmp')

with open(CONFIG_PATH, 'r') as f:
    sheets_config = json.load(f)
    sheet = sheets_config[0]
#=======================================================================================================================================#
with DAG(
    dag_id='Equipament_inspection_dag',
    default_args=default_args,
    description='DAG to process Equipment Inspection data from Google Sheets to MySQL',
    schedule='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False
) as dag:
    
    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=fetch_data,
        op_kwargs={
            'credentials_path': CREDENTIALS_PATH,
            'sheet': sheet,
            'worksheet_name': sheet['worksheet_name'],
            'output_path': os.path.join(TEMP_DIR, 'Equipment_Inspection.csv')
        }
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=Transform_Data_1,
        op_kwargs={
            'input_path': os.path.join(TEMP_DIR, 'Equipment_Inspections.csv'),
            'sheet': sheet,
            'output_path': os.path.join(TEMP_DIR, 'transformed_Equipment_Inspection.csv')
        }
    )

    compare_task = PythonOperator(
        task_id='compare_data',
        python_callable=compare_data,
        op_kwargs={
            'mysql_config': mysql_config,
            'input_path': os.path.join(TEMP_DIR, 'transformed_Equipment_Inspection.csv'),
            'sheet': sheet,
            'output_path': os.path.join(TEMP_DIR, 'missing_data_cEquipment_Inspection.csv')
        }
    )

    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data_to_mysql,
        op_kwargs={
            'mysql_config': mysql_config,
            'input_path': os.path.join(TEMP_DIR, 'missing_data_Equipment_Inspection.csv'),
            'sheet': sheet
        }
    )

    cleanup_task = BashOperator(
        task_id='cleanup_temp_files',
        bash_command=f'rm -rf {TEMP_DIR}/*'
    )

    extract_task >> transform_task >> compare_task >> load_task >> cleanup_task

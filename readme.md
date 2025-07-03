# ETL Pipeline Project Setup

This repository contains an ETL (Extract, Transform, Load) pipeline using Apache Airflow to process data from Google Sheets and load it into a PostgreSQL or MySQL database. Below are the step-by-step instructions to set up the environment on a Ubuntu-based system.

## Prerequisites
- A machine with Ubuntu (e.g., 20.04 or 22.04).
- Git installed (`sudo apt-get install git`).
- Access to a GitHub account for version control.
- Basic knowledge of terminal commands.
- Credentials from Google Sheets API on folder 'credentials'

## Installation and Configuration Steps

### 1. Update and Install Dependencies
Update the package list and install required system packages:

-bash
sudo apt-get update && sudo apt-get upgrade -y
sudo apt-get install python3 python3-pip python3-venv -y
sudo apt-get install -y postgresql postgresql-contrib python3-pip

### 2. Set Up Virtual Environment and Install Python Packages
Create and activate a virtual environment, then install the necessary Python packages:
python3 -m venv .venv
source .venv/bin/activate
pip install "apache-airflow==3.0.2" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.0.2/constraints-3.12.txt"
pip install --upgrade google-api-python-client google-auth-httplib2 google-auth-oauthlib gspread
pip install psycopg2-binary
pip install mysql.connector
pip install mysql-connector-python
pip install pandas
pip install apache-airflow-providers-fab

### 3. Configure Airflow Home Directory
Set the AIRFLOW_HOME environment variable to store Airflow configurations and data:

export AIRFLOW_HOME=~/your_folder/airflow
echo "export AIRFLOW_HOME=~/your_folder/airflow" >> ~/.bashrc
source ~/.bashrc

### 4. Set Up PostgreSQL Database
Configure a PostgreSQL database for Airflow metadata:

-bash
sudo -u postgres psql -c "CREATE DATABASE airflow;"
sudo -u postgres psql -c "CREATE USER airflow WITH PASSWORD 'airflow';"
sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;"
sudo -u postgres psql -c "ALTER DATABASE airflow OWNER TO airflow;"

### 5. Initialize Airflow Database
Initialize Airflow database for the first time:

-bash
airflow db migrate

Once executed, this code will create the necessary files and folders to airflow.
### 6. Configure airflow.cfg
Edit the folowing lines in airflow.cfg file (located in ~/repos/your_folder/airflow/airflow.cfg) 
to use FAB authentication and disable examples and use PostgreSQL to airflow persistent metadata:

auth_manager
auth_manager = airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager
sql_alchemy_conn = postgresql+psycopg2://airflow:airflow@localhost/airflow
load_examples = False

### 7. Start Airflow Standalone
Run Airflow in standalone mode to start the webserver, scheduler, and other components:

-bash
(with the virtual environment)
airflow standalone

## New Sheet

In order to set up a new sheet, add the credential's email to the sheet, and in sheets_config.json, create a net item inside
the [], follow the examples to know what to set into the json file, then create the DAG into the DAGs folder.
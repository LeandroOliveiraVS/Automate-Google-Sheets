from airflow.providers.google.common.hooks.base_google import GoogleBaseHook

def get_creds():
    hook = GoogleBaseHook(gcp_conn_id="google_sheets_conn")
    creds = hook.get_credentials()
    print("Credenciais do Google carregadas com sucesso a partir da Conexão Airflow!")
    return creds
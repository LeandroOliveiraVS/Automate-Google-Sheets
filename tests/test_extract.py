import pandas as pd
import pytest
from src.extract import fetch_data

def test_fetch_data(mocker):

    # ---- Parte 1: Simular o Airflow GoogleBaseHook ----

    # Criamos um "dublê" para as credenciais do Google
    mock_credentials = mocker.MagicMock()

    # Criamos um "dublê" para a instância do Hook
    mock_hook_instance = mocker.MagicMock()

    # Dizemos a ele: "Quando o método .get_credentials() for chamado, retorne nosso objeto de credenciais falso"
    mock_hook_instance.get_credentials.return_value = mock_credentials

    # Interceptamos a CLASSE GoogleBaseHook dentro do arquivo extract.
    # Agora, toda vez que seu código fizer 'GoogleBaseHook(...)', ele não criará um objeto real.
    mock_google_hook = mocker.patch('src.extract.GoogleBaseHook', autospec=True)

    # Retorna a instancia dublê do Hook quando for chamado
    mock_google_hook.return_value = mock_hook_instance

    # ---- Parte 2: Simular o GSpread ----

    dados_falsos_da_planilha = [{'col1': 'dadoA', 'col2': 1}, {'col1': 'dadoB', 'col2': 2}]

    mock_worksheet = mocker.MagicMock()
    mock_worksheet.get_all_records.return_value = dados_falsos_da_planilha

    mock_gspread_client = mocker.MagicMock()
    mock_gspread_client.open_by_key.return_value.worksheet.return_value = mock_worksheet

    # Interceptamos a função 'gspread.authorize' e forçamos ela a retornar nosso cliente falso
    mock_gspread_authorize = mocker.patch('src.extract.gspread.authorize', return_value=mock_gspread_client)

    # ---- Parte 3: Configuração do teste ----
    sheet_config_teste = {'sheet_id': 'id_teste', 'worksheet_name': 'aba_teste'}

    # ---- Parte 4: Executar a função que queremos testar ----
    df_resultado = fetch_data(
        gcp_conn_id="google_cloud_teste",
        sheet_config=sheet_config_teste
    )

    assert isinstance(df_resultado, pd.DataFrame)
    assert len(df_resultado) == 2
    assert df_resultado['col2'].iloc[1] == 2

    # As nossas simulações foram chamadas corretamente?
    # O Hook foi instanciado com o conn_id correto?
    mock_google_hook.assert_called_once_with(gcp_conn_id="google_cloud_teste")
    # O método get_credentials foi chamado?
    mock_hook_instance.get_credentials.assert_called_once()
    # O gspread.authorize foi chamado com as credenciais falsas que o nosso Hook falso forneceu?
    mock_gspread_authorize.assert_called_once_with(mock_credentials)
    # A planilha foi aberta com o ID correto?
    mock_gspread_client.open_by_key.assert_called_once_with('id_teste')
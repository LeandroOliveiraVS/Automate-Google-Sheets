import pandas as pd
from src.load import load_data_to_mssql

def test_load_data(mocker):
    # Simulando o DataFrame final
    df_final = pd.DataFrame({
        'key_column': ['2023-01-01', '2023-01-02', '2023-01-03'],
        'dados': [1, 2, 3],
        'dados2': [4, 5, None]
    })

    # Simulando as configurações da planilha
    sheet_config = {
        'table': 'test_table',
        'key_column': 'key_column'
    }

    # Simulando o hook do Airflow
    mock_hook_instance = mocker.MagicMock()
    mock_mssql_hook_class = mocker.patch('src.load.MsSqlHook', autospec=True)
    mock_mssql_hook_class.return_value = mock_hook_instance

    # Executando a def de load_data_to_mssql
    load_data_to_mssql(
        mssql_conn_id='mssql_test_conn',
        sheet_config=sheet_config,
        df_final=df_final
    )

    # Verificações da função

    # O Hook foi instanciado com o ID de conexão correto?
    mock_mssql_hook_class.assert_called_once_with(mssql_conn_id='mssql_test_conn')

    # O método 'insert_rows' do hook foi chamado exatamente uma vez?
    mock_hook_instance.insert_rows.assert_called_once()

    # Verificação de argumentos passados.
    call_args, call_kwargs = mock_hook_instance.insert_rows.call_args

    # O nome da tabela está correto?
    assert call_kwargs['table'] == 'test_table'

    # O número de linhas a serem inseridas está correto?
    assert len(call_kwargs['rows']) == 3

    # Dados esperados a serem inseridos na tabela mssql
    dados_esperados = [
        ['2023-01-01', 1, 4.0],
        ['2023-01-02', 2, 5.0],
        ['2023-01-03', 3, None]
    ]

    # Os dados enviados para o hook estão corretos, incluindo o None?
    assert call_kwargs['rows'] == dados_esperados

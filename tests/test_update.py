import pandas as pd
from src.update import update_mysql_from_file

def test_update_data(mocker):

    # Simulando o DataFrame
    df_transformado = pd.DataFrame({
        'id_registro': ['2025-01-01', '2025-01-02', '2025-01-03'],
        'dados': [1, 2, 3],
        'dados2': [4, 5, 6]
    })

    # Simulando as configurações da planilha
    sheet_config = sheet_config = {
        'table': 'test',
        'key_column': 'id_registro'
    }

    # Simulando a engine do Airflow
    mock_engine = mocker.MagicMock()
    # Simulando a instancia do Airflow
    mock_hook_instance = mocker.MagicMock()
    mock_hook_instance.get_sqlalchemy_engine.return_value = mock_engine
    # Interceptar a criação da classe MsSqlHook
    mock_mssql_hook_class = mocker.patch('src.update.MsSqlHook', autospec=True)
    mock_mssql_hook_class.return_value = mock_hook_instance
    # Interceptar o método pandas.DataFrame.to_sql para não tentar escrever em um banco real
    mock_to_sql = mocker.patch('pandas.DataFrame.to_sql')

    # Executando a função a testar
    update_mysql_from_file(
        mssql_conn_id='mssql_test_conn',
        sheet_config=sheet_config,
        df_transformado=df_transformado
    )

    # Verificações da função

    # O Hook foi instanciado com o ID de conexão correto ?
    mock_mssql_hook_class.assert_called_once_with(mssql_conn_id='mssql_test_conn')

    # A função tentou carregar o DataFrame para uma tabela de staging?
    mock_to_sql.assert_called_once()

    # Inspecionando a chamada ao to_sql para mais detalhes
    call_args, call_kwargs = mock_to_sql.call_args
    assert call_kwargs['con'] == mock_engine
    # Nome da tabela passada a chamada
    table_name_passed_to_sql = call_args[0]
    assert 'staging_test' in table_name_passed_to_sql # Verifica se o nome da tabela de staging está correto

    # O método 'run' do hook foi chamado duas vezes (uma para MERGE, outra para DROP)?
    assert mock_hook_instance.run.call_count == 2

    # Inspeciona a primeira chamada ao .run() - a query MERGE
    merge_query_call = mock_hook_instance.run.call_args_list[0]
    merge_sql_query = merge_query_call.args[0]

    # A query MERGE foi construída corretamente?
    assert "MERGE INTO [test] AS T" in merge_sql_query
    assert "USING [staging_test_" in merge_sql_query
    assert "ON (T.[id_registro] = S.[id_registro])" in merge_sql_query
    assert "WHEN MATCHED THEN" in merge_sql_query
    assert "UPDATE SET T.[dados] = S.[dados], T.[dados2] = S.[dados2]" in merge_sql_query
    assert "WHEN NOT MATCHED BY TARGET THEN" in merge_sql_query
    assert "INSERT ([id_registro], [dados], [dados2])" in merge_sql_query
    assert "VALUES (S.[id_registro], S.[dados], S.[dados2]);" in merge_sql_query

    # Inspeciona a segunda chamada ao .run() - a query DROP TABLE
    drop_query_call = mock_hook_instance.run.call_args_list[1]
    drop_sql_query = drop_query_call.args[0]
    
    assert "DROP TABLE IF EXISTS [staging_test_" in drop_sql_query



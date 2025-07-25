import pandas as pd

from src.update import update_sql_mysql


def test_update_sql_mysql_com_staging(mocker):
    """
    Testa a função de atualização para MySQL, verificando se as queries
    de UPDATE e INSERT são construídas corretamente.
    """
    # 1. Arrange (Organizar): Preparamos os dados e os mocks
    df_para_atualizar = pd.DataFrame(
        {"id_registro": ["2025-01-01", "2025-01-02"], "dados": ["A", "B_novo"]}
    )

    sheet_config = {
        "table": "test_mysql",
        "key_column": "id_registro",
        "update_cols": ["dados"],
    }

    # Simular o SQLAlchemy engine retornado pelo MySqlHook
    mock_engine = mocker.MagicMock()
    # Simular a instância do MySqlHook
    mock_mysql_hook_instance = mocker.MagicMock()
    mock_mysql_hook_instance.get_sqlalchemy_engine.return_value = mock_engine

    # Interceptar a criação da classe MySqlHook
    mock_mysql_hook_class = mocker.patch("src.update.MySqlHook", autospec=True)
    mock_mysql_hook_class.return_value = mock_mysql_hook_instance

    # Interceptar o método pandas.DataFrame.to_sql
    mock_to_sql = mocker.patch("pandas.DataFrame.to_sql")

    # 2. Act: Executamos a função específica do MySQL
    update_sql_mysql(  # Certifique-se de que este é o nome correto da sua função MySQL
        mysql_conn_id="mysql_test_conn",
        sheet_config=sheet_config,
        df_transformado=df_para_atualizar,
    )

    # 3. Assert (Afirmar): Verificamos o comportamento da função MySQL

    # O Hook do MySQL foi instanciado corretamente?
    mock_mysql_hook_class.assert_called_once_with(mysql_conn_id="mysql_test_conn")

    # A função tentou carregar os dados para a tabela de staging?
    mock_to_sql.assert_called_once()

    # O método 'run' foi chamado duas vezes (UPDATE, DROP)?
    assert mock_mysql_hook_instance.run.call_count == 2

    # Inspeciona a primeira chamada: UPDATE JOIN
    update_query_call = mock_mysql_hook_instance.run.call_args_list[0]
    update_sql_query = update_query_call.args[0]

    # A query UPDATE foi construída corretamente para MySQL?
    assert "UPDATE `test_mysql` T JOIN `staging_test_mysql_" in update_sql_query
    assert "ON T.`id_registro` = S.`id_registro`" in update_sql_query
    assert "SET T.`dados` = S.`dados`" in update_sql_query
    assert (
        "WHERE NOT (T.`dados` <=> S.`dados`)" in update_sql_query
    )  # Verifica a cláusula WHERE

    # Inspeciona a segunda chamada: DROP TABLE
    drop_query_call = mock_mysql_hook_instance.run.call_args_list[1]
    drop_sql_query = drop_query_call.args[0]

    assert "DROP TABLE IF EXISTS `staging_test_mysql_" in drop_sql_query

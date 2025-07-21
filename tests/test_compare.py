import pandas as pd

from src.compare import compare_data


def test_compare_data(mocker):
    # Simulando um DataFrame transformado
    df_transformado = pd.DataFrame(
        {"key_column": ["2023-01-01", "2023-01-02", "2023-01-03"], "dados": [1, 2, 3]}
    )

    # Simulando a configuração da planilha
    sheet_config = {"table": "test_table", "key_column": "key_column"}

    # Simulando o hook do Airflow
    mock_hook_instance = mocker.MagicMock()
    mock_hook_instance.get_first.return_value = ("2023-01-01",)

    mock_mssql_hook_class = mocker.patch("src.compare.MsSqlHook", autospec=True)
    mock_mssql_hook_class.return_value = mock_hook_instance

    # Executando a função que queremos testar
    new_data_df = compare_data(
        mssql_conn_id="mssql_test_conn",
        sheet_config=sheet_config,
        df_transformado=df_transformado,
    )

    # Verificar se o DataFrame retornado contém apenas os novos dados
    assert isinstance(new_data_df, pd.DataFrame)
    # Deve conter os dados de 2023-01-02 e 2023-01-03
    assert len(new_data_df) == 2
    # Verificar se a coluna chave foi convertida para datetime
    assert pd.api.types.is_datetime64_any_dtype(new_data_df["key_column"])

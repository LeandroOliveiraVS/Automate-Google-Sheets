import pandas as pd

from src.transform import Transform_Data_1


def test_transform_data():
    # Simulando um dataframe de entrada
    df_extraido = pd.DataFrame(
        {
            "key_column": ["01/01/2023", "02/01/2023", None],
            "colunas_manter": [1, 2, 3],
            "uncolumn": [4, 5, 6],
        }
    )

    # simulando a configuração da planilha
    sheet_config = {
        "table": "test_table",
        "key_column": "key_column",
        "uncolumns": ["uncolumn"],
        "dates": ["key_column"],
        "hours": [],
    }

    # Chama a função de transformação
    transformed_df = Transform_Data_1(df_extraido, sheet_config)

    # Verifica se o DataFrame transformado tem as colunas corretas
    assert "key_column" in transformed_df.columns
    # Verifica se o retorno é um DataFrame
    assert isinstance(transformed_df, pd.DataFrame)
    # Verifica se uma das linhas foi removida devido ao valor None na coluna chave
    assert len(transformed_df) == 2
    # Verifica se as colunas não desejadas foram removidas
    assert "uncolumn" not in transformed_df.columns
    # Verifica se a coluna chave foi convertida para datetime
    assert transformed_df["key_column"].iloc[0] == "2023-01-01"
    assert transformed_df["key_column"].iloc[1] == "2023-01-02"

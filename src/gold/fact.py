import pandas as pd


def build_fatos_vendas(
    silver_df: pd.DataFrame,
    dim_vendedor: pd.DataFrame,
    dim_administradora: pd.DataFrame,
    dim_tempo: pd.DataFrame
) -> pd.DataFrame:

    print("silver_df:", type(silver_df))
    print("dim_vendedor:", type(dim_vendedor))
    print("dim_administradora:", type(dim_administradora))
    print("dim_tempo:", type(dim_tempo))


    df = silver_df.merge(
        dim_vendedor,
        on="idpessoavendedor",
        how="left"
    )

    if df["sk_vendedor"].isnull().any():
        raise ValueError("Erro no merge da dimensão vendedor.")


    df = df.merge(
        dim_administradora,
        on="administradora",
        how="left"
    )

    if df["sk_administradora"].isnull().any():
        raise ValueError("Erro no merge da dimensão administradora.")


    df = df.merge(
        dim_tempo,
        on="datavenda",
        how="left"
    )

    if df["sk_tempo"].isnull().any():
        raise ValueError("Erro no merge da dimensão tempo.")


    fato = df[[
        "sk_vendedor",
        "sk_administradora",
        "sk_tempo",
        "valorcredito",
        "valorprimeiraparcela",
        "hash_linha"
    ]].copy()


    fato["quantidade"] = 1


    if fato["hash_linha"].duplicated().any():
        raise ValueError("Duplicidade detectada na fato.")

    return fato

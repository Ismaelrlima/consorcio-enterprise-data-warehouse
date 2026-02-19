from gold.dimensions import (
    build_dim_vendedor,
    build_dim_administradora,
    build_dim_tempo
)

from gold.fact import build_fato_vendas

def silver_to_gold(silver_df):

    dim_vendedor = build_dim_vendedor(silver_df)
    dim_administradora = build_dim_administradora(silver_df)
    dim_tempo = build_dim_tempo(silver_df)

    fato = build_fato_vendas(
        silver_df,
        dim_vendedor,
        dim_administradora,
        dim_tempo
    )

    return {
        "dim_vendedor": dim_vendedor,
        "dim_administradora": dim_administradora,
        "dim_tempo": dim_tempo,
        "fato_vendas": fato
    }

from silver.silver_layer import orquestracao_silver
import pandas as pd
import numpy

def build_dim_vendedor(silver_df: pd.DataFrame) -> pd.DataFrame:

    dim = silver_df[[
        "idpessoavendedor",
        "vendedor"
    ]].drop_duplicates()

    dim.reset_index(drop=True, inplace=True)
    #SK
    dim["sk_vendedor"] = dim.index + 1

    dim = dim[[
        "sk_vendedor",
        "idpessoavendedor",
        "vendedor"
    ]]

    return dim

def build_dim_administradora(silver_df: pd.DataFrame) -> pd.DataFrame:

    dim = silver_df[[
        "idconsorcioadministradorabase",
        "administradora"
    ]].drop_duplicates()

    dim.reset_index(drop=True, inplace=True)

    dim["sk_administradora"] = dim.index + 1

    dim = dim [[
        "sk_administradora",
        "idconsorcioadministradorabase",
        "administradora"
    ]]

    return dim

def build_dim_tempo(silver_df: pd.DataFrame) -> pd.DataFrame:

    dim = silver_df[[
        "datavenda"
    ]].drop_duplicates()

    dim['ano'] = dim['datavenda'].dt.year
    dim['mes'] = dim['datavenda'].dt.month
    dim['dia'] = dim['datavenda'].dt.day
    dim['dia_da_semana'] = dim['datavenda'].dt.dayofweek
    dim['trimestre'] = dim['datavenda'].dt.quarter

    dim.reset_index(drop=True, inplace=True)

    dim["sk_tempo"] = dim.index + 1

    dim = dim[[
        "sk_tempo",
        "datavenda",
        "ano",
        "mes",
        "dia",
        "dia_da_semana",
        "trimestre"
    ]]

    return dim
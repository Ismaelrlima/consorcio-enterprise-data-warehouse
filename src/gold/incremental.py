import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv


load_dotenv()


def get_engine():

    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    db = os.getenv("DB_NAME")

    connection_string = (
        f"postgresql://{user}:{password}@{host}:{port}/{db}"
    )

    engine = create_engine(connection_string)

    return engine


def load_to_staging(df: pd.DataFrame):
    """
    Recebe o DataFrame da Gold (fato já estruturada)
    e carrega na tabela staging.

    Essa tabela funciona como área temporária
    antes do merge definitivo.
    """

    engine = get_engine()

    df.to_sql(
        name="fato_vendas_staging",
        con=engine,
        schema="gold",
        if_exists="append",
        index=False,
        method="multi"
    )

    print("Carga para STAGING concluída com sucesso.")


def merge_fato():

    engine = get_engine()

    # SQL puro para execução direta no banco
    merge_query = text("""
        INSERT INTO gold.fato_vendas (
            sk_vendedor,
            sk_administradora,
            sk_tempo,
            valorcredito,
            valorprimeiraparcela,
            quantidade,
            hash_linha
        )
        SELECT
            sk_vendedor,
            sk_administradora,
            sk_tempo,
            valorcredito,
            valorprimeiraparcela,
            quantidade,
            hash_linha
        FROM gold.fato_vendas_staging

        ON CONFLICT (hash_linha) DO NOTHING;

        -- Limpa staging após merge
        TRUNCATE TABLE gold.fato_vendas_staging;
    """)


    with engine.begin() as connection:
        connection.execute(merge_query)

    print("Merge incremental concluído com sucesso.")



def incremental_load_pipeline(df_gold: pd.DataFrame):

    print("Iniciando carga incremental de alto volume...")

    load_to_staging(df_gold)
    merge_fato()

    print("Processo incremental finalizado.")

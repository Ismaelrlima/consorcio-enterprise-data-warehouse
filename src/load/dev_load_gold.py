import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

def get_engine():
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    db = os.getenv("DB_NAME")

    connection_string = f"postgresql://{user}:{password}@{host}:{port}/{db}"
    
    return create_engine(connection_string)


def load_dataframe(df: pd.DataFrame, table_name: str):

    engine = get_engine()

    df.to_sql(
        name=table_name,
        con=engine,
        schema="gold",
        if_exists="append",
        index=False
    )

    print(f"Tabela {table_name} carregada com sucesso.")

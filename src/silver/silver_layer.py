import pandas as pd
import datetime as dt
import hashlib

def bronze_to_silver(bronze_record: dict) -> pd.DataFrame:

    if not bronze_record['payload']:
        raise ValueError('Payload Retornou vazio')


    data = bronze_record['payload']
    df = pd.DataFrame(data)
    
    df['uuid'] = bronze_record['uuid']
    df['ingestion_timestamp'] = bronze_record['ingestion_timestamp']

    return df

def tratamentos_silver(df: pd.DataFrame) -> pd.DataFrame:

    mapeamento = {
    "empresa" : "empresa",
    "vendedor" : "vendedor",
    "datavenda" : "datavenda", 
    "administradora" : "administradora",
    "idconsorcioadministradorabase" : "idconsorcioadministradorabase",
    "idpessoa" : "idpessoa",
    "idpessoavendedor" : "idpessoavendedor", 
    "administradorareduzida" : "administradorareduzida",
    "contrato" : "contrato",
    "pessoa" : "pessoa",
    "modelo" : "modelo",
    "prazo" : "prazo",
    "diavencimento" : "diavencimento",
    "municipio" : "municipio",
    "numeroproposta" : "numeroproposta" ,
    "planovenda" : "planovenda",
    "produtoconsorcio" : "produtoconsorcio",
    "taxalicenciamento" : "taxalicenciamento",
    "siglauf" : "siglauf",
    "valorcredito" : "valorcredito",
    "valorprimeiraparcela" : "valorprimeiraparcela"

    }

    df.rename(columns=mapeamento, inplace=True)

    df['datavenda'] = pd.to_datetime(df['datavenda'], format='%Y-%m-%dT%H:%M:%S', errors='coerce')
    df['diavencimento'] = pd.to_datetime(df['diavencimento'], format='%Y-%m-%dT%H:%M:%S', errors='coerce')

    return df

#teste de função -> foi discartada para entrar uma função vetorizada por motivos de performaçe
"""def gerar_hash(df: pd.DataFrame) -> pd.DataFrame:
    
    hash_cols = ["empresa","datavenda","numeroproposta","contrato","idpessoa","idpessoavendedor","modelo","valorcredito","valorprimeiraparcela"]

    for col in hash_cols:
        if col not in df:
            raise ValueError(f'A coluna ={col}= não existe no DataFrame')

    def calcular_hash(linha):
        valores = [str(linha[col]) for col in hash_cols]

        concat = "|".join(valores)

        return hashlib.sha256(concat.encode('utf-8')).hexdigest()

    df['hash_linha'] = df.apply(calcular_hash, axis=1)

    return df"""



def gerar_hash_vetorizado(df: pd.DataFrame, hash_cols: list) -> pd.Series:
    
    for col in hash_cols:
        if col not in df.columns:
            raise ValueError(f'A coluna ={col}= não existe no DataFrame')
    
    df_hash = df[hash_cols].copy()

    datetime_cols = df_hash.select_dtypes(include=["datetime64[ns]"]).columns
    for col in datetime_cols:
        df_hash[col] = df_hash[col].dt.strftime('%Y-%m-%d %H:%M:%S')

    concat_series = (
        df_hash
        .fillna('')
        .astype(str)
        .agg('|'.join, axis=1)
    )

    return concat_series.map(
        lambda x: hashlib.sha256(x.encode('utf-8')).hexdigest()
    )


def orquestracao_silver(bronze_record: dict) -> pd.DataFrame:

    df = bronze_to_silver(bronze_record)

    df = tratamentos_silver(df)

    hash_cols = ["empresa","datavenda","numeroproposta","contrato","idpessoa","idpessoavendedor","modelo","valorcredito","valorprimeiraparcela"]
    df['hash_linha'] = gerar_hash_vetorizado(df, hash_cols)

    df = df.drop_duplicates(subset=['hash_linha'])

    return df
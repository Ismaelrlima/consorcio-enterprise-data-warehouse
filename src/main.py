import os
import time
from dotenv import load_dotenv

import ingestion.microwork_api as api
import utils.extrator as extrator
import bronze.bronze_layer as bronze
import silver.silver_layer as silver
from silver.silver_layer import orquestracao_silver
from gold.fact import build_fatos_vendas
from gold.incremental import incremental_load_pipeline
from load.dev_load_gold import load_dataframe
from utils.logging_config import setup_logger
from gold.dimensions import (
    build_dim_vendedor,
    build_dim_administradora,
    build_dim_tempo
)




load_dotenv()

logger = setup_logger()

ENVIRONMENT = os.getenv("ENVIRONMENT", "DEV")

DATA_INICIAL = os.getenv("DATA_INICIAL", "2026-02-01")
DATA_FINAL = os.getenv("DATA_FINAL", "2026-02-28")



def run_pipeline():

    start_time = time.time()

    logger.info("========================================")
    logger.info("INICIANDO PIPELINE MICROWORK DW")
    logger.info(f"Ambiente: {ENVIRONMENT}")
    logger.info(f"Período: {DATA_INICIAL} até {DATA_FINAL}")

    try:



        filtros_body = [
            f"DataInicial={DATA_INICIAL}",
            "Administradora=150,163,1072",
            "Novo=True",
            f"DataFinal={DATA_FINAL}",
            "Reposicao=True",
            "PontoVenda=null",
            "Vendedor=null",
            "Supervisor=null",
            "Gerente=null",
            "SituacaoContrato=null",
            "Modelo=null",
            "NaoRecebidoPrimeiraParcela=True",
            "PrimeiraParcelaRecebida=True",
            "RecebidoCartaoAdm=True",
            "NaoRecebidoCartaoAdm=True",
            "Municipio=null",
            "NaoRemessa=True",
            "Remessa=True",
            "NaoPagamentoAdministradora=True",
            "PagamentoAdministradora=True"
        ]

        filtros_str = "; ".join(filtros_body)

        body = {
            "idrelatorioconfiguracao": 194,
            "idrelatorioconsulta": 97,
            "idrelatorioconfiguracaoleiaute": 194,
            "idrelatoriousuarioleiaute": 417,
            "ididioma": 1,
            "listaempresas": [12, 13, 18],
            "filtros": filtros_str
        }

        logger.info("Filtros montados com sucesso.")


        logger.info("Iniciando extração da API...")

        dados = api.buscar_dados_microwork(body)

        logger.info("Extração concluída.")

        primeira_lista = extrator.extrair_primeira_lista(dados)

        camada_bronze = bronze.camada_bronze(primeira_lista)

        logger.info(f"Bronze gerada com {len(camada_bronze)} registros.")



        logger.info("Iniciando transformação Silver...")

        camada_silver = silver.orquestracao_silver(camada_bronze)

        logger.info(f"Silver gerada com {len(camada_silver)} registros.")

        logger.info("Construindo dimensões...")

        dim_vendedor = build_dim_vendedor(camada_silver)
        dim_administradora = build_dim_administradora(camada_silver)
        dim_tempo = build_dim_tempo(camada_silver)

        logger.info("Dimensões construídas com sucesso.")


        logger.info("Construindo fato na camada Gold...")


        fato_df = build_fatos_vendas(
            camada_silver,
            dim_vendedor=dim_vendedor,
            dim_administradora=dim_administradora,
            dim_tempo=dim_tempo
        )

        logger.info(f"Fato construída com {len(fato_df)} registros.")


        logger.info("Iniciando carga no banco...")

        if ENVIRONMENT == "DEV":
            logger.info("Usando carga DEV (append simples).")
            load_dataframe(fato_df, "fato_vendas")

        else:
            logger.info("Usando carga PROD (incremental alto volume).")
            incremental_load_pipeline(fato_df)

        logger.info("Carga finalizada com sucesso.")

    except Exception as e:

        logger.error("Erro durante execução do pipeline.")
        logger.exception(e)
        raise

    finally:

        end_time = time.time()
        total_time = round(end_time - start_time, 2)

        logger.info(f"Tempo total de execução: {total_time} segundos")
        logger.info("PIPELINE FINALIZADO")
        logger.info("========================================")



if __name__ == "__main__":
    run_pipeline()

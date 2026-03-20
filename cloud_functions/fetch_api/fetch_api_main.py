"""
Cloud Function: fetch-api-breweries
Camada: Bronze
Descrição:
    Extrai dados paginados da API Open Brewery DB e persiste o resultado
    como CSV na camada Bronze do Data Lake no Google Cloud Storage.

Destino GCS:
    gs://{BUCKET_NAME}/raw/breweries/{YYYY-MM}/open_brewery_{date}_{timestamp}.csv

Logs de execução:
    gs://{BUCKET_NAME}/logs/{YYYY-MM}/log_{timestamp}.txt

Variáveis de ambiente obrigatórias:
    BUCKET_NAME (str): Nome do bucket GCS onde os dados serão persistidos.

Dependências:
    functions-framework, requests, pandas, google-cloud-storage
"""

import requests
import logging
import sys
import os
import io
import time
import pandas as pd
from datetime import datetime, date
from google.cloud import storage
from google.api_core import exceptions
import functions_framework

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

API_BASE_URL = "https://api.openbrewerydb.org/v1/breweries"
RAW_PREFIX = "raw/breweries"
LOGS_PREFIX = "logs"


def fetch_api_data(base_url: str, limit: int = 200) -> list[dict]:
    """
    Coleta todos os registros da API Open Brewery DB via paginação.

    Itera página a página até receber uma resposta vazia, indicando fim
    dos dados. Aguarda 1 segundo entre páginas para respeitar rate limits
    da API. Lança exceção em qualquer falha HTTP, timeout ou erro inesperado,
    permitindo que o caller trate e registre o erro adequadamente.

    Args:
        base_url (str): URL base da API, sem parâmetros de paginação.
                        Exemplo: "https://api.openbrewerydb.org/v1/breweries"
        limit (int): Quantidade de registros por página. Padrão: 200
                     (máximo suportado pela API Open Brewery DB).

    Returns:
        list[dict]: Lista com todos os registros retornados pela API.
                    Cada dicionário representa uma cervejaria com campos
                    como id, name, brewery_type, city, state, country, etc.

    Raises:
        requests.exceptions.HTTPError: Quando a API retorna status 4xx ou 5xx.
        requests.exceptions.Timeout: Quando a requisição excede 20 segundos.
        Exception: Para qualquer outro erro inesperado durante a coleta.
    """
    page = 1
    all_data = []

    while True:
        url = f"{base_url}?page={page}&per_page={limit}"
        try:
            response = requests.get(url, timeout=20)
            response.raise_for_status()
            records = response.json()

            if not records:
                logger.info("Sem mais registros. Paginação encerrada.")
                break

            all_data.extend(records)
            logger.info(f"Página {page} coletada. Total até agora: {len(all_data)}")
            page += 1
            time.sleep(1)

        except requests.exceptions.HTTPError as e:
            logger.error(f"Erro HTTP na página {page}: {e}")
            raise
        except requests.exceptions.Timeout as e:
            logger.error(f"Timeout na página {page}: {e}")
            raise
        except Exception as e:
            logger.error(f"Erro inesperado na página {page}: {e}")
            raise

    return all_data


def normalize_data(raw_data: list[dict]) -> pd.DataFrame:
    """
    Normaliza a lista de dicionários retornados pela API em um DataFrame pandas.

    Converte todos os campos string para o tipo str, substituindo valores
    None pelo string vazio para evitar problemas de tipagem no CSV.
    Converte longitude e latitude para float (pd.to_numeric com coerce),
    resultando em NaN para valores inválidos ou ausentes.

    """
    df = pd.DataFrame(raw_data)

    str_cols = [
        "id",
        "name",
        "brewery_type",
        "address_1",
        "address_2",
        "address_3",
        "city",
        "state_province",
        "postal_code",
        "country",
        "longitude",
        "latitude",
        "phone",
        "website_url",
        "state",
        "street",
    ]
    for col in str_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).replace("None", "")

    for coord in ["longitude", "latitude"]:
        if coord in df.columns:
            df[coord] = pd.to_numeric(df[coord], errors="coerce")

    logger.info(f"Dados normalizados. Shape: {df.shape}")
    return df


def upload_to_bucket(
    bucket_name: str,
    file_content: io.StringIO,
    destination_blob_name: str,
    storage_client: storage.Client,
) -> dict:
    """
    Faz upload de conteúdo CSV para um blob no Google Cloud Storage.

    Usa upload_from_string para evitar escrita em disco local, o que é
    essencial em ambientes serverless como Cloud Functions.

    Args:
        bucket_name (str): Nome do bucket GCS de destino.
        file_content (io.StringIO): Buffer em memória com o conteúdo CSV.
                                    Deve ter sido posicionado em seek(0) pelo caller.
        destination_blob_name (str): Caminho completo do blob no bucket.
                                     Exemplo: "raw/breweries/2025-01/open_brewery_2025-01-15_20250115-060000.csv"
        storage_client (storage.Client): Cliente autenticado do GCS.

    Returns:
        dict: {"status": "success"} em caso de sucesso,
              {"status": "error", "message": str} em caso de falha.
    """
    try:
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_string(file_content.getvalue(), content_type="text/csv")
        logger.info(f"Upload bem-sucedido: gs://{bucket_name}/{destination_blob_name}")
        return {"status": "success"}
    except exceptions.GoogleCloudError as e:
        logger.error(f"Erro ao fazer upload: {e}")
        return {"status": "error", "message": str(e)}


def upload_log_to_bucket(
    bucket_name: str,
    log_content: str,
    log_path: str,
    storage_client: storage.Client,
) -> None:
    """
    Salva o log de execução da Cloud Function como arquivo .txt no GCS.

    Chamada tanto em caso de sucesso quanto de falha, garantindo rastreabilidade
    completa de todas as execuções. Em caso de erro no próprio upload do log,
    registra no logger local mas não relança a exceção para não mascarar
    erros anteriores.

    Args:
        bucket_name (str): Nome do bucket GCS de destino.
        log_content (str): Conteúdo textual do log capturado durante a execução.
        log_path (str): Caminho do blob de log no bucket.
                        Exemplo: "logs/2025-01/log_20250115-060000.txt"
        storage_client (storage.Client): Cliente autenticado do GCS.

    """
    try:
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(log_path)
        blob.upload_from_string(log_content, content_type="text/plain")
        logger.info(f"Log salvo em: gs://{bucket_name}/{log_path}")
    except Exception as e:
        logger.error(f"Erro ao salvar log: {e}")


@functions_framework.http
def open_brewery_extracao(request):
    """
    Entrypoint HTTP da Cloud Function de extração Bronze.

    Orquestra o fluxo completo de extração:
      1. Valida a variável de ambiente BUCKET_NAME.
      2. Chama fetch_api_data() para coletar todos os registros da API.
      3. Chama normalize_data() para normalizar os dados em DataFrame.
      4. Serializa o DataFrame como CSV em memória (sem escrita em disco).
      5. Faz upload do CSV para o GCS na camada Bronze.
      6. Persiste o log de execução no GCS independentemente do resultado.

    O log é gravado em todas as situações (sucesso ou falha), garantindo
    rastreabilidade completa. O timestamp no nome do arquivo garante que
    múltiplas execuções no mesmo dia não se sobrescrevam.

    Args:
        request: Objeto de requisição HTTP do Flask/functions-framework.
                 Não utiliza body ou query params — toda a configuração
                 vem de variáveis de ambiente.

    Returns:
        tuple[str, int]:
            - ("Arquivo gerado e enviado com sucesso.", 200) em caso de sucesso.
            - ("Variável de ambiente BUCKET_NAME ausente.", 500) se BUCKET_NAME não estiver definido.
            - ("Falha ao obter dados da API.", 500) se a coleta falhar.
            - ("Nenhum dado retornado da API.", 500) se a API retornar lista vazia.
            - ("Erro no processamento dos dados.", 500) se normalização ou upload falhar.

    GCS Output:
        gs://{BUCKET_NAME}/raw/breweries/{YYYY-MM}/open_brewery_{date}_{timestamp}.csv
        gs://{BUCKET_NAME}/logs/{YYYY-MM}/log_{timestamp}.txt
    """
    log_buffer = io.StringIO()
    log_handler = logging.StreamHandler(log_buffer)
    log_handler.setFormatter(
        logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    )
    logger.addHandler(log_handler)

    logger.info(f"Início da execução: {datetime.utcnow()}")

    bucket_name = os.getenv("BUCKET_NAME")
    if not bucket_name:
        logger.error("Variável de ambiente BUCKET_NAME está ausente.")
        return ("Variável de ambiente BUCKET_NAME ausente.", 500)

    storage_client = storage.Client()
    data_hoje = date.today()
    ano_mes = data_hoje.strftime("%Y-%m")
    timestamp = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    log_path = f"{LOGS_PREFIX}/{ano_mes}/log_{timestamp}.txt"

    logger.info("Iniciando coleta da API Open Brewery DB.")
    try:
        raw_data = fetch_api_data(API_BASE_URL)
    except Exception as e:
        logger.error(f"Falha crítica ao coletar dados: {e}")
        upload_log_to_bucket(
            bucket_name, log_buffer.getvalue(), log_path, storage_client
        )
        logger.removeHandler(log_handler)
        return ("Falha ao obter dados da API.", 500)

    if not raw_data:
        logger.warning("Nenhum dado retornado da API.")
        upload_log_to_bucket(
            bucket_name, log_buffer.getvalue(), log_path, storage_client
        )
        logger.removeHandler(log_handler)
        return ("Nenhum dado retornado da API.", 500)

    logger.info("Normalizando dados.")
    try:
        df_final = normalize_data(raw_data)
        csv_buffer = io.StringIO()
        df_final.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)

        destino = f"{RAW_PREFIX}/{ano_mes}/open_brewery_{data_hoje}_{timestamp}.csv"
        result = upload_to_bucket(bucket_name, csv_buffer, destino, storage_client)

        if result["status"] != "success":
            raise Exception(result.get("message", "Erro desconhecido no upload."))

    except Exception as e:
        logger.error(f"Erro durante normalização ou upload: {e}")
        upload_log_to_bucket(
            bucket_name, log_buffer.getvalue(), log_path, storage_client
        )
        logger.removeHandler(log_handler)
        return ("Erro no processamento dos dados.", 500)

    upload_log_to_bucket(bucket_name, log_buffer.getvalue(), log_path, storage_client)
    logger.removeHandler(log_handler)
    logger.info("Execução finalizada com sucesso.")
    return ("Arquivo gerado e enviado com sucesso.", 200)

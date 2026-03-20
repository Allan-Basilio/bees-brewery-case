"""
DAG: breweries_pipeline
Camada: Orquestração
Descrição:
    Pipeline Medallion completo para ingestão e transformação de dados
    da Open Brewery DB API no Google Cloud Platform.

    Fluxo:
        1. [Bronze]  Cloud Function extrai a API e persiste CSV no GCS.
        2. [Sensor]  Valida presença do arquivo CSV na camada Bronze.
        3. [Silver]  Dataproc Serverless transforma e persiste em Parquet particionado.
        4. [Sensor]  Valida presença dos dados na camada Silver.
        5. [Gold]    Dataproc Serverless agrega e persiste visão analítica.
        6. [Sensor]  Valida presença dos dados na camada Gold.
        7. [Infra]   Cloud Function desliga a VM GCE do Airflow.

    Configurações:
        - Schedule: diário às 06h (horário de Brasília)
        - Retries: 3 tentativas por task com intervalo de 5 minutos
        - Catchup: desabilitado (executa apenas o intervalo atual)
        - Max active runs: 1 (evita execuções paralelas conflitantes)
        - Shutdown: executado sempre ao final (trigger_rule=all_done),
          garantindo que a VM seja desligada mesmo em caso de falha

    Variáveis
        PROJECT_ID   : ID do projeto GCP
        REGION       : Região dos recursos (ex: "southamerica-east1")
        BUCKET_NAME  : Nome do bucket GCS do Data Lake
        FETCH_URL    : URL da Cloud Function fetch-api-breweries
        SHUTDOWN_URL : URL da Cloud Function shutdown-vm-breweries

    Autenticação:
        As chamadas HTTP às Cloud Functions usam Identity Token gerado via
        google.oauth2.id_token, garantindo que apenas a Service Account
        da VM Airflow possa invocar as funções.

Dependências:
    apache-airflow, apache-airflow-providers-google, requests, google-auth
"""

from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateBatchOperator,
)
from airflow.providers.google.cloud.sensors.gcs import (
    GCSObjectsWithPrefixExistenceSensor,
)
from airflow.operators.python import PythonOperator
from airflow.models import Variable

from pendulum import timezone
from datetime import datetime, date, timedelta
import requests
import google.oauth2.id_token
from google.auth.transport.requests import Request

local_tz = timezone("America/Sao_Paulo")

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

data_hoje = date.today()
ano_mes = data_hoje.strftime("%Y-%m")


PROJECT_ID = "PROJECT_ID"
REGION = "REGION"
BUCKET_NAME = "BUCKET_NAME"

FETCH_URL = "FETCH_URL"  # Endpoint da CloudFunction que faz o consumo da API

SHUTDOWN_URL = "SHUTDOWN_URL"  # Endpoint da CloudFunction que desliga a VM


SILVER_SCRIPT = f"gs://{BUCKET_NAME}/spark_jobs/silver_breweries.py"
GOLD_SCRIPT = f"gs://{BUCKET_NAME}/spark_jobs/gold_breweries.py"


# Funções Python das tasks


def call_fetch_api() -> None:
    """
    Invoca a Cloud Function de ingestão Bronze via HTTP autenticado.
    """
    auth_req = Request()
    id_token = google.oauth2.id_token.fetch_id_token(auth_req, FETCH_URL)
    resp = requests.post(
        FETCH_URL,
        headers={
            "Authorization": f"Bearer {id_token}",
            "Content-Type": "application/json",
        },
        json={},
        timeout=1200,
    )
    print(resp.status_code, resp.text)
    if resp.status_code != 200:
        raise Exception(f"Cloud Function retornou {resp.status_code}: {resp.text}")


def call_shutdown_vm() -> None:
    """
    Invoca a Cloud Function de desligamento da VM GCE via HTTP autenticado.
    """
    auth_req = Request()
    id_token = google.oauth2.id_token.fetch_id_token(auth_req, SHUTDOWN_URL)
    resp = requests.post(
        SHUTDOWN_URL,
        headers={
            "Authorization": f"Bearer {id_token}",
            "Content-Type": "application/json",
        },
        json={},
        timeout=30,
    )
    print(resp.status_code, resp.text)


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------
with DAG(
    dag_id="breweries_pipeline",
    default_args=default_args,
    schedule="0 6 * * *",
    start_date=datetime(2026, 18, 3, tzinfo=local_tz),
    catchup=False,
    max_active_runs=1,
    description="Pipeline Medallion: Bronze → Silver → Gold | Open Brewery DB",
    tags=["breweries", "medallion", "dataproc"],
) as dag:

    # 1. Bronze: Cloud Function extrai a API e persiste CSV no GCS
    trigger_fetch_api = PythonOperator(
        task_id="trigger_fetch_api",
        python_callable=call_fetch_api,
        execution_timeout=timedelta(minutes=20),
    )

    # 2. Sensor: valida presença do CSV Bronze antes de iniciar a Silver
    # poke_interval=30s, timeout=600s — se o arquivo não aparecer em 10 min,
    # a task falha e aciona os retries do default_args.
    check_ingest = GCSObjectsWithPrefixExistenceSensor(
        task_id="check_ingest",
        bucket=BUCKET_NAME,
        prefix=f"raw/breweries/{ano_mes}/",
        poke_interval=30,
        timeout=600,
    )

    # 3. Silver: Dataproc Serverless executa silver_layer.py
    # Sem cluster fixo — Dataproc provisiona recursos sob demanda e
    # desaloca ao término do job, eliminando custo ocioso.
    run_silver = DataprocCreateBatchOperator(
        task_id="run_silver",
        project_id=PROJECT_ID,
        region=REGION,
        batch={
            "pyspark_batch": {
                "main_python_file_uri": SILVER_SCRIPT,
            }
        },
    )

    # 4. Sensor: valida presença dos dados Silver antes de iniciar a Gold
    check_silver = GCSObjectsWithPrefixExistenceSensor(
        task_id="check_silver",
        bucket=BUCKET_NAME,
        prefix="trusted/breweries/",
        poke_interval=30,
        timeout=600,
    )

    # 5. Gold: Dataproc Serverless executa gold_layer.py
    run_gold = DataprocCreateBatchOperator(
        task_id="run_gold",
        project_id=PROJECT_ID,
        region=REGION,
        batch={
            "pyspark_batch": {
                "main_python_file_uri": GOLD_SCRIPT,
            },
        },
    )

    # 6. Sensor: valida presença dos dados Gold antes do shutdown
    check_gold = GCSObjectsWithPrefixExistenceSensor(
        task_id="check_gold",
        bucket=BUCKET_NAME,
        prefix="gold/breweries/",
        poke_interval=30,
        timeout=600,
    )

    # 7. Shutdown: desliga a VM GCE do Airflow ao final do pipeline.
    shutdown_vm = PythonOperator(
        task_id="shutdown_vm",
        python_callable=call_shutdown_vm,
        execution_timeout=timedelta(minutes=5),
        trigger_rule="all_done",
    )

    # Dependências do DAG
    (
        trigger_fetch_api
        >> check_ingest
        >> run_silver
        >> check_silver
        >> run_gold
        >> check_gold
        >> shutdown_vm
    )

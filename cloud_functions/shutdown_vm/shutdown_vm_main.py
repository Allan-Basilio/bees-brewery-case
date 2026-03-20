"""
Cloud Function: shutdown-vm-breweries
Camada: Infraestrutura / Controle de Custos
Descrição:
    Desliga a VM GCE onde o Apache Airflow está rodando ao final do pipeline
    de breweries. Acionada como última task do DAG via PythonOperator,
    com trigger_rule="all_done" — garante execução independentemente de
    sucesso ou falha das tasks anteriores, evitando VMs ociosas.

Variáveis de ambiente:
    PROJECT_ID : ID do projeto GCP.
    VM_NAME   : Nome da instância GCE a ser desligada.
    ZONE      : Zona GCP da instância.

Dependências:
    functions-framework, google-cloud-compute
"""

import os
import logging
import functions_framework
from google.cloud import compute_v1

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


@functions_framework.http
def shutdown_vm(request):
    """
    Entrypoint HTTP da Cloud Function de desligamento da VM GCE.

    Lê as configurações da instância via variáveis de ambiente e invoca
    a API do Compute Engine para desligar a VM de forma assíncrona.
    O retorno da operação contém o nome da operation GCP, que pode ser
    monitorado via Cloud Console ou `gcloud compute operations describe`.

    A função é projetada para ser chamada com autenticação via Identity Token
    (Bearer token), gerado pelo Airflow com google.oauth2.id_token antes
    de cada requisição HTTP.

    Args:
        request: Objeto de requisição HTTP do Flask/functions-framework.
                 Não utiliza body ou query params — toda a configuração
                 vem de variáveis de ambiente.
    """
    project_id = "PROJECT_ID"
    vm_name = "VM_NAME"
    zone = "ZONE"

    logger.info(f"Desligando  VM: {vm_name} (projeto={project_id}, zona={zone})")

    try:
        instances_client = compute_v1.InstancesClient()
        operation = instances_client.stop(
            project=project_id,
            zone=zone,
            instance=vm_name,
        )
        return (f"VM {vm_name} está sendo desligada. Operation: {operation.name}", 200)

    except Exception as e:
        logger.error(f"Erro ao desligar VM {vm_name}: {e}")
        return (f"Erro ao desligar VM: {str(e)}", 500)

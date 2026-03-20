"""
Spark Job: silver_breweries.py
Camada: Silver (Trusted)
Descrição:
    Lê o CSV da camada Bronze referente ao mês corrente, aplica transformações
    de limpeza e tipagem (TRIM + CAST), adiciona colunas de controle e persiste
    os dados em formato Parquet particionado por country e state na camada Silver.

    Além dos dados transformados, o job gera e persiste:
      - Metadados de qualidade por coluna (nulos, cardinalidade, tipo)
      - Dicionário de dados com descrição semântica de cada campo
      - Controle de volumetria em modo append para auditoria histórica

Origem GCS:
    gs://{BUCKET_NAME}/raw/breweries/ref_partition={YYYYMM}/

Destino GCS:
    gs://{BUCKET_NAME}/silver/breweries/country={}/state={}/
    gs://{BUCKET_NAME}/metadados/silver/silver_breweries/
    gs://{BUCKET_NAME}/controle/silver/silver_breweries/  (append)

Variáveis de ambiente:
    PROJECT_ID : ID do projeto GCP.
    LOCATION   : Região GCP.

Dependências:
    pyspark, google-cloud-storage, pytz
"""

from google.cloud import storage
import pytz
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType
from pyspark.sql import functions as F
import os
from datetime import datetime
import builtins

# Datas de referência
agora = datetime.now(pytz.timezone("America/Sao_Paulo"))
dthproc = agora.strftime("%Y%m%d%H%M%S")  # Timestamp completo para controle
_DIA_PROC_ = agora.strftime("%Y%m%d")  # Data do processamento (YYYYMMDD)
_REF_PROC_ = agora.strftime("%Y%m")  # Referência mensal da partição (YYYYMM)

# Parâmetros de paths no GCS
bucket_lake = "BUCKET_LAKE_NAME"
bucket_raw = "BUCKET_RAW_NAME"
bucket_silver = "BUCKET_SILVER_NAME"
bucket_control = "BUCKET_SILVER_CONTROL_NAME"
bucket_metadados_silver = "BUCKET_METADADOS_SILVER_NAME"
output = "silver_breweries"

project_id = "PROJECT_ID"
location = "LOCATION"

# Spark Session
spark = SparkSession.builder.appName("Silver Breweries").getOrCreate()

# Leitura da Bronze (partição do mês corrente)
path_dados = f"gs://{bucket_lake}/{bucket_raw}"

df_breweries = spark.read.csv(f"{path_dados}/{_REF_PROC_}")

df_breweries.createOrReplaceTempView("df_breweries")

qtd = df_breweries.count()
qtd_cols = len(df_breweries.columns)
print(f"Bronze lida: {qtd} linhas, {qtd_cols} colunas")

if qtd == 0:
    raise ValueError(
        f"Nenhum registro na partição ref_partition={_REF_PROC_}. Abortando."
    )

# Transformação principal
# Aplicação de TRIM em todos os campos string para remover espaços residuais
# vindos da API e CAST explícito para garantir schema consistente entre execuções.
# Campos de controle (ts_proc, ts_proc_partition) são adicionados para rastreabilidade.
lake = spark.sql(
    """
    SELECT
        ref,
        ref_partition,
        {dthproc} AS ts_proc,
        {dthproc} AS ts_proc_partition,
        CAST(TRIM(id)             AS STRING) AS id,
        CAST(TRIM(name)           AS STRING) AS name,
        CAST(TRIM(brewery_type)   AS STRING) AS brewery_type,
        CAST(TRIM(address_1)      AS STRING) AS address_1,
        CAST(TRIM(address_2)      AS STRING) AS address_2,
        CAST(TRIM(address_3)      AS STRING) AS address_3,
        CAST(TRIM(city)           AS STRING) AS city,
        CAST(TRIM(state_province) AS STRING) AS state_province,
        CAST(TRIM(postal_code)    AS STRING) AS postal_code,
        CAST(TRIM(country)        AS STRING) AS country,
        CAST(longitude            AS DOUBLE) AS longitude,
        CAST(latitude             AS DOUBLE) AS latitude,
        CAST(TRIM(phone)          AS STRING) AS phone,
        CAST(TRIM(website_url)    AS STRING) AS website_url,
        CAST(TRIM(state)          AS STRING) AS state,
        CAST(TRIM(street)         AS STRING) AS street
    FROM df_breweries
    """.format(
        dthproc=dthproc
    )
)
lake.createOrReplaceTempView("lake")
lake.cache()

qtd = lake.count()
print(f"Volumetria após transformação: {qtd} linhas")

if qtd == 0:
    raise ValueError("Nenhum registro após transformação Silver. Abortando.")

# Escrita em Parquet particionado por country e state

path_silver = f"gs://{bucket_lake}/{bucket_silver}"
print(f"Escrevendo Silver em: {path_silver}")

lake.coalesce(2).write.partitionBy("country", "state").parquet(
    path_silver, mode="overwrite"
)
print("Silver layer salva com sucesso.")

# Geração de metadados de qualidade por coluna
# Para cada coluna do DataFrame transformado, calcula:
#   - Tipo de dado (Spark simpleString)
#   - Quantidade de valores nulos
#   - Percentual de nulos sobre o total de registros
#   - Cardinalidade (quantidade de valores distintos)
total_rows = lake.count()
metadata_list = []

for col_name in lake.columns:
    try:
        col_type = lake.schema[col_name].dataType.simpleString()

        if "." in col_name or " " in col_name:
            col_ref = F.col(f"`{col_name}`")
        else:
            col_ref = F.col(col_name)

        null_count = lake.filter(col_ref.isNull()).count()
        percent_nulls = (
            builtins.round((null_count / total_rows) * 100, 2)
            if total_rows > 0
            else 0.0
        )
        cardinality = lake.select(col_name).distinct().count()
        metadata_list.append(
            (col_name, col_type, null_count, percent_nulls, cardinality)
        )
    except Exception as e:
        print(f"Erro ao processar coluna {col_name}: {e}")

metadata_schema = StructType(
    [
        StructField("coluna", StringType(), True),
        StructField("tipo", StringType(), True),
        StructField("qt_nulos", LongType(), True),
        StructField("percent_nulos", DoubleType(), True),
        StructField("cardinalidade", LongType(), True),
    ]
)
metadata_df = spark.createDataFrame(metadata_list, metadata_schema).orderBy(
    F.desc("percent_nulos")
)
metadata_df.createOrReplaceTempView("metadata_df")

# Estrutura da tabela (schema + zona)
metadados = [(col, tipo, output, "silver") for col, tipo in lake.dtypes]
schema_structure = StructType(
    [
        StructField("coluna", StringType(), nullable=False),
        StructField("tipo", StringType(), nullable=False),
        StructField("tabela", StringType(), nullable=False),
        StructField("zona", StringType(), nullable=False),
    ]
)
metadata_structure = spark.createDataFrame(data=metadados, schema=schema_structure)
metadata_structure.createOrReplaceTempView("metadata_structure")

# Dicionário de dados
desc_colunas = [
    ("ref", "Referência da data de processamento no formato YYYYMMDD."),
    ("ref_partition", "Referência do ano/mês da partição no formato YYYYMM."),
    ("ts_proc", "Timestamp de processamento do job Spark."),
    ("ts_proc_partition", "Timestamp de processamento para partição."),
    (
        "id",
        "Identificador único da cervejaria (UUID). Convertido para STRING com TRIM.",
    ),
    ("name", "Nome da cervejaria. Convertido para STRING com TRIM."),
    ("brewery_type", "Tipo da cervejaria. Convertido para STRING com TRIM."),
    ("address_1", "Linha principal do endereço. Convertido para STRING com TRIM."),
    (
        "address_2",
        "Segunda linha do endereço (pode ser nulo). Convertido para STRING com TRIM.",
    ),
    (
        "address_3",
        "Terceira linha do endereço (pode ser nulo). Convertido para STRING com TRIM.",
    ),
    ("city", "Nome da cidade. Convertido para STRING com TRIM."),
    ("state_province", "Estado ou província. Convertido para STRING com TRIM."),
    ("postal_code", "CEP ou código postal. Convertido para STRING com TRIM."),
    ("country", "Nome do país. Convertido para STRING com TRIM."),
    ("longitude", "Coordenada geográfica de longitude. Convertido para DOUBLE."),
    ("latitude", "Coordenada geográfica de latitude. Convertido para DOUBLE."),
    ("phone", "Número de telefone de contato. Convertido para STRING com TRIM."),
    ("website_url", "URL do site da cervejaria. Convertido para STRING com TRIM."),
    ("state", "Nome do estado. Convertido para STRING com TRIM."),
    ("street", "Endereço da rua. Convertido para STRING com TRIM."),
]
schema_dic = StructType(
    [
        StructField("coluna", StringType(), nullable=False),
        StructField("descricao", StringType(), nullable=False),
    ]
)
dic_structure = spark.createDataFrame(data=desc_colunas, schema=schema_dic)
dic_structure.createOrReplaceTempView("dicionario_dados")

# Join final de metadados e escrita no GCS
meta_join = spark.sql(
    """
    SELECT
        a.zona,
        a.tabela,
        lower(a.coluna) AS coluna,
        a.tipo,
        b.descricao,
        c.qt_nulos,
        c.percent_nulos,
        c.cardinalidade
    FROM metadata_structure AS a
    LEFT JOIN dicionario_dados AS b ON lower(a.coluna) = lower(b.coluna)
    LEFT JOIN metadata_df      AS c ON lower(a.coluna) = lower(c.coluna)
"""
)
meta_join.cache()
meta_join.count()

path_metadados = f"gs://{bucket_lake}/{bucket_metadados_silver}/{output}"
print(f"Salvando metadados em: {path_metadados}")
meta_join.coalesce(1).write.parquet(path_metadados, mode="overwrite")

# Controle de volumetria
controle = spark.sql(
    """
    SELECT
        '{tb}' AS name_file,
        '{tb}' AS name_file_partition,
        ref_partition,
        ts_proc,
        ts_proc_partition,
        COUNT(*) AS qtd_registros
    FROM lake
    GROUP BY 1, 2, 3, 4, 5
    ORDER BY 1, 2, 3, 4, 5
    """.format(
        tb=output
    )
)
path_control = f"gs://{bucket_lake}/{bucket_control}/{output}"
print(f"Salvando controle em: {path_control}")
controle.coalesce(1).write.parquet(path_control, mode="append")

print("Silver Breweries finalizado")

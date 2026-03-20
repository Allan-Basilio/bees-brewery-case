"""
Spark Job: gold_breweries.py
Camada: Gold (Analítica)
Descrição:
    Lê os dados da camada Silver (Parquet particionado), aplica agregação
    analítica para calcular a quantidade de cervejarias por tipo e localização,
    e persiste o resultado na camada Gold.

    Além da visão agregada, o job gera e persiste:
      - Metadados de qualidade por coluna (nulos, cardinalidade, tipo)
      - Dicionário de dados com descrição semântica de cada campo
      - Controle de volumetria em modo append para auditoria histórica

Origem GCS:
    gs://{BUCKET_NAME}/trusted/breweries/

Destino GCS:
    gs://{BUCKET_NAME}/gold/breweries/                      (overwrite)
    gs://{BUCKET_NAME}/metadados/gold/gold_breweries/       (overwrite)
    gs://{BUCKET_NAME}/controle/gold/gold_breweries/        (append)

Variáveis de ambiente:
    BUCKET_NAME : Nome do bucket GCS.

Dependências:
    pyspark, pytz
"""

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
bucket_trusted = "BUCKET_SILVER_NAME"
bucket_gold = "BUCKET_GOLD_NAME"
bucket_control = "BUCKET_GOLD_CONTROL_NAME"
bucket_metadados_gold = "BUCKET_METADADOS_GOLD_NAME"
output = "gold_breweries"

# Spark Session
spark = SparkSession.builder.appName("Gold Breweries").getOrCreate()

# Leitura da Silver
path_dados = f"gs://{bucket_lake}/{bucket_trusted}"

df_breweries = spark.read.parquet(path_dados)
df_breweries.createOrReplaceTempView("df_breweries")

qtd = df_breweries.count()
qtd_cols = len(df_breweries.columns)
print(f"Silver lida: {qtd} linhas, {qtd_cols} colunas")

if qtd == 0:
    raise ValueError("Nenhum registro na Silver layer. Abortando Gold.")

# ---------------------------------------------------------------------------
# Agregação
# Calcula a quantidade de cervejarias agrupada por:
#   - brewery_type: tipo da cervejaria (micro, nano, regional, brewpub, etc.)
#   - city:         cidade onde a cervejaria está localizada
#   - state_province: estado ou província
#   - country:      país
#
# Resultado ordenado por brewery_count DESC para facilitar análises
# de concentração geográfica e por tipo.
#
# ts_proc e ref_partition são adicionados como colunas de controle
# para rastreabilidade e particionamento histórico.
lake = spark.sql(
    """
    SELECT
        brewery_type,
        city,
        state_province,
        country,
        COUNT(*)   AS brewery_count,
        {dthproc}  AS ts_proc,
        {ref_proc} AS ref_partition
    FROM df_breweries
    GROUP BY brewery_type, city, state_province, country
    ORDER BY brewery_count DESC
    """.format(
        dthproc=dthproc, ref_proc=_REF_PROC_
    )
)
lake.createOrReplaceTempView("lake")
lake.cache()

qtd_gold = lake.count()
print(f"Registros na Gold (grupos distintos): {qtd_gold}")

if qtd_gold == 0:
    raise ValueError("Nenhum registro na agregação Gold. Abortando.")

lake.show(10, False)


# Escrita em Parquet

path_gold = f"gs://{bucket_lake}/{bucket_gold}"
print(f"Escrevendo Gold em: {path_gold}")

lake.coalesce(1).write.parquet(path_gold, mode="overwrite")
print("Gold layer salva com sucesso.")

# ---------------------------------------------------------------------------
# Geração de metadados de qualidade por coluna
# ---------------------------------------------------------------------------
# Para cada coluna do DataFrame agregado, calcula:
#   - Tipo de dado
#   - Quantidade de valores nulos
#   - Percentual de nulos sobre o total de registros
#   - Cardinalidade (quantidade de valores distintos)
total_rows = lake.count()
metadata_list = []

for col_name in lake.columns:
    try:
        col_type = lake.schema[col_name].dataType.simpleString()
        col_ref = (
            F.col(f"`{col_name}`")
            if "." in col_name or " " in col_name
            else F.col(col_name)
        )
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
metadados = [(col, tipo, output, "gold") for col, tipo in lake.dtypes]
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
    (
        "brewery_type",
        "Tipo da cervejaria (micro, nano, regional, brewpub, large, planning, bar, contract, proprietor, closed).",
    ),
    ("city", "Nome da cidade onde estão localizadas as cervejarias contadas."),
    (
        "state_province",
        "Estado ou província onde estão localizadas as cervejarias contadas.",
    ),
    ("country", "País onde estão localizadas as cervejarias contadas."),
    ("brewery_count", "Quantidade de cervejarias agrupadas por tipo e localização."),
    ("ts_proc", "Timestamp de processamento do job Spark que gerou o registro."),
    ("ref_partition", "Referência do ano/mês de processamento no formato YYYYMM."),
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

path_metadados = f"gs://{bucket_lake}/{bucket_metadados_gold}/{output}"
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
        COUNT(*) AS qtd_registros
    FROM lake
    GROUP BY 1, 2, 3, 4
    ORDER BY 1, 2, 3, 4
    """.format(
        tb=output
    )
)
path_control = f"gs://{bucket_lake}/{bucket_control}/{output}"
print(f"Salvando controle em: {path_control}")
controle.coalesce(1).write.parquet(path_control, mode="append")

print(" Gold Breweries finalizado com sucesso")

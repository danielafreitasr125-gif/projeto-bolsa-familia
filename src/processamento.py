from pyspark.sql import SparkSession
from pyspark.sql.functions import col regexp_replace
spark = SparkSession.builder\
.appName("Bolsa familia")\
.getOrCreate()

#caminho do arquivo
caminho_csv = "dados/pagamentos.csv"

df = spark.read\
    .option("header", True)\
    .option("inferSchema",True)\
    .option("sep", ";")\
    .option("encoding","ISO-8859-1")\
    .csv(caminho_csv)

df.show(10)

df.printSchema()

#padronizaçao de dados

df_tratado = df.withColumnRenamed(\
    "MÊS COMPETÊNCIA|","mes_competencia"
    )

#padronizaçao de dados 
df_tratado = df_tratado.withColumnRenamed(\
    "NOME FAVORECIDO","nome_favorecido"
    )

df_tratado.show()

#processamento com mais colunas
df_tratado = df

colunas_padrao = {
    "MÊS COMPETÊNCIA": "mes_competencia",
    "MÊS REFERÊNCIA": "mes_referencia", 
    "UF": "uf" , 
    "CÓDIGO MUNICÍPIO SIAFI": "codigo_municipio_siaf",
    "NOME MUNICÍPIO": "nome_municipio",
    "CPF FAVORECIDO": "cpf_favorecido",
    "NIS FAVORECIDO": "nis_favorecido",
    "NOME FAVORECIDO": "nome_favorecido",
    "VALOR PARCELA": "valor_parcela"
}

for antiga, nova in colunas_padrao.items():
    df_tratado = df_tratado.withColumnRenamed(antiga, nova)
                                  
# Tratamento Automatico

import unicodedata 
import re

def padronizar_nome(col):
    col = col.lower()
    col = unicodedata.normalize("NFD", col)
    col = col.encore("ascii", "ignore").decode("utf-8")
    col = re.sub(r"[^a-z0-9]+","_", col)
    col = col.strip("_")
    return col



df_tratado = df_tratado.todf(
    *[padronizar_nome(c) for c in df_tratado.columns]
)
df_tratado = df_tratado.withColumn(
    "valor_parcela",
    regexp_replace(
        regexp_replace(col("valor_parcela"), r"\.", ""),  # remove milhar
        ",", "."  # troca vírgula por ponto
    ).cast("decimal(10,2)")
)
df_tratado = df_tratado.dropna()

resumo = df_tratado.groupBy("UF") \
    .agg(
        _sum("VALOR_PARCELA").alias("TOTAL_PAGO"),
        count("NIS_FAVORECIDO").alias("QTD_BENEFICIARIOS")

    )
resumo.show()

# Agrupar por UF e calcular o valor medio e total das parcelas
df_tratado.agg(
    round(avg("valor_parcela"), 2).alias("VALOR_MEDIO"),
    _sum("valor_parcela").alias("VALOR_TOTAL")
).show()
# Agrupar por UF e contar a quantidade de registros
df_tratado.groupBy("UF") \
    .agg(
        _sum("VALOR_PARCELA").alias("TOTAL_PAGO")
    ).orderBy("TOTAL_PAGO",ascending=False) \
    .show(26)

ranking_favorecido = df_tratado.groupBy("cpf_favorecido","nome_favorecido")\
    .agg(
        _sum("valor_parcela").alias("total_recebido"),
        count("valor_parcela").alias("qtd_parcelas")
    ).orderBy(desc("total_recebido"))

print("Top 10 beneficiarios que mais receberam:")
ranking_favorecido.show(10, truncate=False)

media_geral = df_tratado.agg(avg("valor_parcela").alias("media_geral")).collect()[0]["media_geral"]
print(f"Valor médio geral das parcelas: R$ {media_geral:.2f}")

df_uf = df_tratado.groupBy("uf") \
    .agg(_sum("valor_parcela").alias("total_pago")) \
    .orderBy("total_pago", ascending=False) \
    .limit(10) \
    .toPandas()

# Gráfico
plt.figure()
plt.bar(df_uf["uf"], df_uf["total_pago"])
plt.title("Top 10 UFs - Total Pago Bolsa Família")
plt.xlabel("UF")
plt.ylabel("Total Pago")
plt.show()


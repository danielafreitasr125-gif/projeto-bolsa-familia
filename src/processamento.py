from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, sum, avg, count, desc, round
import matplotlib.pyplot as plt

spark = SparkSession.builder\
.appName("Bolsa familia")\
.config("spark.driver.memory", "8g")\
.config("spark.executor.memory", "8g")\
.config("spark.sql.shuffle.partitions", "50")\
.getOrCreate()

caminho_csv = "dados/pagamentos.csv"

df = spark.read\
.option("header", True )\
.option("inferSchema", True)\
.option("sep", ";")\
.option("encoding", "ISO-8859-1")\
.csv(caminho_csv)

df.show(10)
df.printSchema()

##Padronização#
df_tratado = df

df_tratado = df_tratado.withColumnRenamed(\
        "MÊS COMPETÊNCIA", "mes_competencia")\
        .withColumnRenamed(\
        "MÊS REFERÊNCIA", "mes_referencia")\
        .withColumnRenamed(\
        "UF", "uf")\
        .withColumnRenamed(\
        "CÓDIGO MUNICÍPIO SIAFI", "codigo_municipio_SIAFI")\
        .withColumnRenamed(\
        "NOME MUNICÍPIO", "nome_municipio")\
        .withColumnRenamed(\
        "CPF FAVORECIDO", "CPF_favorecido")\
        .withColumnRenamed(\
        "NIS FAVORECIDO", "NIS_favorecido")\
        .withColumnRenamed(\
        "NOME FAVORECIDO", "nome_favorecido")\
        .withColumnRenamed(\
        "VALOR PARCELA", "valor_parcela")

df_tratado.show(50)

# df_tratado = df_tratado.dropna(subset = ["valor_parcela"])
df_tratado = df_tratado.dropna()

df_tratado = df_tratado.withColumn (
    "valor_parcela",
    regexp_replace(col("valor_parcela"), ",", ".")
)

df_tratado.show(50)

df_tratado = df_tratado.withColumn (
    "valor_parcela",
    col("valor_parcela").cast("decimal(10, 2)")
)

df_tratado.show(50)
df_tratado.printSchema()

df_tratado = (
    df_tratado
    .dropna()
    .withColumn (
        "valor_parcela",
        regexp_replace(col("valor_parcela"), ",", ".")
    )
    .withColumn (
        "valor_parcela",
        col("valor_parcela").cast("decimal(10, 2)")
    )
    .withColumn (
        "valorfinal",
        col("valor_parcela").cast("decimal(10, 4)")
    )
)

df_tratado.show(28)

df_tratado.agg (
    sum("valor_parcela").alias("Total Pago"),
    round(avg("valor_parcela"), 4).alias("media_pagamento")
).show

ranking_favorecido = df_tratado.groupBy("CPF_favorecido", "nome_favorecido")\
    .agg(
        sum("valor_parcela").alias("valor_total_acumulado"),
        count("valor_parcela").alias("quantidade_parcelas")
    )\
    .orderBy(desc("valor_total_acumulado"))

print ("Ranking dos 10 Primeiros com Valores Acumulados")
ranking_favorecido.show(10, truncate = False)

media_geral = df_tratado.agg(avg("valor_parcela")).collect()[0][0]
print (f"A Média do 190 é R$:{media_geral:.2f}")

df_uf = df_tratado.groupBy("uf") \
    .agg(sum("valor_parcela").alias("total_pago")) \
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
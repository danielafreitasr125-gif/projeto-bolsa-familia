from pyspark.sql.functions import col, regexp_replace, substring

def tratar_dados (df):
    df_tratado = df

    df_tratado = df_tratado.withColumnRenamed(\
        "MÊS COMPETÊNCIA", "data_competencia")\
        .withColumnRenamed(\
        "MÊS REFERÊNCIA", "data_referencia")\
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
    
    df_tratado = df_tratado.dropna()

    df_tratado = df_tratado.withColumn (
        "valor_parcela",
        regexp_replace(col("valor_parcela"), ",", ".")
    )

    df_tratado = df_tratado.withColumn (
    "valor_parcela",
    col("valor_parcela").cast("decimal(10, 2)")
    )

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
        .withColumn (
            "ano_competencia",
            substring(col("data_competencia"),1,4)
        )
        .withColumn (
            "mes_competencia",
            substring(col("data_competencia"),5,6)
        )
        .withColumn (
            "ano_referencia",
            substring(col("data_competencia"),1,4)
        )
        .withColumn (
            "mes_referencia",
            substring(col("data_competencia"),5,6)
        )
    )

    return df_tratado
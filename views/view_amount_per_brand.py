from pyspark.sql import functions
from spark_config import spark

"""
Script para retornar o montante (valor) transacionado por bandeira e status
"""
if __name__ == '__main__':

    df1 = spark.read.csv(path="../output/sanitize_transactions/",
                         header=True,
                         inferSchema=True,
                         sep=";")

    df_brand = spark.read.csv(path="../card_brand.csv",
                              header=True,
                              inferSchema=True)

    df2 = df1.groupBy(
        functions.date_trunc("day", df1.created_at).alias("transaction_day"),
        df1.card_brand,
        df1.status
    ).agg(
        functions.sum(df1.valor).cast("decimal(15,2)").alias("valor")
    )

    df3 = df2.join(df_brand, on=df2.card_brand == df_brand.brand_code, how="left")

    df4 = df3.select(
        df3.transaction_day,
        df3.card_brand,
        df3.brand_name,
        df3.status,
        df3.valor
    ).orderBy(
        df3.transaction_day,
        functions.asc_nulls_last(df3.card_brand),
        df3.status
    )

    df4.show(100, truncate=False)
    df4.printSchema()

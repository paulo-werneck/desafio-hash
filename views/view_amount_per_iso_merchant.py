from pyspark.sql import functions
from spark_config import spark

"""
Script para retornar o montante (valor) transacionado pago por ISO / merchant, por mes 
"""
if __name__ == '__main__':

    df1 = spark.read.csv(path="../output/sanitize_transactions/",
                         header=True,
                         inferSchema=True,
                         sep=";")

    df2 = df1.where(
        df1.status == "paid"
    ).groupBy(
        functions.date_trunc("month", df1.created_at).alias("transaction_month"),
        df1.iso_id,
        df1.merchant_id
    ).agg(
        functions.sum(df1.valor).cast("decimal(15,2)").alias("valor")
    )

    df3 = df2.select(
        df2.iso_id,
        df1.merchant_id,
        df2.transaction_month,
        df2.valor
    ).orderBy(
        df2.iso_id,
        df2.transaction_month,
        df2.valor
    )

    df3.show(truncate=False)
    df3.printSchema()

from pyspark.sql import functions
from spark_config import spark

"""
Script retornar os comerciantes que tem transacionado menos durante o mes 
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
        df1.merchant_id
    ).agg(
        functions.sum(df1.valor).cast("decimal(15,2)").alias("valor")
    )

    df3 = df2.select(
        functions.concat_ws(
            "-",
            functions.year(df2.transaction_month),
            functions.month(df2.transaction_month)
        ).alias("transaction_month"),
        df1.merchant_id,
        df2.valor
    ).orderBy(
        df2.valor
    )

    df4 = df3.where(df3.valor < 10000)

    df4.show(100, truncate=False)
    df3.printSchema()

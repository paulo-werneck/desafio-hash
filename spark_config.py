from pyspark.sql import SparkSession

spark = SparkSession.\
    builder.\
    appName('SPARK - Desafio Hash - ETL transactions').\
    getOrCreate()

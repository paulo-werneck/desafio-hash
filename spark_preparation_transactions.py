import json
from pyspark.sql import functions
from spark_config import spark


def casting_fields(data_frame, json_schema):
    """
    Funcao para fazer casting dos campos dos dataframes a partir de um schema em json
    :param data_frame: data frame a ser alterado
    :param json_schema: caminho do json com o schema do dataset
    :return: novo data frame com os campos convertidos
    """

    df_header = data_frame.columns
    return data_frame.select([
        data_frame[col].cast(schema[1]).alias(schema[0])
        for schema, col in zip(json_schema.items(), df_header)
    ])


def lower_header(data_frame):
    """
    Funcao para colocar o cabecalho dos dataframes em minusculo
    :param data_frame: data frame a ser alterado
    :return: novo data frame com o cabecalho em minusculo
    """

    for col in data_frame.columns:
        data_frame = data_frame.withColumnRenamed(col, col.lower())
    return data_frame


def get_json_schema_mapping(json_schema):
    try:
        with open(json_schema, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f'Arquivo {json_schema} não encontrado')


if __name__ == '__main__':

    df = spark.read.csv(path="transactions.csv",
                        header=True,
                        inferSchema=False,
                        sep=";")

    df1 = lower_header(data_frame=df)

    df2 = df1.select(
        df1.id,
        df1.created_at,
        df1.merchant_id,
        df1.valor,
        df1.n_parcelas,
        functions.round(df1.valor / df1.n_parcelas, 2),
        functions.initcap(df1.nome_no_cartao),
        functions.when(
            functions.lower(df1.status) == "refunded", "refused"
        ).when(
            functions.lower(df1.status) == "in process", "processing"
        ).otherwise(functions.lower(df1.status)),
        df1.card_id,
        df1.iso_id,
        df1.card_brand,
        functions.regexp_replace(functions.lower(df1.documento), pattern=r"(\D+)", replacement="")
    )

    df3 = casting_fields(data_frame=df2, json_schema=get_json_schema_mapping("types_mapping.json"))

    df3.write.csv(path="output/sanitize_transactions", header=True, sep=";")

 
import json


def read_csv_to_tmp_table(spark, path_file, tablename, database):
    df = (
        spark.read.format("csv")
        .option("header", True)
        .option("delimiter", ";")
        .option("ignoreLeadingWhiteSpace", True)
        .load("file://" + path_file)
    )
    df.write.mode("overwrite").option("TRANSLATED_TO_EXTERNAL", "TRUE").saveAsTable(
        f"{database}.{tablename}"
    )
    return df


def get_content_filtred(json_file, filter_item, filter_value=None):
    content_filtred = []
    with open(json_file) as param_file:
        json_content = json.load(param_file)
        mapping_table = json_content["mapping_table"]
        content = list(json_content["tables"])
    for item in content:
        if item.get(filter_item) and (
            filter_value is None or item[filter_item] == filter_value
        ):
            content_filtred.append(item)
    return mapping_table, content_filtred


def load_spark(spark_csv):
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.master("local")
        .appName("local-tests-spark")
        .config("spark.ui.port", 4059)
        .getOrCreate()
    )
    return spark.read.csv(spark_csv, sep=";", header=True)


def get_df_by_index(df, debut, fin):
    from pyspark.sql.functions import monotonically_increasing_id

    df = df.withColumn("index", monotonically_increasing_id())
    df = df.filter((df.index >= debut) & (df.index <= fin))
    return df

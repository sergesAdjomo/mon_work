from pyspark.sql.functions import explode_outer, from_json, col, lit
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, ArrayType
from expose.setschema import build_spark_schema
from traitement_spark.code.schema_registry import get_json_format_schema

from icdc.hdputils.configuration import confUtils
from icdc.hdputils.hdfs import hdfsUtils
from icdc.hdputils.hive import hiveUtils
from icdc.hdputils.spark import sparkUtils
from datetime import datetime
import logging
import os

os.environ["HTTPS_PROXY"] = "http://pxy-http-srv.serv.cdc.fr:8080"
os.environ["HTTP_PROXY"] = "http://pxy-http-srv.serv.cdc.fr:8080"

conf_utils = confUtils()
hdfs_utils = hdfsUtils()
spark_utils = sparkUtils()

conf = conf_utils.config

hive_utils = hiveUtils(spark_utils)
build_schema = build_spark_schema(get_json_format_schema())

db_brute = conf.get("HIVE", "DB_HIVE_BRUTE")
db_lac = conf.get("HIVE", "DB_HIVE_LAC")
db_lac_path = conf.get("HIVE", "DB_HIVE_LAC_PATH")

def expose_data(flux: str) -> None:
    table_name = ""
    brute_tb = ""
    suffixes = ""

    if flux == "utilisateur":
        table_name = "utilisateur"
        brute_tb = "ciam_data"
        suffixes = [
            "utilisateur",
            "rattachements",
            "email",
            "fonction",
            "habilitations",
            "telephone",
            "auth",
            "auth_type",
        ]
    elif flux == "structures":
        table_name = "structure"
        brute_tb = "structures"
        suffixes = ["structure"]

    df = config_schema_dataframe(db_brute, brute_tb)
    df = extract_and_write_tables_by_ids(
        df=df.select(f"data.{table_name}.*"),
        root_table_name=table_name,
        suffixes=suffixes,
    )

def write_table(db_lac, db_lac_path, table_name, df):
    hive_utils.hive_append_table(
        df,
        db_lac,
        table_name,
        f"{db_lac_path}/{table_name}",
        partitionByColumns=None,
        hive_format="parquet",
        do_impala=False,
    )

def config_schema_dataframe(db_brute, brute_tb) -> DataFrame:
    try:
        df = spark_utils.spark.read.table(f"{db_brute}.{brute_tb}")
        df = df.withColumn("data", from_json(col("value"), build_schema)).select("data")
        return df
    except Exception as e:
        logging.error(f"Flux variable not set properly {e}")
        raise

def extract_and_write_tables_by_ids(
    df: DataFrame, suffixes: list, root_table_name: str
) -> None:
    def recursive_extract(current_df: DataFrame, current_table: str, id_cols=list):
        scalar_cols = [
            field.name
            for field in current_df.schema.fields
            if not isinstance(field.dataType, (StructType, ArrayType))
        ]

        select_cols = [c for c in scalar_cols if "id" in c.lower()]
        combined_id_fields = list(set(id_cols + select_cols))
        extra_id_cols = []

        for field in current_df.schema.fields:
            if isinstance(field.dataType, StructType):
                for subfield in field.dataType.fields:
                    if "id" in subfield.name.lower():
                        extra_id_cols.append(
                            col(f"{field.name}.{subfield.name}").alias(
                                f"{subfield.name}"
                            )
                        )

        final_cols = list(set(scalar_cols) | set(combined_id_fields))

        result_df = current_df.select(
            *[col(c) for c in final_cols if c in current_df.columns]
        )

        result_df = drop_duplicate_columns(result_df)
        result_df = add_insertion_date(result_df)
        result_df = result_df.distinct()
        write_table(db_lac, db_lac_path, current_table, result_df)
        print(f"Written table: {current_table}")

        for field in current_df.schema.fields:
            name, dtype = field.name, field.dataType
            next_table = f"{current_table}_{name}"

            # Handle nested arrays or structs matching suffix
            if any(name.endswith(suffix) for suffix in suffixes):
                if isinstance(dtype, StructType):
                    nested_df = current_df.select(
                        col(name + ".*"),
                        *[
                            col(c)
                            for c in combined_id_fields
                            if c in current_df.columns
                        ],
                    )
                    recursive_extract(nested_df, next_table, combined_id_fields)
                elif isinstance(dtype, ArrayType):
                    exploded_df = current_df.withColumn(name, explode_outer(col(name)))
                    nested_df = exploded_df.select(
                        col(f"{name}.*"),
                        *[
                            col(c)
                            for c in combined_id_fields
                            if c in exploded_df.columns
                        ],
                    )
                    recursive_extract(nested_df, next_table, combined_id_fields)

    recursive_extract(df, current_table=root_table_name, id_cols=[])


def drop_duplicate_columns(df):
    counter_set = set()
    cols_to_drop = [c for c in df.columns if c in counter_set or counter_set.add(c)]
    print(counter_set)
    return df.drop(*cols_to_drop)


def add_insertion_date(df):
    return df.withColumn("xx_date_insertion", lit(datetime.now()))





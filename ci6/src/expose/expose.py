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

FLUX_CONFIG = {
    "utilisateurs": {
        "table_name": "utilisateur",
        "brute_tb": "utilisateurs",
        "suffixes": ["utilisateur", "rattachements", "email", "fonction", 
                    "habilitations", "telephone", "auth", "authType"]
    },
    "structures": {
        "table_name": "structure", 
        "brute_tb": "structures",
        "suffixes": ["structure"]
    }
}

def expose_data(flux: str) -> None:
    if not flux or flux not in FLUX_CONFIG:
        raise ValueError(f"Flux '{flux}' non reconnu ou vide. Options valides: {list(FLUX_CONFIG.keys())}")
    
    config = FLUX_CONFIG[flux]
    df = _load_and_parse_data(config["brute_tb"], config["table_name"])
    _extract_and_write_tables(df, config["table_name"], config["suffixes"])

def _load_and_parse_data(brute_tb: str, table_name: str) -> DataFrame:
    try:
        df = spark_utils.spark.read.table(f"{db_brute}.{brute_tb}")
        df = df.withColumn("data", from_json(col("value"), build_schema))
        return df.select(f"data.{table_name}.*")
    except Exception as e:
        raise

def _write_table_optimized(table_name: str, df: DataFrame) -> None:
    try:
        key_columns = [c for c in df.columns if c.lower().endswith('id')]
        if 'version' in df.columns:
            key_columns.append('version')
        if not key_columns:
            key_columns = [c for c in df.columns if c != 'xx_date_insertion']
        
        df = df.dropDuplicates(key_columns)
        
        if not hive_utils.hive_check_exist_table(db_lac, table_name):
            _write_to_hive(table_name, df)
            return
        
        existing_df = spark_utils.spark.read.table(f"{db_lac}.{table_name}")
        new_data = df.exceptAll(existing_df.select(*df.columns))
        
        count_new = new_data.count()
        if count_new > 0:
            _write_to_hive(table_name, new_data)
    except Exception as e:
        _write_to_hive(table_name, df)

def _write_to_hive(table_name: str, df: DataFrame) -> None:
    hive_utils.hive_append_table(
        df, db_lac, table_name, f"{db_lac_path}/{table_name}",
        partitionByColumns=None, hive_format="parquet", do_impala=False
    )

def _extract_and_write_tables(df: DataFrame, root_table_name: str, suffixes: list) -> None:
    def _recursive_extract(current_df: DataFrame, current_table: str, id_cols: list = None):
        if id_cols is None:
            id_cols = []
            
        scalar_cols = [f.name for f in current_df.schema.fields 
                      if not isinstance(f.dataType, (StructType, ArrayType))]
        
        id_cols_current = list(set(id_cols + [c for c in scalar_cols if "id" in c.lower()]))
        
        result_df = (current_df
                    .select(*[c for c in scalar_cols if c in current_df.columns])
                    .dropDuplicates()
                    .withColumn("xx_date_insertion", lit(datetime.now())))
        
        unique_columns = list(dict.fromkeys(result_df.columns))
        result_df = result_df.select(*unique_columns)
        
        _write_table_optimized(current_table, result_df)
        
        for field in current_df.schema.fields:
            if not any(field.name.endswith(suffix) for suffix in suffixes):
                continue
                
            next_table = f"{current_table}_{field.name}"
            
            if isinstance(field.dataType, StructType):
                nested_df = current_df.select(
                    col(field.name + ".*"),
                    *[col(c) for c in id_cols_current if c in current_df.columns]
                )
                _recursive_extract(nested_df, next_table, id_cols_current)
                
            elif isinstance(field.dataType, ArrayType):
                exploded_df = current_df.withColumn(field.name, explode_outer(col(field.name)))
                nested_df = exploded_df.select(
                    col(f"{field.name}.*"),
                    *[col(c) for c in id_cols_current if c in exploded_df.columns]
                )
                _recursive_extract(nested_df, next_table, id_cols_current)
    
    _recursive_extract(df, root_table_name)

def config_schema_dataframe(db_brute, brute_tb) -> DataFrame:
    try:
        df = spark_utils.spark.read.table(f"{db_brute}.{brute_tb}")
        df = df.withColumn("data", from_json(col("value"), build_schema)).select("data")
        return df
    except Exception:
        raise

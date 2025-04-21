# ingesteur_spark_JDBC/src/spark_jdbc/code/constants.py

from pyspark.sql.types import StructType, StructField, StringType, TimestampType

INIT_TIMESTAMP = "1970-01-01 00:00:00"

SCHEMA_LAST_VALUE = StructType(

[StructField('database', StringType(), True),

StructField('table', StringType(), True),

StructField('ref_col', StringType(), True),

StructField('last_value', TimestampType(), True)

])

KEY_LAST_VALUE = ["database", "table", "ref_col"]

COL_LAST_VALUE = "last_value"
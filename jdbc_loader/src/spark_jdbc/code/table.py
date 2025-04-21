# ingesteur_spark_JDBC/src/spark_jdbc/code/table.py

from spark_jdbc.code.reference_column_last_value import ReferenceColumnLastValue

from spark_jdbc.code.constants import COL_LAST_VALUE, INIT_TIMESTAMP

from pyspark.sql.functions import col

import traceback

class Table:



def init(self,reference_column_last_value:ReferenceColumnLastValue, unit_param:dict):

try:

self.settings = reference_column_last_value.settings

self.jdbc_table = unit_param["jdbc_table"]

self.jdbc_schema = unit_param["jdbc_schema"]

self.jdbc_ingest_type = unit_param["jdbc_ingest_type"]

self.jdbc_reference_column = unit_param["jdbc_reference_column"]

self.jdbc_query = unit_param["jdbc_query"]

#self.jdbc_reference_column_type = unit_param["jdbc_reference_column_type"]

self.jdbc_partition_column = unit_param["jdbc_partition_column"]

self.jdbc_partition_column_type = unit_param["jdbc_partition_column_type"]

self.jdbc_lower_bound = unit_param["jdbc_lower_bound"]

self.jdbc_upper_bound = unit_param["jdbc_upper_bound"]

self.jdbc_num_partitions = unit_param["jdbc_num_partitions"]

self.jdbc_fetchsize = unit_param["jdbc_fetchsize"]

self.hive_table = unit_param["hive_table"]

self.hive_table_full = unit_param["hive_table_full"]

self.hive_database = unit_param["hive_database"]

self.hive_table_format = unit_param["hive_table_format"]

self.hive_schema_partition = unit_param["hive_schema_partition"]

self.hive_ingest_full = unit_param["hive_ingest_full"]

self.hive_write_mode = unit_param["hive_write_mode"]

self.__set_reference_column_max_value(reference_column_last_value)



except Exception as e:

# Renvoyer l'exception au Threadpool et continuer sur une autre table

traceback.print_exc()

raise Exception(e)



def __set_reference_column_max_value(self,reference_column_last_value):

if(self.jdbc_ingest_type in [ 'delta', 'delta_query']):

last_value = INIT_TIMESTAMP

result = reference_column_last_value.input_last_value_df \

.filter((col("database")==self.jdbc_schema) & (col("table")==self.jdbc_table) & (col("ref_col")==self.jdbc_reference_column)) \

.select(col(COL_LAST_VALUE).cast("string")).first()

if result is not None:

last_value = result[0]

else:

self.settings.logger.debug(f"la table {self.jdbc_schema}.{self.jdbc_table} avec la colonne de reference {self.jdbc_reference_column} n'existe pas dans le fichier {self.settings.last_value_file_snapshot}")

self.reference_column_max_value = last_value

self.settings.logger.debug(f"la valeur de la colonne de reference {self.jdbc_reference_column} de la table {self.jdbc_schema}.{self.jdbc_table} est : {self.reference_column_max_value}")

else:

self.reference_column_max_value = None
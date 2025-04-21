from spark_jdbc.code.params import Params
from spark_jdbc.code.table import Table
from spark_jdbc.code.ingestion import Ingestion
from spark_jdbc.code.reference_column_last_value import ReferenceColumnLastValue
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType
import concurrent.futures
from datetime import datetime
import pytz

timezone = pytz.timezone("Europe/Paris")

class Runner:

    def init(self, params: Params):
        self.params = params
        self.settings = params.settings
        self.reference_column_last_value = ReferenceColumnLastValue(self.settings)
        self.settings.logger.debug("affichage des données en entrée last_value")
        self.reference_column_last_value.input_last_value_df.show(n=1000, truncate=False)
        self.lancement_data = []

    def ingest_table(self, unit_param):
        start_time = datetime.now()
        table = Table(self.reference_column_last_value, unit_param)
        ingestion = Ingestion(table)
        try:
            self.settings.logger.debug(f"lecture de la {table.jdbc_table} ")
            ingestion.read_table()
            self.settings.logger.debug(f"ecriture de la {table.jdbc_table} ")
            if(ingestion.jdbc_table_df_empty == False):  # noqa: E712
                ingestion.write_mode(ingestion.jdbc_table_df, table.hive_write_mode, table.hive_table, partition=table.hive_schema_partition)
                self.settings.logger.debug(f"Fin ingestion de la {table.jdbc_schema}.{table.jdbc_table}. Durée:{datetime.now() - start_time} s")
                self.lancement_data.append({
                    "jdbc_schema": table.jdbc_schema,
                    "jdbc_table": table.jdbc_table,
                    "jdbc_ingest_type": table.jdbc_ingest_type,
                    "hive_database": table.hive_database,
                    "hive_table": table.hive_table,
                    "date_insertion": datetime.now(),
                    "statut": "ok",
                    "nb_lignes": ingestion.jdbc_table_df.count(),
                    "erreur": "",
                    "duree": str(datetime.now() - start_time)
                })
                ingestion.generate_reference_column_max_value_dataframe()
                self.reference_column_last_value.add_dataframe_row_last_value(ingestion.reference_column_max_value_df)
                if (table.hive_ingest_full):
                    self.settings.logger.debug(f"ecriture dans la table {table.hive_table_full} ")
                    ingestion.write_mode(ingestion.jdbc_table_df, "append", table.hive_table_full, partition=table.hive_schema_partition)
                    self.settings.logger.debug(f"fin d'ecriture dans la table {table.hive_table_full} ")
            else:
                self.settings.logger.debug(f"Pas d'ingestion pour la table {table.jdbc_table}")
                self.lancement_data.append({
                    "jdbc_schema": table.jdbc_schema,
                    "jdbc_table": table.jdbc_table,
                    "jdbc_ingest_type": table.jdbc_ingest_type,
                    "hive_database": table.hive_database,
                    "hive_table": table.hive_table,
                    "date_insertion": datetime.now(),
                    "statut": "vide",
                    "nb_lignes": 0,
                    "erreur": "",
                    "duree": str(datetime.now() - start_time)
                })

        except Exception as ex:
            self.settings.logger.debug(f"probleme de lecture de la {table.jdbc_table}")
            self.lancement_data.append({
                "jdbc_schema": table.jdbc_schema,
                "jdbc_table": table.jdbc_table,
                "jdbc_ingest_type": table.jdbc_ingest_type,
                "hive_database": table.hive_database,
                "hive_table": table.hive_table,
                "date_insertion": datetime.now(),
                "statut": "ko",
                "nb_lignes": None,
                "erreur": str(ex),
                "duree": str(datetime.now() - start_time)
            })
            raise ex
        return self.lancement_data

    def run(self):
        self.settings.logger.info("DEBUT: ####### main: Debut des ingestions")
        start_time = datetime.now()
        exceptions = []
        
        # Lancement des réparations en parallel
        with concurrent.futures.ThreadPoolExecutor(max_workers=int(self.settings.nb_jdbc_parallel)) as executor:
            # Pour chacunes des tables
            futures = {executor.submit(self.ingest_table, table): table for table in self.params.params}
            
            # Vérifie le résultat afin de s'assurer qu'on a pas levé d'erreur
            for future in concurrent.futures.as_completed(futures):
                table = futures[future]
                try:
                    future.result()
                except Exception as ex:
                    self.settings.logger.error(f"L'ingestion de la table '{table}' a entraîné une erreur : {str(ex)}")
                    exceptions.append(ex)
        
        if self.lancement_data:
            schema = StructType([
                StructField("jdbc_schema", StringType(), True),
                StructField("jdbc_table", StringType(), True),
                StructField("jdbc_ingest_type", StringType(), True),
                StructField("hive_database", StringType(), True),
                StructField("hive_table", StringType(), True),
                StructField("date_insertion", TimestampType(), True),
                StructField("statut", StringType(), True),
                StructField("nb_lignes", LongType(), True),
                StructField("erreur", StringType(), True),
                StructField("duree", StringType(), True)
            ])
            df_resume_lancement = self.settings.spark.createDataFrame(self.lancement_data, schema=schema)
            self.settings.logger.info("le resume des tables alimentes")
            self.settings.logger.info(f"{df_resume_lancement.show(n=1000, truncate=False)}")
            self.settings.logger.info("Ecriture dans tx_eploit")
            df_resume_lancement.write.mode("append").parquet(self.settings.tx_exploit)
            self.reference_column_last_value.generate_dataframe_last_value()
            self.reference_column_last_value.write_all_dataframe_last_value()

        if len(exceptions) > 0:
            raise Exception(f"Liste des erreurs rencontrés: {exceptions}")
        self.settings.logger.info("FIN: ####### main: Fin du job Durée: {0} s".format((datetime.now() - start_time)))
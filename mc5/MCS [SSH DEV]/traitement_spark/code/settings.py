 
#!/usr/hdp/current/spark2-client python
# -*- coding: utf-8 -*-
from datetime import datetime
from pyspark.sql.functions import sum, avg, max, count


class Settings:
    def __init__(self, config, setting_logger, date_ctrlm):
        self.config = config
        # import pdb; pdb.set_trace()
        self.logger = setting_logger
        self.date_ctrlm = date_ctrlm

    def get_app_name(self):
        return self.config.get("DEFAULT", "APP_NAME")

    def get_log_local_path(self):
        return self.config.get("LOG", "LOCAL_LOG_PATH")

    def get_hdfs_path_brute(self):
        return str(self.config.get("HDFS", "HDFS_PATH_BRUTE")).lower()

    def get_hive_config(self) -> dict:
        # hive_database_brute = str(config.get('HIVE' , 'DB_HIVE_BRUTE' )).lower()
        # hive_path_brute = str(config.get('HIVE' , 'DB_HIVE_BRUTE_PATH' )).lower()
        hive_database_travail = str(self.config.get("HIVE", "DB_HIVE_TRAVAIL")).lower()
        hive_path_traitement = str(
            self.config.get("HIVE", "DB_HIVE_TRAVAIL_PATH")
        ).lower()
        # hive_database_lac = str(config.get('HIVE' , 'DB_HIVE_LAC' )).lower()
        # hive_path_lac = str(config.get('HIVE' , 'DB_HIVE_LAC_PATH' )).lower()
        return {
            "hive_database_travail": hive_database_travail,
            "hive_path_traitement": hive_path_traitement,
        }

    def generate_tx_exploit(
        self,
        spark,
        hive_utils,
        date_debut,
        table_traitee,
        statut,
        nb_lignes_traitees,
        app_name,
    ):
        """
        Cette fonction permet d'inserer une ligne d'exploitation dans la table tx_exploit du code appli concerne.
        """

        tx_exploit_data = {
            "id_job": str(spark.spark.sparkContext.applicationId),
            "nom_job": str(app_name),
            "nom_app": str(app_name),
            "code_appli": str(app_name),
            "table_traitee": str(table_traitee),
            "date_debut": date_debut,
            "statut": str(statut),
            "date_fin": datetime.now().strftime("%Y%m%d%H%M%S"),
            "nb_lignes_traitees": int(nb_lignes_traitees),
        }

        df_txexploit = spark.spark.createDataFrame([tx_exploit_data])
        df_txexploit.show(100, True)
        
        self.logger.info(spark.catalog.listColumns("tx_exploit" ,  self.get_hive_config().get("hive_database_travail")))

        hive_utils.hive_append_table(
            df_txexploit,
            self.get_hive_config().get("hive_database_travail"),
            "tx_exploit",
            table_path=self.get_hive_config().get("hive_path_traitement")
            + "/{0}".format("tx_exploit"),
        )
        return True

 
import socket
import time
from datetime import datetime

from icdc.hdputils.hive import hiveUtils
from pyspark.sql.functions import col
from traitement_spark.code.settings import Settings
from traitement_spark.code.utils import CommonUtils

import service_innovant.service_innovant_fields as Fields
from service_innovant.service_innovant_dataframe import \
    TraitementServiceInnovantDataFrame


class TraitementServiceInnovant(CommonUtils):

    def __init__(self, spark, config):

        self.spark = spark
        self.config = config

        super().__init__(self.spark, self.config)

        # sys.excepthook = config.handle_exception
        self.logger = self.config.logger
        self.conf = self.config.config
        self.date_ctrlm = self.config.dateCrtlm()

        self.start_time = time.time()
        self.timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")

        # recuperation des parametres de configuration
        self.app_name = self.conf.get("DEFAULT", "APP_NAME")
        self.logger_local_name = self.conf.get("LOG", "LOG_FILENAME")
        self.logger_hdfs_name = f"{self.timestamp}'_'{self.logger_local_name}"
        self.logger_hdfs_path = (
            f"{self.conf.get('LOG', 'HDFS_LOG_PATH')}{self.logger_hdfs_name}"
        )
        self.logger_local_path = (
            f"{self.conf.get('LOG', 'LOCAL_LOG_PATH')}{self.logger_local_name}"
        )

        self.logger.info("INFO: recuperation des variables de configuration")
        self.logger.info(f"INFO: app_name : {self.app_name}")
        self.logger.info(f"INFO: DRIVER :  {socket.gethostname()}")
        self.logger.info(f"INFO: fichier de LOG : {self.logger_local_path}")
        self.logger.info(f"Date Ctrlm : {self.date_ctrlm}")

        # recuperation des paramètres depuis la conf
        self.nom_table = "service_innovant"
        self.db_lac = str(self.conf.get("HIVE", "DB_HIVE_LAC"))
        self.tx_exploit = self.conf.get("HIVE", "TX_EXPLOIT")
        self.env = self.conf.get("DEFAULT", "ENVIRONMENT")
        self.db_travail = self.conf.get("HIVE", "DB_TRAVAIL")
        self.db_lac = self.conf.get("HIVE", "DB_LAC")
        self.db_lac_path = self.conf.get("HIVE", "DB_HIVE_LAC_PATH")
        self.db_travail_path = self.conf.get("HIVE", "DB_HIVE_TRAVAIL_PATH")
        self.db_wa7 = self.conf.get("HIVE", "DB_SRC_WA7")
        self.db_ct3 = self.conf.get("HIVE", "DB_SRC_CT3")
        # informations technique
        self.DATE_DEB_ALIM = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        # Settings:
        self.settings = Settings(self.conf, self.logger, self.date_ctrlm)
        self.app_name = self.settings.get_app_name()
        ####################################################################################################################################

        # import Type page dataframe objects
        self.df = TraitementServiceInnovantDataFrame(self.spark, self.config)

        # Columns
        self.bv_tracking_columns = [
            Fields.FIELDS.get("idvisit"),
            Fields.FIELDS.get("pagetitle"),
            Fields.FIELDS.get("timespent"),
            Fields.FIELDS.get("url"),
            Fields.FIELDS.get("xx_jour"),
            Fields.FIELDS.get("type"),
        ]

    def submit(self):
        self.logger.info("START PROCESSING SERVICE INNOVANT")

        try:
            self.df.df = self.read_table(self.db_ct3, "bv_tracking").select(
                *self.bv_tracking_columns
            )

            self.df.filter_service_innovant()
            self.df.add_inno_columns()
            self.df.filter_page_offres()
            self.df.add_page_offres_columns()
            self.df.df = self.df.page_offres_df.union(
                self.df.service_innovant_df
            ).select(
                col(Fields.FIELDS.get("idvisit")),
                col(Fields.FIELDS.get("pagetitle")).alias("pagetitle"),
                col(Fields.FIELDS.get("timespent")).alias("timespent"),
                col(Fields.FIELDS.get("url")).alias("url"),
                col(Fields.FIELDS.get("xx_jour")).alias("xx_jour"),
                col(Fields.FIELDS.get("type")).alias("type"),
                col("Nom_Services"),
                col("Nom_Categorie"),
            )

            return self.df.df

        except Exception as e:
            self.logger.error(f"error caused by: {e}")
            raise

    def process(self):
        self.logger.info("Computing NB_LIGN_TRT...")
        service_innovant = self.submit()
        NB_LIGN_TRT: int = service_innovant.count()

        try:
            self.write_table(
                service_innovant,
                self.db_lac,
                self.nom_table,
                f"{self.db_lac_path}/{self.nom_table}",
            )
        except Exception as e:
            self.logger.error(f"Error caused by: {e}")
            if NB_LIGN_TRT > 0:
                self.settings.generate_tx_exploit(
                    self.spark,
                    self.hive_utils,
                    self.DATE_DEB_ALIM,
                    self.nom_table,
                    "OK",
                    NB_LIGN_TRT,
                    self.app_name,
                )
            elif NB_LIGN_TRT == 0:
                self.settings.generate_tx_exploit(
                    self.spark,
                    self.hive_utils,
                    self.DATE_DEB_ALIM,
                    self.nom_table,
                    "KO",
                    NB_LIGN_TRT,
                    self.app_name,
                )
        except Exception as e:
            self.logger.error(f"Error caused by: {e}")
            raise

 
import socket
import time
from datetime import datetime

from pyspark.sql.functions import col
from traitement_spark.code.settings import Settings
from traitement_spark.code.utils import CommonUtils

import tracking_site.tracking_site_fields as Fields
from tracking_site.tracking_site_dataframe import \
    TraitementTrackingSiteDataFrame


class TraitementTrackingSite(CommonUtils):
    """
    Cette action permet de faire le traitement
    spécifique de performance organic - tracking site
    """

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

        # TODO doit bouger en dehors de cette class
        self.logger.info(f"Date Ctrlm : {self.date_ctrlm}")

        # recuperation des paramètres depuis la conf
        self.nom_table = "tracking_site"
        self.db_lac = str(self.conf.get("HIVE", "DB_HIVE_LAC"))
        self.tx_exploit = self.conf.get("HIVE", "TX_EXPLOIT")
        self.env = self.conf.get("DEFAULT", "ENVIRONMENT")
        self.db_ct3 = self.conf.get("HIVE", "DB_SRC_CT3")
        self.db_travail = self.conf.get("HIVE", "DB_TRAVAIL")
        self.db_lac = self.conf.get("HIVE", "DB_LAC")
        self.db_lac_path = self.conf.get("HIVE", "DB_HIVE_LAC_PATH")
        self.db_travail_path = self.conf.get("HIVE", "DB_HIVE_TRAVAIL_PATH")
        self.db_wa7 = self.conf.get("HIVE", "DB_SRC_WA7")
        self.settings = Settings(self.conf, self.logger, self.date_ctrlm)

        # informations technique
        self.DATE_DEB_ALIM = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # table spec
        self.brute_tb = "tb_matomo_brute_11"
        self.detail_tb = "tb_matomo_brute_11_actiondetails"

        # import Type page dataframe objects
        self.df = TraitementTrackingSiteDataFrame(self.spark, self.config)
        self.goalnames = Fields.GOALNAMES

    def submit(self):
        self.logger.info("START PROCESSING TRACKING SITE DATAMART")

        try:
            self.df.df = (
                self.read_table(self.db_ct3, "bv_tracking")
                .withColumn(
                    Fields.FIELDS.get("visit_duration"),
                    col(Fields.FIELDS.get("visit_duration")).cast("float"),
                )
                .withColumn(
                    Fields.FIELDS.get("timespent"),
                    col(Fields.FIELDS.get("timespent")).cast("int"),
                )
            )

            self.df.compute_nbrows_tps_passe_total()
            self.df.tracking_detail()
            self.df.compute_tracking_indicator()
            self.df.compute_matomo_kpi()

            return self.df.df

        except Exception as e:
            self.logger.error(f"error caused by: {e}")
            raise

    def process(self):
        self.logger.info("Computing NB_LIGN_TRT...")
        tracking_site = self.submit()
        NB_LIGN_TRT: int = tracking_site.count()

        try:
            self.write_table(
                tracking_site,
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

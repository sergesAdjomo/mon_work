 
import socket
import time
from datetime import datetime

from icdc.hdputils.hive import hiveUtils
from pyspark.sql.functions import col
from traitement_spark.code.settings import Settings
from traitement_spark.code.utils import CommonUtils

import campagne_marketing.campagne_marketing_fields as Fields
from campagne_marketing.campagne_marketing_dataframe import \
    CampagneMarketingDataFrame


class TraitementCampagneMarketing(CommonUtils):

    def __init__(self, spark, config):
        self.spark = spark
        self.config = config
        super().__init__(self.spark, self.config)

        self.logger = self.config.logger
        self.conf = self.config.config
        self.date_ctrlm = self.config.dateCrtlm()

        self.start_time = time.time()
        self.timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")

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

        # Variables
        self.env = self.conf.get("DEFAULT", "ENVIRONMENT")
        self.db_ct3 = self.conf.get("HIVE", "DB_SRC_CT3")
        self.nom_table = "campagne_marketing"
        self.db_lac_path = self.conf.get("HIVE", "DB_HIVE_LAC_PATH")
        self.db_lac = str(self.conf.get("HIVE", "DB_HIVE_LAC"))
        self.db_lac = self.conf.get("HIVE", "DB_LAC")
        self.settings = Settings(self.conf, self.logger, self.date_ctrlm)

        # informations technique
        self.DATE_DEB_ALIM = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # nom des tables
        self.bv_operation = "bv_operation_marketing"
        self.bv_diffusion = "bv_diffusion"
        self.pv_diffusion_log = "pv_diffusion_log"
        self.bv_log_message = "bv_log_message"
        self.bv_recipient_fonction = "bv_recipient_fonction"

        # liste des attributs pour chaque table

        self.bv_operation_col = [
            Fields.JOINT_KEYS.get("id_operation_hub"),
            Fields.FIELDS.get("nom_operation"),
            Fields.FIELDS.get("libelle_operation"),
            Fields.FIELDS.get("typologie_operation"),
            Fields.FIELDS.get("date_creation"),
            Fields.FIELDS.get("itempsfort"),
        ]

        self.bv_diffusion_col = [
            Fields.JOINT_KEYS.get("id_campagne"),
            Fields.FIELDS.get("libelle_url"),
            Fields.JOINT_KEYS.get("id_delivery"),
            Fields.JOINT_KEYS.get("id_trackingurl"),
            Fields.FIELDS.get("type_de_click"),
            Fields.FIELDS.get("sinternalname"),
        ]

        self.pv_diffusion_log_col = [
            Fields.JOINT_KEYS.get("id_delivery"),
            Fields.JOINT_KEYS.get("id_rcpt_fct"),
            Fields.JOINT_KEYS.get("id_msg"),
            Fields.JOINT_KEYS.get("id_url"),
        ]

        self.bv_log_message_col = [
            Fields.JOINT_KEYS.get("id_logmsg"),
            Fields.FIELDS.get("raison_echec"),
        ]

        self.bv_recipient_fonction_col = [
            Fields.JOINT_KEYS.get("hub_id_client"),
            Fields.FIELDS.get("mail_contact"),
            Fields.FIELDS.get("libelle_fonction"),
            Fields.FIELDS.get("code_segment"),
            Fields.FIELDS.get("libelle_segment"),
            Fields.FIELDS.get("nom_region"),
        ]

        # import dataframe objects
        self.df = CampagneMarketingDataFrame(self.spark, self.config)

    def submit(self):
        self.logger.info("START PROCESSING CAMPAGNE MARKETING DATAMART")
        try:

            # read table
            bv_operation = self.read_table(self.db_ct3, f"{self.bv_operation}").select(
                *self.bv_operation_col
            )
            bv_diffusion = (
                self.read_table(self.db_ct3, f"{self.bv_diffusion}")
                .select(*self.bv_diffusion_col)
                .distinct()
            )
            pv_diffusion_log = (
                self.read_table(self.db_ct3, f"{self.pv_diffusion_log}")
                .select(*self.pv_diffusion_log_col)
                .distinct()
            )
            bv_log_message = self.read_table(
                self.db_ct3, f"{self.bv_log_message}"
            ).select(*self.bv_log_message_col)
            bv_recipient_fonction = (
                self.read_table(self.db_ct3, f"{self.bv_recipient_fonction}")
                .select(*self.bv_recipient_fonction_col)
                .distinct()
            )

            self.df.df = (
                bv_operation.join(
                    bv_diffusion,
                    on=bv_diffusion[Fields.JOINT_KEYS.get("id_campagne")]
                    == bv_operation[Fields.JOINT_KEYS.get("id_operation_hub")],
                    how="left",
                )
                .join(
                    pv_diffusion_log,
                    on=(
                        bv_diffusion[Fields.JOINT_KEYS.get("id_delivery")]
                        == pv_diffusion_log[Fields.JOINT_KEYS.get("id_delivery")]
                    )
                    & (
                        bv_diffusion[Fields.JOINT_KEYS.get("id_trackingurl")]
                        == pv_diffusion_log[Fields.JOINT_KEYS.get("id_url")]
                    ),
                    how="inner",
                )
                .drop(pv_diffusion_log[Fields.JOINT_KEYS.get("id_delivery")])
                .join(
                    bv_log_message,
                    on=bv_log_message[Fields.JOINT_KEYS.get("id_logmsg")]
                    == pv_diffusion_log[Fields.JOINT_KEYS.get("id_msg")],
                    how="left",
                )
                .join(
                    bv_recipient_fonction,
                    on=bv_recipient_fonction[Fields.JOINT_KEYS.get("hub_id_client")]
                    == pv_diffusion_log[Fields.JOINT_KEYS.get("id_rcpt_fct")],
                    how="inner",
                )
                .filter(
                    ~bv_recipient_fonction[Fields.FIELDS.get("mail_contact")].like(
                        "%@caissedesdepots.fr"
                    )
                    & (
                        bv_operation[Fields.FIELDS.get("nom_operation")].like("ACS_%")
                        | bv_operation[Fields.FIELDS.get("nom_operation")].like("CMP_%")
                    )
                )
            )

            self.df.compute_adobe_indicator()
            self.df.compute_adobe_kpi()

            return self.df.df

        except Exception as e:
            self.logger.error(f"error caused by: {e}")
            raise

    def process(self):
        self.logger.info("Computing NB_LIGN_TRT...")
        campagne_marketing = self.submit()
        NB_LIGN_TRT: int = campagne_marketing.count()

        try:
            self.write_table(
                campagne_marketing,
                self.db_lac,
                self.nom_table,
                f"{self.db_lac_path}/{self.nom_table}",
            )
        except Exception as e:
            self.logger.error(f"unable to continue due to issues related to : {e}")
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
            self.logger.error(f"Error occured due to: {e}")

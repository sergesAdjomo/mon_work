import socket
import time
from datetime import datetime

from icdc.hdputils.hive import hiveUtils
from pyspark.sql.functions import col, date_format, substring
from traitement_spark.code.settings import Settings
from traitement_spark.code.utils import CommonUtils

import qualification_ols.qualification_ols_fields as Fields
from qualification_ols.qualification_ols_dataframe import \
    TraitementQualificationOLSDataFrame


class TraitementQualificationOLS(CommonUtils):

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

        self.logger.info(f"Date Ctrlm : {self.date_ctrlm}")

        # Variables
        self.env = self.conf.get("DEFAULT", "ENVIRONMENT")
        self.db_ct3 = self.conf.get("HIVE", "DB_SRC_CT3")
        self.nom_table = "qualification_ols"
        self.db_lac_path = self.conf.get("HIVE", "DB_HIVE_LAC_PATH")
        self.db_lac = str(self.conf.get("HIVE", "DB_HIVE_LAC"))
        self.db_lac = self.conf.get("HIVE", "DB_LAC")
        self.settings = Settings(self.conf, self.logger, self.date_ctrlm)

        # informations technique
        self.DATE_DEB_ALIM = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # nom des tables
        self.bv_pm = "bv_personne_morale_bdt"
        self.bv_tiers = "bv_tiers_bdt"
        self.bv_coord_postales = "bv_coordonnees_postales"
        self.bv_departement = "bv_departement"
        self.bv_region = "bv_region"

        # liste des attributs pour chaque table

        self.bv_pm_col = [
            Fields.FIELDS.get("siren"),
            Fields.FIELDS.get("siret"),
            Fields.FIELDS.get("denom_unite_legale"),
            Fields.FIELDS.get("sous_cat"),
            Fields.FIELDS.get("etat_admin"),
            Fields.FIELDS.get("is_tete_groupe"),
            Fields.FIELDS.get("is_ols"),
        ]

        self.bv_tiers_col = [
            Fields.FIELDS.get("code_tiers"),
            Fields.FIELDS.get("siren"),
            Fields.FIELDS.get("siret"),
            Fields.FIELDS.get("code_etatiers"),
        ]

        self.bv_coord_postales_col = [
            Fields.FIELDS.get("code_tiers"),
            Fields.FIELDS.get("adr_code_postal"),
            Fields.FIELDS.get("lib_bureau_distrib"),
            Fields.FIELDS.get("dat_horodat"),
        ]

        self.bv_departement_col = [
            Fields.FIELDS.get("code_departement"),
            Fields.FIELDS.get("code_region"),
        ]

        self.bv_region_col = [
            Fields.FIELDS.get("code_region"),
            Fields.FIELDS.get("lib_clair_region"),
        ]

        # import dataframe objects
        self.df = TraitementQualificationOLSDataFrame(self.spark, self.config)

    def submit(self):
        self.logger.info("START PROCESSING QUALIFICATION OLS DATAMART")
        try:

            # read table
            self.df.bv_pm = self.read_table(self.db_ct3, f"{self.bv_pm}").select(
                *self.bv_pm_col
            )
            self.df.bv_tiers = self.read_table(self.db_ct3, f"{self.bv_tiers}").select(
                *self.bv_tiers_col
            )
            self.df.bv_coord_postales = self.read_table(
                self.db_ct3, f"{self.bv_coord_postales}"
            ).select(*self.bv_coord_postales_col)
            self.df.bv_departement = self.read_table(
                self.db_ct3, f"{self.bv_departement}"
            ).select(*self.bv_departement_col)
            self.df.bv_region = self.read_table(
                self.db_ct3, f"{self.bv_region}"
            ).select(*self.bv_region_col)

            self.df.compute_code_tiers_cases()
            self.df.compute_df_with_priority_code_tiers()
            self.df.join_bvs()
            self.df.compute_siret_par_date()
            self.df.compute_qualif_ols_dataframe()

            return self.df.df

        except Exception as e:
            self.logger.error(f"error caused by: {e}")
            raise

    def process(self):
        self.logger.info("Computing NB_LIGN_TRT...")
        qualif_ols = self.submit()
        NB_LIGN_TRT: int = qualif_ols.count()

        try:
            self.write_table(
                qualif_ols,
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
            else:
                self.logger.error(f"Error occured due to: {e}")
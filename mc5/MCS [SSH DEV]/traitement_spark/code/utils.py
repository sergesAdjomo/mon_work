 
from pyspark.sql import DataFrame
from icdc.hdputils.hive import hiveUtils
from datetime import datetime
from traitement_spark.code.settings import Settings


class CommonUtils:
    def __init__(self, spark_utils, config):

        self.logger = config.logger
        self.spark = spark_utils
        self.spark.setSparkConf(
            confs=[
                ("spark.es.net.ssl", "true"),
                ("spark.hadoop.hive.exec.dynamic.partition", "true"),
                (
                    "spark.hadoop.hive.exec.dynamic.partition.mode",
                    "nonstrict",
                ),  # does not prevent data loss in partitions
                ("spark.sql.session.timeZone", "CET"),
                ("spark.app.name", "MC5_APP"),
            ]
        )
        self.hive_utils = hiveUtils(self.spark)

        self.logger = config.logger
        self.conf = config.config
        self.date_ctrlm = config.dateCrtlm()
        self.settings = Settings(self.conf, self.logger, self.date_ctrlm)

        # informations technique
        self.DATE_DEB_ALIM = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        self.nom_table = ""
        self.app_name = ""
        self.db_wa7 = self.conf.get("HIVE", "DB_SRC_WA7")
        self.nom_table = ""
        self.detail_tb = ""
        self.table_path = ""

    def read_table(self, db_zone: str, tb_name: str, *args) -> DataFrame:
        sep = ", "
        columns = sep.join(args)
        try:
            if not args:
                query = self.hive_utils.hive_execute_query(
                    f"select * from {db_zone}.{tb_name}"
                )
                return query
            else:
                query = self.hive_utils.hive_execute_query(
                    f"select {columns} from  {db_zone}.{tb_name}"
                )
                return query
        except Exception as e:
            print(f"Unable to load table: {e}")
            raise

    def write_table(
        self,
        df: DataFrame,
        db: str,
        nom_table: str,
        table_path: str,
        *args,
    ):
        try:
            self.hive_utils.hive_overwrite_table(
                df,
                db,
                nom_table,
                table_path,
                drop_and_recreate_table=False,
                partitionByColumns=None,
                hive_format="parquet",
                overwrite_only_partitions=False,
                do_impala=True,
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

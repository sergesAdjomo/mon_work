import time
from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit

from icdc.hdputils.configuration import confUtils
from icdc.hdputils.spark import sparkUtils
from icdc.hdputils.hive import hiveUtils
from traitement_spark.code.settings import Settings
from traitement_spark.code.utils import CommonUtils


class TraitementQualificationOLSStarModelMart(CommonUtils):
    
    def __init__(self, spark: sparkUtils, config: confUtils = None):
        self.spark = spark
        self.config = config or {}
        super().__init__(self.spark, self.config)
        
        self._setup_variables()
        self.hive_utils = hiveUtils(self.spark)
        
    def _setup_variables(self):
        self.logger = self.config.logger
        self.conf = self.config.config
        self.date_ctrlm = self.config.dateCrtlm()

        self.app_name = self.conf.get("DEFAULT", "APP_NAME")
        self.env = self.conf.get("DEFAULT", "ENVIRONMENT")
        
        self.db_lac = self.conf.get("HIVE", "DB_HIVE_TRAVAIL")
        self.db_mart = self.conf.get("HIVE", "DB_HIVE_LAC")
        
        self.table_mart = "qualification_ols"
        
        self.DATE_DEB_ALIM = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.settings = Settings(self.conf, self.logger, self.date_ctrlm)
        
    def process(self):
        try:
            dim_pm_bdt = self.spark.spark.table(f"{self.db_lac}.dim_pm_bdt")
            dim_localisation = self.spark.spark.table(f"{self.db_lac}.dim_localisation")
            dim_temps = self.spark.spark.table(f"{self.db_lac}.dim_temps")
            ft_qualif_donnees_usage = self.spark.spark.table(f"{self.db_lac}.ft_qualif_donnees_usage")
            
            df_final = self.build_mart(dim_pm_bdt, dim_localisation, dim_temps, ft_qualif_donnees_usage)
            
            self.save_to_hive(df_final)
            
            NB_LIGN_TRT = df_final.count()
            
            self.settings.generate_tx_exploit(
                self.spark,
                self.hive_utils,
                self.DATE_DEB_ALIM,
                self.table_mart,
                "OK",
                NB_LIGN_TRT,
                self.app_name
            )
            
            return True
            
        except Exception:
            self.settings.generate_tx_exploit(
                self.spark,
                self.hive_utils,
                self.DATE_DEB_ALIM,
                self.table_mart,
                "KO",
                0,
                self.app_name
            )
            return False

    def build_mart(self,
                   dim_pm_bdt: DataFrame,
                   dim_localisation: DataFrame,
                   dim_temps: DataFrame,
                   ft_qualif_donnees_usage: DataFrame) -> DataFrame:
        
        # Extraire SIREN de ft_qualif_donnees_usage
        base_df = ft_qualif_donnees_usage.select(
            col("annee_mois_siren"),
            col("siren")
        ).distinct()
        
        # Jointure avec dim_pm_bdt pour récupérer les informations de l'entreprise
        pm_data = dim_pm_bdt.select(
            col("annee_mois_siren"),
            col("raison_sociale"),
            col("sous_categorie"),
            col("tete_de_groupe"),
            col("code_tiers")
        )
        
        # Jointure avec dim_localisation pour récupérer ville et région
        loc_data = dim_localisation.select(
            col("annee_mois_siren"),
            col("ville"),
            col("region")
        )
        
        # Construction de la table finale par jointures successives
        result_df = base_df \
            .join(pm_data, on="annee_mois_siren", how="left") \
            .join(loc_data, on="annee_mois_siren", how="left")
        
        # Sélection et formatage des colonnes finales selon les spécifications
        final_df = result_df.select(
            col("siren"),
            col("raison_sociale"),
            col("sous_categorie"),
            col("ville"),
            col("region").alias("région"),
            col("code_tiers"),
            when(col("tete_de_groupe") == True, 1).otherwise(0).alias("tête_de_groupe")
        )
        
        return final_df

    def save_to_hive(self, df: DataFrame):
        df = df.cache()
        
        try:
            self.hive_utils.hive_overwrite_table(
                df,
                self.db_mart,
                self.table_mart,
                f"{self.db_mart}/{self.table_mart}",
                True,
                None,
                "parquet",
                False
            )
            
        except Exception:
            try:
                df.write \
                    .format("parquet") \
                    .mode("overwrite") \
                    .option("compression", "snappy") \
                    .saveAsTable(f"{self.db_mart}.{self.table_mart}")
            except Exception:
                raise
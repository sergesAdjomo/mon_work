import time
from datetime import datetime
from icdc.hdputils.hive import hiveUtils
from traitement_spark.code.settings import Settings
from traitement_spark.code.utils import CommonUtils
from qualification_ols_star_model.constants.fields import FIELDS


class TraitementQualificationOLSStarModel(CommonUtils):
    
    def __init__(self, spark, config):
        self.spark = spark
        self.config = config
        super().__init__(self.spark, self.config)
        
        self.logger = self.config.logger
        self.conf = self.config.config
        self.date_ctrlm = self.config.dateCrtlm()
        
        self.app_name = self.conf.get("DEFAULT", "APP_NAME")
        self.env = self.conf.get("DEFAULT", "ENVIRONMENT")
        self.db_ct3 = self.conf.get("HIVE", "DB_SRC_CT3")
        self.db_lac_path = self.conf.get("HIVE", "DB_HIVE_TRAVAIL_PATH")
        self.db_lac = self.conf.get("HIVE", "DB_HIVE_TRAVAIL")
        
        self.settings = Settings(self.conf, self.logger, self.date_ctrlm)
        self.DATE_DEB_ALIM = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.hive_utils = hiveUtils(self.spark)
        
        # Tables
        self.table_mart = "qualification_ols_star_model"
        self.dim_pm_bdt_table = "dim_pm_bdt"
        self.dim_temps_table = "dim_temps"
        self.dim_localisation_table = "dim_localisation"
        self.ft_qualif_donnees_usage_table = "ft_qualif_donnees_usage"
        
        self.df = None

    def _initialize_dataframe(self):
        try:
            from qualification_ols_star_model.dataframe import TraitementQualificationOLSStarModelDataFrame
            self.df = TraitementQualificationOLSStarModelDataFrame(self.spark, self.config)
            return True
        except Exception:
            return False

    def _read_source_tables(self):
        columns = {
            "bv_pm": [FIELDS.get("siren"), "siret", FIELDS.get("denom_unite_legale"), 
                     FIELDS.get("sous_cat"), FIELDS.get("etat_admin"), FIELDS.get("is_tete_groupe"), 
                     FIELDS.get("is_ols"), FIELDS.get("etab_siege")],
            "bv_tiers": [FIELDS.get("code_tiers"), FIELDS.get("siren"), "siret", FIELDS.get("code_etatiers")],
            "bv_coord_postales": [FIELDS.get("code_tiers"), FIELDS.get("adr_code_postal"), 
                                 FIELDS.get("lib_bureau_distrib"), FIELDS.get("dat_horodat")],
            "bv_departement": [FIELDS.get("code_departement"), FIELDS.get("code_region")],
            "bv_region": [FIELDS.get("code_region"), FIELDS.get("lib_clair_region")]
        }
        
        # Tables essentielles
        self.df.bv_pm = self.read_table(self.db_ct3, "bv_personne_morale_bdt").select(*columns["bv_pm"])
        self.df.bv_tiers = self.read_table(self.db_ct3, "bv_tiers_bdt").select(*columns["bv_tiers"])
        
        # Tables optionnelles
        for table_name in ["bv_coordonnees_postales", "bv_coordonnee_postale"]:
            try:
                self.df.bv_coord_postales = self.read_table(self.db_ct3, table_name).select(*columns["bv_coord_postales"])
                break
            except:
                continue
        
        try:
            self.df.bv_departement = self.read_table(self.db_ct3, "bv_departement").select(*columns["bv_departement"])
        except:
            self.df.bv_departement = None
            
        try:
            self.df.bv_region = self.read_table(self.db_ct3, "bv_region").select(*columns["bv_region"])
        except:
            self.df.bv_region = None

    def submit(self):
        if not self._initialize_dataframe():
            raise Exception("Erreur d'initialisation du gestionnaire de dataframes")
        
        self._read_source_tables()
        return self.df.process()

    def _write_tables(self, dataframes):
        tables = [
            ("dim_pm_bdt", self.dim_pm_bdt_table),
            ("dim_temps", self.dim_temps_table),
            ("dim_localisation", self.dim_localisation_table),
            ("ft_qualif_donnees_usage", self.ft_qualif_donnees_usage_table)
        ]
        
        total_lignes = 0
        
        for key, table in tables:
            df = dataframes.get(key)
            if df is not None:
                row_count = df.count()
                total_lignes += row_count
                
                self.hive_utils.hive_overwrite_table(
                    df, self.db_lac, table, f"{self.db_lac_path}/{table}",
                    True, None, "parquet", False
                )
        
        return total_lignes

    def process(self):
        if not self._initialize_dataframe():
            return 1
        
        try:
            dataframes = self.submit()
            self._write_tables(dataframes)
            
            model_df = dataframes.get("model")
            if model_df:
                nb_lignes = model_df.count()
                status = "OK"
            else:
                facts_df = dataframes.get("ft_qualif_donnees_usage")
                nb_lignes = facts_df.count() if facts_df else 0
                status = "OK" if nb_lignes > 0 else "KO"
            
            self.settings.generate_tx_exploit(
                self.spark, self.hive_utils, self.DATE_DEB_ALIM, 
                self.table_mart, status, nb_lignes, self.app_name
            )
            
            return True
            
        except Exception:
            self.settings.generate_tx_exploit(
                self.spark, self.hive_utils, self.DATE_DEB_ALIM,
                self.table_mart, "KO", 0, self.app_name
            )
            return False
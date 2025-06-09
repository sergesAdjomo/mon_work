# qualification_ols_star_model/dataframe.py
from pyspark.sql import DataFrame
import traceback
from traitement_spark.code.settings import Settings
from traitement_spark.code.utils import CommonUtils

from qualification_ols_star_model.constants.fields import FIELDS
from qualification_ols_star_model.transformations.dim_pm_bdt import prepare_dim_pm_bdt
from qualification_ols_star_model.transformations.dim_temps import prepare_dim_temps
from qualification_ols_star_model.transformations.dim_localisation import prepare_dim_localisation
from qualification_ols_star_model.transformations.ft_qualif_donnees_usage import prepare_ft_qualif_donnees_usage
from qualification_ols_star_model.utils.dataframe_helpers import join_star_model, validate_dataframe


class TraitementQualificationOLSStarModelDataFrame(CommonUtils):
    
    def __init__(self, spark, config):
        self.spark = spark
        self.config = config

        self.bv_pm: DataFrame = None
        self.bv_tiers: DataFrame = None
        self.bv_coord_postales: DataFrame = None
        self.bv_departement: DataFrame = None
        self.bv_region: DataFrame = None
        self.bv_requetes: DataFrame = None
        self.bv_finances: DataFrame = None

        self.dim_pm_bdt: DataFrame = None
        self.dim_temps: DataFrame = None
        self.dim_localisation: DataFrame = None
        self.ft_qualif_donnees_usage: DataFrame = None

        self.df: DataFrame = None

        super().__init__(self.spark, self.config)

        self.settings = Settings(self.config, self.logger, self.date_ctrlm)
        
    def prepare_star_model(self):
        try:
            if not self._validate_source_data():
                return self
            
            try:
                self.dim_pm_bdt = prepare_dim_pm_bdt(
                    self.spark, self.bv_pm, self.bv_tiers, self.date_ctrlm
                )
            except Exception:
                self.dim_pm_bdt = self._create_empty_dim_pm_bdt()
            
            try:
                self.dim_temps = prepare_dim_temps(
                    self.spark, self.date_ctrlm
                )
            except Exception:
                self.dim_temps = self._create_empty_dim_temps()
            
            try:
                if self.bv_coord_postales is not None and self.bv_coord_postales.count() > 0:
                    self.dim_localisation = prepare_dim_localisation(
                        self.dim_pm_bdt, self.bv_coord_postales, self.bv_departement, self.bv_region
                    )
                else:
                    self.dim_localisation = self._create_empty_dim_localisation()
            except Exception:
                self.dim_localisation = self._create_empty_dim_localisation()
            
            try:
                self.ft_qualif_donnees_usage = prepare_ft_qualif_donnees_usage(
                    self.dim_pm_bdt,
                    self.bv_requetes,
                    self.bv_finances
                )
            except Exception:
                self.ft_qualif_donnees_usage = self._create_empty_ft_qualif_donnees_usage()
            
            try:
                self.df = join_star_model(
                    self.ft_qualif_donnees_usage, 
                    self.dim_pm_bdt, 
                    self.dim_temps, 
                    self.dim_localisation
                )
            except Exception:
                self.df = self.ft_qualif_donnees_usage
            
        except Exception:
            pass
        
        return self
    
    def _create_empty_dim_pm_bdt(self):
        query = """
        SELECT 
            CAST(NULL AS STRING) AS SIREN,
            CAST(NULL AS STRING) AS annee,
            CAST(NULL AS STRING) AS Raison_sociale,
            CAST(NULL AS STRING) AS Sous_categorie,
            CAST(NULL AS STRING) AS Tete_de_groupe,
            CAST(NULL AS STRING) AS annee_mois,
            CAST(NULL AS STRING) AS annee_mois_SIREN,
            CAST(NULL AS STRING) AS code_tiers
        WHERE 1=0
        """
        
        try:
            return self.hive_utils.hive_execute_query(query)
        except Exception:
            return None
    
    def _create_empty_dim_temps(self):
        from datetime import datetime
        
        current_date = datetime.now()
        current_year = current_date.year
        current_month = current_date.month
        annee_mois = f"{current_year}_{current_month:02d}"
        
        query = f"""
        SELECT 
            '{annee_mois}' AS annee_mois,
            '{current_year}' AS annee,
            '{current_month:02d}' AS mois
        """
        
        try:
            return self.hive_utils.hive_execute_query(query)
        except Exception:
            fallback_query = """
            SELECT 
                CAST(NULL AS STRING) AS annee_mois,
                CAST(NULL AS STRING) AS annee,
                CAST(NULL AS STRING) AS mois
            WHERE 1=0
            """
            
            try:
                return self.hive_utils.hive_execute_query(fallback_query)
            except Exception:
                return None
    
    def _create_empty_dim_localisation(self):
        query = """
        SELECT 
            CAST(NULL AS STRING) AS annee_mois_SIREN,
            CAST(NULL AS STRING) AS annee_mois,
            CAST(NULL AS STRING) AS adr_code_postal,
            CAST(NULL AS STRING) AS code_Departement,
            CAST(NULL AS STRING) AS Ville,
            CAST(NULL AS STRING) AS Region
        WHERE 1=0
        """
        
        try:
            return self.hive_utils.hive_execute_query(query)
        except Exception:
            return None
    
    def _create_empty_ft_qualif_donnees_usage(self):
        query = """
        SELECT 
            CAST(NULL AS STRING) AS annee_mois_SIREN,
            CAST(NULL AS STRING) AS annee_mois,
            CAST(NULL AS STRING) AS SIREN,
            CAST(NULL AS INT) AS nb_de_tiers,
            CAST(NULL AS DOUBLE) AS chiffre_d_affaire_moyen,
            CAST(NULL AS DOUBLE) AS montant_signe
        WHERE 1=0
        """
        
        try:
            return self.hive_utils.hive_execute_query(query)
        except Exception:
            return None
    
    def _validate_source_data(self):
        success = True
        
        if self.bv_pm is None:
            success = False
        else:
            pm_count = self.bv_pm.count()
            if pm_count == 0:
                pass
        
        if self.bv_tiers is None:
            success = False
        else:
            tiers_count = self.bv_tiers.count()
            if tiers_count == 0:
                pass
        
        if self.bv_coord_postales is None:
            pass
        else:
            cp_count = self.bv_coord_postales.count()
            if cp_count == 0:
                pass
        
        if self.bv_departement is None:
            pass
        else:
            dept_count = self.bv_departement.count()
        
        if self.bv_region is None:
            pass
        else:
            region_count = self.bv_region.count()
        
        return success
    
    def process(self):
        try:
            self.prepare_star_model()
            
            result = {}
            
            if self.dim_pm_bdt is not None:
                result["dim_pm_bdt"] = self.dim_pm_bdt
            
            if self.dim_temps is not None:
                result["dim_temps"] = self.dim_temps
            
            if self.dim_localisation is not None:
                result["dim_localisation"] = self.dim_localisation
            
            if self.ft_qualif_donnees_usage is not None:
                result["ft_qualif_donnees_usage"] = self.ft_qualif_donnees_usage
            
            if self.df is not None:
                result["qualification_ols_star_model"] = self.df
            
            return result
            
        except Exception:
            result = {}
            
            if hasattr(self, "dim_pm_bdt") and self.dim_pm_bdt is not None:
                result["dim_pm_bdt"] = self.dim_pm_bdt
            
            if hasattr(self, "dim_temps") and self.dim_temps is not None:
                result["dim_temps"] = self.dim_temps
            
            if hasattr(self, "dim_localisation") and self.dim_localisation is not None:
                result["dim_localisation"] = self.dim_localisation
            
            if hasattr(self, "ft_qualif_donnees_usage") and self.ft_qualif_donnees_usage is not None:
                result["ft_qualif_donnees_usage"] = self.ft_qualif_donnees_usage
            
            if hasattr(self, "df") and self.df is not None:
                result["qualification_ols_star_model"] = self.df
            
            return result
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
    """Classe principale pour la création du modèle en étoile."""
    
    def __init__(self, spark, config):
        self.spark = spark
        self.config = config

        # Tables sources
        self.bv_pm: DataFrame = None
        self.bv_tiers: DataFrame = None
        self.bv_coord_postales: DataFrame = None
        self.bv_departement: DataFrame = None
        self.bv_region: DataFrame = None
        
        # Tables pour les métriques (optionnelles)
        self.bv_requetes: DataFrame = None
        self.bv_finances: DataFrame = None

        # Tables du modèle en étoile
        self.dim_pm_bdt: DataFrame = None
        self.dim_temps: DataFrame = None
        self.dim_localisation: DataFrame = None
        self.ft_qualif_donnees_usage: DataFrame = None

        # Datamart final
        self.df: DataFrame = None

        super().__init__(self.spark, self.config)

        self.settings = Settings(self.config, self.logger, self.date_ctrlm)
        
    def prepare_star_model(self):
        """
        Prépare le modèle en étoile complet avec gestion des erreurs.
        """
        self.logger.info("Préparation du modèle en étoile")
        
        try:
            # Vérification des données sources
            if not self._validate_source_data():
                self.logger.error("Validation des données sources échouée")
                return self
            
            # Préparation des dimensions avec gestion des erreurs individuelles
            try:
                self.logger.info("Génération de la dimension DIM_PM_BDT")
                self.dim_pm_bdt = prepare_dim_pm_bdt(
                    self.spark, self.bv_pm, self.bv_tiers, self.date_ctrlm
                )
                self.logger.info(f"DIM_PM_BDT générée avec {self.dim_pm_bdt.count()} lignes")
            except Exception as e:
                self.logger.error(f"Erreur lors de la génération de DIM_PM_BDT: {e}")
                self.logger.error(traceback.format_exc())
                # Créer un DataFrame minimal pour continuer
                self.dim_pm_bdt = self._create_empty_dim_pm_bdt()
            
            try:
                self.logger.info("Génération de la dimension DIM_TEMPS")
                self.dim_temps = prepare_dim_temps(
                    self.spark, self.date_ctrlm
                )
                self.logger.info(f"DIM_TEMPS générée avec {self.dim_temps.count()} lignes")
            except Exception as e:
                self.logger.error(f"Erreur lors de la génération de DIM_TEMPS: {e}")
                self.logger.error(traceback.format_exc())
                # Créer un DataFrame minimal pour continuer
                self.dim_temps = self._create_empty_dim_temps()
            
            try:
                self.logger.info("Génération de la dimension DIM_LOCALISATION")
                if self.bv_coord_postales is not None and self.bv_coord_postales.count() > 0:
                    self.dim_localisation = prepare_dim_localisation(
                        self.dim_pm_bdt, self.bv_coord_postales, self.bv_departement, self.bv_region
                    )
                    self.logger.info(f"DIM_LOCALISATION générée avec {self.dim_localisation.count()} lignes")
                else:
                    self.logger.warning("Pas de données de coordonnées postales disponibles")
                    self.dim_localisation = self._create_empty_dim_localisation()
            except Exception as e:
                self.logger.error(f"Erreur lors de la génération de DIM_LOCALISATION: {e}")
                self.logger.error(traceback.format_exc())
                # Créer un DataFrame minimal pour continuer
                self.dim_localisation = self._create_empty_dim_localisation()
            
            try:
                self.logger.info("Génération de la table de faits FT_qualif_donnees_usage")
                self.ft_qualif_donnees_usage = prepare_ft_qualif_donnees_usage(
                    self.dim_pm_bdt,
                    self.bv_requetes,
                    self.bv_finances
                )
                self.logger.info(f"FT_qualif_donnees_usage générée avec {self.ft_qualif_donnees_usage.count()} lignes")
            except Exception as e:
                self.logger.error(f"Erreur lors de la génération de FT_qualif_donnees_usage: {e}")
                self.logger.error(traceback.format_exc())
                # Créer un DataFrame minimal pour continuer
                self.ft_qualif_donnees_usage = self._create_empty_ft_qualif_donnees_usage()
            
            # Assemblage du modèle en étoile
            try:
                self.logger.info("Création du modèle en étoile complet")
                self.df = join_star_model(
                    self.ft_qualif_donnees_usage, 
                    self.dim_pm_bdt, 
                    self.dim_temps, 
                    self.dim_localisation
                )
                self.logger.info(f"Modèle en étoile généré avec {self.df.count()} lignes")
            except Exception as e:
                self.logger.error(f"Erreur lors de la création du modèle en étoile: {e}")
                self.logger.error(traceback.format_exc())
                # En dernier recours, utiliser la table de faits comme modèle
                self.df = self.ft_qualif_donnees_usage
            
        except Exception as e:
            self.logger.error(f"Erreur générale lors de la préparation du modèle en étoile: {e}")
            self.logger.error(traceback.format_exc())
        
        return self
    
    def _create_empty_dim_pm_bdt(self):
        """Crée un DataFrame DIM_PM_BDT vide mais avec la structure correcte."""
        from pyspark.sql.types import StructType, StructField, StringType
        
        schema = StructType([
            StructField("SIREN", StringType(), True),
            StructField("annee", StringType(), True),
            StructField("Raison_sociale", StringType(), True),
            StructField("Sous_categorie", StringType(), True),
            StructField("Tete_de_groupe", StringType(), True),
            StructField("annee_mois", StringType(), True),
            StructField("annee_mois_SIREN", StringType(), True),
            StructField("code_tiers", StringType(), True)
        ])
        
        return self.spark.createDataFrame([], schema)
    
    def _create_empty_dim_temps(self):
        """Crée un DataFrame DIM_TEMPS vide mais avec la structure correcte."""
        from pyspark.sql.types import StructType, StructField, StringType
        from datetime import datetime
        
        schema = StructType([
            StructField("annee_mois", StringType(), True),
            StructField("annee", StringType(), True),
            StructField("mois", StringType(), True)
        ])
        
        # Au minimum, créer une ligne avec les données actuelles
        current_date = datetime.now()
        current_year = current_date.year
        current_month = current_date.month
        annee_mois = f"{current_year}_{current_month:02d}"
        
        data = [(annee_mois, str(current_year), str(current_month))]
        return self.spark.createDataFrame(data, schema)
    
    def _create_empty_dim_localisation(self):
        """Crée un DataFrame DIM_LOCALISATION vide mais avec la structure correcte."""
        from pyspark.sql.types import StructType, StructField, StringType
        
        schema = StructType([
            StructField("annee_mois_SIREN", StringType(), True),
            StructField("annee_mois", StringType(), True),
            StructField("adr_code_postal", StringType(), True),
            StructField("code_Departement", StringType(), True),
            StructField("Ville", StringType(), True),
            StructField("Region", StringType(), True)
        ])
        
        return self.spark.createDataFrame([], schema)
    
    def _create_empty_ft_qualif_donnees_usage(self):
        """Crée un DataFrame FT_qualif_donnees_usage vide mais avec la structure correcte."""
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
        
        schema = StructType([
            StructField("annee_mois_SIREN", StringType(), True),
            StructField("annee_mois", StringType(), True),
            StructField("SIREN", StringType(), True),
            StructField("nb_de_tiers", IntegerType(), True),
            StructField("chiffre_d_affaire_moyen", DoubleType(), True),
            StructField("montant_signe", DoubleType(), True)
        ])
        
        return self.spark.createDataFrame([], schema)
    
    def _validate_source_data(self):
        """
        Validation des données sources.
        """
        success = True
        
        # Vérifier que les DataFrames obligatoires sont disponibles
        if self.bv_pm is None:
            self.logger.error("Table BV_PM non disponible")
            success = False
        else:
            pm_count = self.bv_pm.count()
            self.logger.info(f"Nombre de lignes dans BV_PM: {pm_count}")
            if pm_count == 0:
                self.logger.warning("Table BV_PM est vide")
        
        if self.bv_tiers is None:
            self.logger.error("Table BV_TIERS non disponible")
            success = False
        else:
            tiers_count = self.bv_tiers.count()
            self.logger.info(f"Nombre de lignes dans BV_TIERS: {tiers_count}")
            if tiers_count == 0:
                self.logger.warning("Table BV_TIERS est vide")
        
        # Les autres tables sont optionnelles
        if self.bv_coord_postales is None:
            self.logger.warning("Table BV_COORDONNEES_POSTALES non disponible")
        else:
            cp_count = self.bv_coord_postales.count()
            self.logger.info(f"Nombre de lignes dans BV_COORDONNEES_POSTALES: {cp_count}")
            if cp_count == 0:
                self.logger.warning("Table BV_COORDONNEES_POSTALES est vide")
        
        if self.bv_departement is None:
            self.logger.warning("Table BV_DEPARTEMENT non disponible")
        else:
            dept_count = self.bv_departement.count()
            self.logger.info(f"Nombre de lignes dans BV_DEPARTEMENT: {dept_count}")
        
        if self.bv_region is None:
            self.logger.warning("Table BV_REGION non disponible")
        else:
            region_count = self.bv_region.count()
            self.logger.info(f"Nombre de lignes dans BV_REGION: {region_count}")
        
        return success
    
    def process(self):
        """
        Exécute le traitement complet.
        
        Returns:
            dict: Dictionnaire des DataFrames générés
        """
        try:
            # Préparer le modèle en étoile
            self.prepare_star_model()
            
            # Retourner tous les DataFrames
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
            
        except Exception as e:
            self.logger.error(f"Erreur lors du traitement: {e}")
            self.logger.error(traceback.format_exc())
            # Retourner les DataFrames disponibles même en cas d'erreur
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
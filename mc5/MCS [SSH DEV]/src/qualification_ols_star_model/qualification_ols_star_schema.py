"""
Module principal pour l'orchestration du modèle en étoile qualification_ols.

Ce module contient la classe principale qui orchestre la création
et le chargement de toutes les dimensions et de la table de faits.
"""

from pyspark.sql import SparkSession, DataFrame
from typing import Any, Optional

from .qualification_ols_config import TABLES
from .qualification_ols_dim_pm_bdt import create_dim_pm_bdt, save_dim_pm_bdt
from .qualification_ols_dim_temps import create_dim_temps, save_dim_temps
from .qualification_ols_dim_localisation import create_dim_localisation, save_dim_localisation
from .qualification_ols_ft_qualif_donnees_usage import create_ft_qualif_donnees_usage, save_ft_qualif_donnees_usage

class QualificationOLSStarSchema:
    """
    Classe principale pour la création et la gestion du modèle en étoile.
    
    Cette classe orchestre la création et le chargement des différentes
    dimensions et de la table de faits du modèle en étoile.
    """
    
    def __init__(self, spark: SparkSession, config: Any):
        """
        Initialise la classe avec les dépendances nécessaires.
        
        Args:
            spark: Session Spark active
            config: Configuration contenant les paramètres nécessaires
        """
        self.spark = spark
        self.config = config
        self.logger = config.logger
        self.db_lac = config.db_lac
        
        # DataFrames du modèle
        self.source_df: Optional[DataFrame] = None
        self.dim_pm_bdt: Optional[DataFrame] = None
        self.dim_temps: Optional[DataFrame] = None
        self.dim_localisation: Optional[DataFrame] = None
        self.fact_table: Optional[DataFrame] = None
        
    def extract_data(self):
        """
        Extrait les données source depuis le lac de données.
        
        Returns:
            self pour permettre le chaînage des méthodes
        """
        self.logger.info("Extraction des données source pour le modèle en étoile")
        self.source_df = self.spark.table(f"{self.db_lac}.qualification_ols")
        return self
        
    def transform_data(self):
        """
        Transforme les données source en dimensions et table de faits.
        
        Returns:
            self pour permettre le chaînage des méthodes
        """
        if self.source_df is None:
            raise ValueError("Les données source n'ont pas été extraites")
            
        self.logger.info("Transformation des données en modèle en étoile")
        
        # Création des dimensions
        self.dim_pm_bdt = create_dim_pm_bdt(self.source_df)
        self.dim_temps = create_dim_temps(self.source_df)
        self.dim_localisation = create_dim_localisation(self.source_df)
        
        # Création de la table de faits
        self.fact_table = create_ft_qualif_donnees_usage(self.source_df)
        
        return self
        
    def load_data(self):
        """
        Charge les dimensions et la table de faits dans le lac de données.
        
        Returns:
            self pour permettre le chaînage des méthodes
        """
        self.logger.info("Chargement du modèle en étoile dans le lac de données")
        
        # Vérification des DataFrames
        if any(df is None for df in [self.dim_pm_bdt, self.dim_temps, self.dim_localisation, self.fact_table]):
            raise ValueError("Certains DataFrames n'ont pas été créés")
            
        # Sauvegarde des dimensions
        save_dim_pm_bdt(self.dim_pm_bdt, self.spark, self.db_lac)
        save_dim_temps(self.dim_temps, self.spark, self.db_lac)
        save_dim_localisation(self.dim_localisation, self.spark, self.db_lac)
        
        # Sauvegarde de la table de faits
        save_ft_qualif_donnees_usage(self.fact_table, self.spark, self.db_lac)
        
        self.logger.info("Modèle en étoile chargé avec succès")
        return self
        
    def execute(self):
        """
        Exécute l'ensemble du pipeline ETL (Extract, Transform, Load).
        
        Returns:
            bool: True si l'exécution a réussi, False sinon
        """
        try:
            self.extract_data() \
                .transform_data() \
                .load_data()
                
            self.logger.info("Pipeline ETL du modèle en étoile exécuté avec succès")
            return True
        except Exception as e:
            self.logger.error(f"Erreur lors de l'exécution du pipeline ETL: {e}")
            raise

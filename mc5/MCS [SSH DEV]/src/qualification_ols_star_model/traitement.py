# qualification_ols_star_model/utils/traitement.py
import socket
import time
import traceback
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from typing import Dict, Any, Optional

from icdc.hdputils.hive import hiveUtils
from icdc.hdputils.impala import impalaUtils
from traitement_spark.code.settings import Settings
from traitement_spark.code.utils import CommonUtils

from qualification_ols_star_model.constants.fields import FIELDS
from qualification_ols_star_model.dataframe import TraitementQualificationOLSStarModelDataFrame


class TraitementQualificationOLSStarModel(CommonUtils):
    """Classe principale de traitement pour le modèle en étoile qualification_ols."""

    def __init__(self, spark, config=None):
        """
        Initialise le traitement.
        
        Args:
            spark: Session Spark
            config: Configuration
        """
        self.spark = spark
        self.config = config or {}
        super().__init__(self.spark, self.config)
        
        # Configuration et journalisation
        self._setup_logging()
        self._setup_variables()
        
        # Initialisation du hive_utils
        self.hive_utils = hiveUtils(self.spark)
        
    def _setup_logging(self):
        """Configure les paramètres de logging."""
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
        self.logger.info(f"INFO: DRIVER : {socket.gethostname()}")
        self.logger.info(f"INFO: fichier de LOG : {self.logger_local_path}")
        self.logger.info(f"Date Ctrlm : {self.date_ctrlm}")

    def _setup_variables(self):
        """
        Initialise les variables de configuration nécessaires au traitement.
        """
        self.timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        self.app_name = self.conf.get("DEFAULT", "APP_NAME")
        
        # Chemins et databases Hive
        self.db_hive_travail = self.conf.get("HIVE", "DB_HIVE_TRAVAIL")
        self.db_hive_lac = self.conf.get("HIVE", "DB_HIVE_LAC")
        self.db_lac_path = self.conf.get("HIVE", "DB_HIVE_LAC_PATH")
        self.lac_path = self.conf.get("HIVE", "LAC_PATH", fallback="/apps/hive/warehouse/")
        
        # Base de données contenant les tables sources (BV_PM, BV_TIERS, etc.)
        self.db_ct3 = self.conf.get("HIVE", "DB_SRC_CT3")
        
        # Variables de contrôle
        self.DATE_DEB_ALIM = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.DATE_FIN_ALIM = None
        
        self.logger.info(f"Base de données de travail: {self.db_hive_travail}")
        self.logger.info(f"Base de données LAC: {self.db_hive_lac}")
        self.logger.info(f"Chemin LAC: {self.lac_path}")
        
        # Configuration technique
        self.settings = Settings(self.conf, self.logger, self.date_ctrlm)
        
        # Tables du modèle en étoile
        self.df = lambda: None  # Namespace pour les DataFrames
        self.table_bv_coord_postales = None
        
    def process(self):
        """
        Méthode principale qui exécute la création et l'écriture des tables du schéma en étoile.
        """
        self.logger.info("Début du traitement qualification_ols_star_model (création des tables)")
        
        try:
            # 1. Lecture des tables sources
            self._read_source_tables()
            
            # 2. Préparation des DataFrames du modèle en étoile
            star_model_df_generator = TraitementQualificationOLSStarModelDataFrame(self.spark, self.config)
            
            # Transfert des DataFrames sources vers le générateur
            if hasattr(self, 'df'):
                for attr_name in dir(self.df):
                    if not attr_name.startswith('_') and hasattr(self.df, attr_name):
                        df_value = getattr(self.df, attr_name)
                        if isinstance(df_value, DataFrame):
                            setattr(star_model_df_generator, attr_name, df_value)
                            self.logger.info(f"DataFrame {attr_name} transféré vers le générateur.")
            
            # Génération des DataFrames du modèle en étoile
            tables_dict = star_model_df_generator.process()

            if not tables_dict:
                self.logger.warning("Aucun DataFrame n'a été généré.")
                raise ValueError("Dictionnaire de DataFrames vide.")

            # 3. Écriture des tables dans Hive
            self._write_tables_to_hive(tables_dict)
            
            # 4. Génération du rapport d'exécution
            nb_lignes = sum([df.count() if df is not None else 0 for df in tables_dict.values()])
            self._generate_tx_exploit(nb_lignes, "OK")
            
            self.logger.info("Traitement du modèle en étoile qualification_ols terminé avec succès.")
            return True
            
        except Exception as e:
            self.logger.error(f"Erreur lors du traitement du modèle en étoile qualification_ols : {str(e)}")
            self.logger.error(traceback.format_exc())
            self._generate_tx_exploit(0, "KO")
            return False
            
    def _read_source_tables(self):
        """
        Lecture des tables sources nécessaires au traitement.
        """
        self.logger.info("Lecture des tables sources...")
        
        # Initialisation de l'espace de noms pour les DataFrames
        self.df = lambda: None
        
        # Lecture de la table BV_PM (en fait bv_personne_morale_bdt d'après le code qui fonctionne)
        try:
            self.logger.info("Lecture de la table bv_personne_morale_bdt...")
            self.df.bv_pm = self.hive_utils.hive_read_table(self.db_ct3, "bv_personne_morale_bdt")
            
            if self.df.bv_pm is not None:
                self.logger.info("Schéma de la table bv_personne_morale_bdt:")
                self.df.bv_pm.printSchema()
                self.logger.info(f"Nombre de lignes dans bv_personne_morale_bdt: {self.df.bv_pm.count()}")
            else:
                self.logger.error("La table bv_personne_morale_bdt est None après lecture")
                raise ValueError("La table bv_personne_morale_bdt n'a pas pu être lue correctement")
        except Exception as e:
            self.logger.error(f"Erreur lors de la lecture de bv_personne_morale_bdt: {str(e)}")
            self.df.bv_pm = None
        
        # Lecture de la table BV_TIERS_BDT
        try:
            self.logger.info("Lecture de la table bv_tiers_bdt...")
            self.df.bv_tiers = self.hive_utils.hive_read_table(self.db_ct3, "bv_tiers_bdt")
            
            if self.df.bv_tiers is not None:
                self.df.bv_tiers = self.df.bv_tiers.select(
                    "code_tiers", "siren", "siret", "code_etatiers",
                    "date_creation_tiers", "date_fermeture_tiers"
                )
                
                self.logger.info("Schéma de la table bv_tiers_bdt:")
                self.df.bv_tiers.printSchema()
                
                columns_list = self.df.bv_tiers.columns
                self.logger.info(f"Colonnes disponibles dans bv_tiers_bdt: {columns_list}")
                
                self.logger.info(f"Nombre de lignes dans bv_tiers_bdt: {self.df.bv_tiers.count()}")
            else:
                self.logger.error("La table bv_tiers_bdt est None après lecture")
                raise ValueError("La table bv_tiers_bdt n'a pas pu être lue correctement")
        except Exception as e:
            self.logger.error(f"Erreur lors de la lecture de bv_tiers_bdt: {str(e)}")
            self.logger.error(traceback.format_exc())
            self.df.bv_tiers = None
        
        # Lecture de la table des coordonnées postales (essayer plusieurs variantes)
        coordpostales_variations = [
            "BV_COORDONNEES_POSTALES", 
            "BV_COORDONNEES_POSTAL", 
            "BV_COORD_POSTALES", 
            "BV_COORDONNEES_POST",
            "bv_coordonnees_postales"
        ]
        self.df.bv_coord_postales = None
        for table_name in coordpostales_variations:
            try:
                self.logger.info(f"Tentative de lecture de la table {table_name}...")
                self.df.bv_coord_postales = self.hive_utils.hive_read_table(self.db_ct3, table_name)
                self.table_bv_coord_postales = table_name
                self.logger.info(f"Table {table_name} trouvée et lue avec succès")
                self.logger.info(f"Nombre de lignes dans {table_name}: {self.df.bv_coord_postales.count()}")
                break
            except Exception as e:
                self.logger.warning(f"Table {table_name} non disponible: {str(e)}")
                continue
        
        if self.df.bv_coord_postales is None:
            self.logger.warning("Aucune table de coordonnées postales n'a pu être lue.")
            # Créer un DataFrame vide avec le schéma attendu pour éviter les erreurs en aval
            from pyspark.sql.types import StructType, StructField, StringType
            schema = StructType([
                StructField("code_postal", StringType(), True),
                StructField("id_commune", StringType(), True),
                StructField("nom_commune", StringType(), True),
                StructField("code_departement", StringType(), True)
            ])
            # Création du DataFrame vide via sparkSession
            self.df.bv_coord_postales = self.spark.createDataFrame([], schema)
            self.table_bv_coord_postales = "BV_COORDONNEES_POSTALES"

        # Lecture de la table bv_departement
        try:
            self.logger.info("Lecture de la table bv_departement...")
            self.df.bv_departement = self.hive_utils.hive_read_table(self.db_ct3, "bv_departement")
            if self.df.bv_departement is not None:
                self.logger.info("Schéma de la table bv_departement:")
                self.df.bv_departement.printSchema()
                self.logger.info(f"Nombre de lignes dans bv_departement: {self.df.bv_departement.count()}")
            else:
                self.logger.warning("La table bv_departement est None après lecture")
        except Exception as e:
            self.logger.error(f"Erreur lors de la lecture de bv_departement: {str(e)}")
            self.df.bv_departement = None
            
        # Lecture de la table bv_region
        try:
            self.logger.info("Lecture de la table bv_region...")
            self.df.bv_region = self.hive_utils.hive_read_table(self.db_ct3, "bv_region")
            if self.df.bv_region is not None:
                self.logger.info("Schéma de la table bv_region:")
                self.df.bv_region.printSchema()
                self.logger.info(f"Nombre de lignes dans bv_region: {self.df.bv_region.count()}")
            else:
                self.logger.warning("La table bv_region est None après lecture")
        except Exception as e:
            self.logger.error(f"Erreur lors de la lecture de bv_region: {str(e)}")
            self.df.bv_region = None

    def _write_tables_to_hive(self, tables_dict: Dict[str, DataFrame]):
        """
        Écriture des tables du modèle en étoile dans Hive.
        
        Args:
            tables_dict: Dictionnaire des DataFrames à écrire (nom_table -> DataFrame)
        """
        self.logger.info("Écriture des tables du modèle en étoile dans Hive...")
        tables_written_successfully = 0
        
        # Chemin de base pour les tables Hive
        hive_db_path = f"{self.lac_path}{self.db_lac}"
        self.logger.info(f"Chemin de base pour les tables Hive: {hive_db_path}")
        
        for table_name, df in tables_dict.items():
            if df is None:
                self.logger.warning(f"DataFrame pour {table_name} est None, ignore l'écriture.")
                continue
                
            try:
                # Affichage du schéma et du nombre de lignes
                self.logger.info(f"Schéma de la table {table_name}:")
                df.printSchema()
                
                nb_lignes = df.count()
                self.logger.info(f"Nombre de lignes dans {table_name}: {nb_lignes}")
                
                # Chemin complet de la table
                table_path = f"{hive_db_path}/{table_name}"
                
                # Écriture de la table avec hive_utils
                self.logger.info(f"Écriture de la table {self.db_lac}.{table_name} dans {table_path}...")
                self.hive_utils.hive_overwrite_table(
                    df,
                    self.db_lac,
                    table_name,
                    table_path,
                    True,  # Supprimer et recréer
                    None,  # Pas de partitionnement
                    "parquet",
                    False   # Pas d'ajout de colonnes de métadonnées
                )
                
                tables_written_successfully += 1
                self.logger.info(f"Table {table_name} écrite avec succès.")
                
            except Exception as e:
                self.logger.error(f"Erreur lors de l'écriture de {table_name}: {str(e)}")
                self.logger.error(traceback.format_exc())
        
        self.logger.info(f"Écriture des tables terminée. {tables_written_successfully} tables écrites avec succès.")
        return tables_written_successfully
    
    def _generate_tx_exploit(self, nb_lignes, status="OK"):
        """
        Génération du rapport d'exécution TX_EXPLOIT.
        
        Args:
            nb_lignes: Nombre de lignes traitées
            status: Statut de l'exécution (OK ou KO)
        """
        self.logger.info(f"Génération du TX_EXPLOIT (statut: {status}, nb_lignes: {nb_lignes})...")
        
        # Utilisation de la méthode standard de settings pour générer le TX_EXPLOIT
        try:
            self.settings.generate_tx_exploit(
                self.spark,
                self.hive_utils,
                self.DATE_DEB_ALIM,
                "qualification_ols_star_model",
                status,
                nb_lignes,
                self.app_name
            )
            self.logger.info("TX_EXPLOIT généré avec succès via settings.")
        except Exception as e:
            self.logger.error(f"Erreur lors de la génération du TX_EXPLOIT: {str(e)}")
            self.logger.error(traceback.format_exc())
    
    def build_mart(
        self,
        dim_pm_bdt: DataFrame,
        dim_localisation: DataFrame,
        dim_temps: DataFrame,
        ft_qualif_donnees_usage: DataFrame) -> DataFrame:
        """
        Version simplifiée pour diagnostic - ne fait que lire les premières lignes des tables.
        """
        self.logger.info("DIAGNOSTIC: Construction simplifiée de la MART pour diagnostic")
        
        # Afficher uniquement le nombre de lignes de chaque table
        try:
            self.logger.info(f"DIAGNOSTIC: Table dim_pm_bdt - Nombre de lignes: {dim_pm_bdt.count()}")
            # Afficher les premières lignes pour vérifier le contenu
            self.logger.info("DIAGNOSTIC: Voici un aperçu de dim_pm_bdt:")
            dim_pm_bdt.show(5)
        except Exception as e:
            self.logger.error(f"DIAGNOSTIC: Erreur lors de la lecture de dim_pm_bdt: {str(e)}")
        
        try:    
            self.logger.info(f"DIAGNOSTIC: Table dim_localisation - Nombre de lignes: {dim_localisation.count()}")
            # Afficher les premières lignes
            self.logger.info("DIAGNOSTIC: Voici un aperçu de dim_localisation:")
            dim_localisation.show(5)
        except Exception as e:
            self.logger.error(f"DIAGNOSTIC: Erreur lors de la lecture de dim_localisation: {str(e)}")
            
        try:
            self.logger.info(f"DIAGNOSTIC: Table dim_temps - Nombre de lignes: {dim_temps.count()}")
            # Afficher les premières lignes
            self.logger.info("DIAGNOSTIC: Voici un aperçu de dim_temps:")
            dim_temps.show(5)
        except Exception as e:
            self.logger.error(f"DIAGNOSTIC: Erreur lors de la lecture de dim_temps: {str(e)}")
            
        try:
            self.logger.info(f"DIAGNOSTIC: Table ft_qualif_donnees_usage - Nombre de lignes: {ft_qualif_donnees_usage.count()}")
            # Afficher les premières lignes
            self.logger.info("DIAGNOSTIC: Voici un aperçu de ft_qualif_donnees_usage:")
            ft_qualif_donnees_usage.show(5)
        except Exception as e:
            self.logger.error(f"DIAGNOSTIC: Erreur lors de la lecture de ft_qualif_donnees_usage: {str(e)}")
        
        # Ne pas faire de jointures complexes, juste retourner la table dim_pm_bdt pour test
        self.logger.info("DIAGNOSTIC: Création d'une MART simplifiée pour test")
        try:
            # Créer un DataFrame minimal pour test
            from pyspark.sql.functions import lit
            
            # Si ft_qualif_donnees_usage a au moins une ligne, l'utiliser comme base
            if ft_qualif_donnees_usage.count() > 0:
                self.logger.info("DIAGNOSTIC: Utilisation de ft_qualif_donnees_usage comme base")
                df_final = ft_qualif_donnees_usage.limit(100)
            # Sinon utiliser dim_pm_bdt
            elif dim_pm_bdt.count() > 0:
                self.logger.info("DIAGNOSTIC: Utilisation de dim_pm_bdt comme base")
                df_final = dim_pm_bdt.limit(100)
            # Si tout échoue, créer un DataFrame artificiel
            else:
                self.logger.info("DIAGNOSTIC: Création d'un DataFrame artificiel")
                df_final = self.spark.spark.createDataFrame([("test",)], ["test_col"])
                
            # Ajouter une colonne de diagnostic
            df_final = df_final.withColumn("diagnostic_col", lit("test"))
            
            self.logger.info("DIAGNOSTIC: DataFrame final créé avec succès")
            df_final.printSchema()
            df_final.show(5)
            
            return df_final
            
        except Exception as e:
            self.logger.error(f"DIAGNOSTIC: Erreur lors de la création de la MART simplifiée: {str(e)}")
            # En cas d'échec, retourner un DataFrame minimal
            self.logger.info("DIAGNOSTIC: Création d'un DataFrame de secours")
            return self.spark.spark.createDataFrame([("backup",)], ["backup_col"])

    def save_to_hive(self, df: DataFrame):
        """
        Sauvegarde le DataFrame final dans la table Hive cible.
        """
        self.logger.info(f"Sauvegarde de la MART dans {self.db_mart}.{self.table_mart}")
        
        try:
            # Utiliser hive_utils comme dans star_model
            self.hive_utils.hive_overwrite_table(
                df,
                self.db_mart,
                self.table_mart,
                f"{self.db_mart}/{self.table_mart}",  # Chemin HDFS
                True,   # Supprimer et recréer pour garantir le bon ordre des colonnes
                None,   # Pas de partitionnement
                "parquet",
                False
            )
            self.logger.info(f"Table {self.table_mart} écrite avec succès dans {self.db_mart}")
        except Exception as e:
            self.logger.error(f"Erreur lors de l'écriture de la table {self.table_mart}: {e}")
            self.logger.error(traceback.format_exc())
            raise


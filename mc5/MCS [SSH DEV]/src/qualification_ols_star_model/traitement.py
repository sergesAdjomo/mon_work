import socket
import time
import traceback
from datetime import datetime

from icdc.hdputils.hive import hiveUtils
from traitement_spark.code.settings import Settings
from traitement_spark.code.utils import CommonUtils

from qualification_ols_star_model.constants.fields import FIELDS
from qualification_ols_star_model.dataframe import TraitementQualificationOLSStarModelDataFrame


class TraitementQualificationOLSStarModel(CommonUtils):
    """Classe principale de traitement pour le modèle en étoile qualification_ols."""

    def __init__(self, spark, config):
        """
        Initialise le traitement.
        
        Args:
            spark: Session Spark
            config: Configuration
        """
        self.spark = spark
        self.config = config
        super().__init__(self.spark, self.config)

        # Configuration et journalisation
        self._setup_logging()
        self._setup_variables()
        
        # Dataframe handler
        self.df = TraitementQualificationOLSStarModelDataFrame(self.spark, self.config)

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
        """Configure les variables pour le traitement."""
        # Variables d'environnement
        self.env = self.conf.get("DEFAULT", "ENVIRONMENT")
        self.db_ct3 = self.conf.get("HIVE", "DB_SRC_CT3")
        
        # Utiliser db_dev_mc5_travail au lieu de db_dev_mc5
        self.db_lac_path = self.conf.get("HIVE", "DB_HIVE_TRAVAIL_PATH")
        self.db_lac = "db_dev_mc5_travail"  # Pour écrire les tables dans db_dev_mc5_travail
        self.logger.info(f"Base de données cible configurée à: {self.db_lac}")
        
        self.settings = Settings(self.conf, self.logger, self.date_ctrlm)

        # informations techniques
        self.DATE_DEB_ALIM = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Noms des tables source (chaînes de caractères, pas des attributs de données)
        self.table_bv_pm = "bv_personne_morale_bdt"
        self.table_bv_tiers = "bv_tiers_bdt"
        self.table_bv_coord_postales = "bv_coordonnees_postales"
        self.table_bv_departement = "bv_departement"
        self.table_bv_region = "bv_region"

        # Noms des tables en étoile
        self.table_mart = "qualification_ols_star_model"  # Vue globale
        self.dim_pm_bdt_table = "dim_pm_bdt"
        self.dim_temps_table = "dim_temps"
        self.dim_localisation_table = "dim_localisation"
        self.ft_qualif_donnees_usage_table = "ft_qualif_donnees_usage"

        # Initialisation du hive_utils
        self.hive_utils = hiveUtils(self.spark)

    def submit(self):
        """
        Exécute le traitement principal.
        
        Returns:
            dict: Dictionnaire des DataFrames générés
        """
        self.logger.info("START PROCESSING QUALIFICATION OLS STAR MODEL DATAMART")
        try:
            # Lire les tables sources et préparer les données
            self._read_source_tables()
            
            # Traiter les données et générer le modèle en étoile
            dataframes = self.df.process()
            
            self.logger.info("QUALIFICATION OLS STAR MODEL SUCCESSFULLY PROCESSED")
            return dataframes
            
        except Exception as e:
            self.logger.error(f"PROCESSING ERROR: {e}")
            self.logger.error(traceback.format_exc())
            raise

    def _read_source_tables(self):
        """Lecture des tables sources avec les colonnes nécessaires."""
        # Colonnes requises pour chaque table
        bv_pm_col = [
            FIELDS.get("siren"),
            "siret",
            FIELDS.get("denom_unite_legale"),
            FIELDS.get("sous_cat"),
            FIELDS.get("etat_admin"),
            FIELDS.get("is_tete_groupe"),
            FIELDS.get("is_ols"),
            FIELDS.get("etab_siege"),
        ]

        bv_tiers_col = [
            FIELDS.get("code_tiers"),
            FIELDS.get("siren"),
            "siret",
            FIELDS.get("code_etatiers"),
        ]

        bv_coord_postales_col = [
            FIELDS.get("code_tiers"),
            FIELDS.get("adr_code_postal"),
            FIELDS.get("lib_bureau_distrib"),
            FIELDS.get("dat_horodat"),
        ]

        bv_departement_col = [
            FIELDS.get("code_departement"),
            FIELDS.get("code_region"),
        ]

        bv_region_col = [
            FIELDS.get("code_region"),
            FIELDS.get("lib_clair_region"),
        ]

        # Lecture des tables principales, tenter les lectures individuelles
        self.df.bv_pm = None
        self.df.bv_tiers = None
        self.df.bv_coord_postales = None
        self.df.bv_departement = None
        self.df.bv_region = None

        # Lire la table bv_personne_morale_bdt
        try:
            self.logger.info(f"Lecture de la table {self.table_bv_pm}")
            self.df.bv_pm = self.read_table(self.db_ct3, self.table_bv_pm).select(*bv_pm_col)
            self.logger.info(f"Table {self.table_bv_pm} lue avec succès")
        except Exception as e:
            self.logger.error(f"Erreur lors de la lecture de la table {self.table_bv_pm}: {e}")
            # La table est essentielle, nous ne pouvons pas continuer sans elle
            raise

        # Lire la table bv_tiers_bdt
        try:
            self.logger.info(f"Lecture de la table {self.table_bv_tiers}")
            self.df.bv_tiers = self.read_table(self.db_ct3, self.table_bv_tiers).select(*bv_tiers_col)
            self.logger.info(f"Table {self.table_bv_tiers} lue avec succès")
        except Exception as e:
            self.logger.error(f"Erreur lors de la lecture de la table {self.table_bv_tiers}: {e}")
            # La table est essentielle, nous ne pouvons pas continuer sans elle
            raise

        # Tenter de lire les coordonnées postales avec différents noms possibles
        coordpostales_variations = [
            self.table_bv_coord_postales,
            "bv_coordonnee_postale",
            "BV_COORDONNEES_POSTALES",
            "bv_coordonnees_postal",
            "coordonnees_postales"
        ]

        for table_name in coordpostales_variations:
            try:
                self.logger.info(f"Tentative de lecture de la table {table_name}")
                self.df.bv_coord_postales = self.read_table(self.db_ct3, table_name).select(*bv_coord_postales_col)
                self.logger.info(f"Table {table_name} lue avec succès")
                break
            except Exception as e:
                self.logger.warning(f"Table {table_name} non trouvée : {e}")
                continue

        if self.df.bv_coord_postales is None:
            self.logger.error("Impossible de trouver une table de coordonnées postales")
            # Nous pouvons continuer sans cette table, mais certaines fonctionnalités seront limitées
            self.logger.warning("Le modèle en étoile sera construit sans informations de localisation")

        # Lire la table bv_departement
        try:
            self.logger.info(f"Lecture de la table {self.table_bv_departement}")
            self.df.bv_departement = self.read_table(self.db_ct3, self.table_bv_departement).select(*bv_departement_col)
            self.logger.info(f"Table {self.table_bv_departement} lue avec succès")
        except Exception as e:
            self.logger.error(f"Erreur lors de la lecture de la table {self.table_bv_departement}: {e}")
            # Nous pouvons continuer sans cette table, mais certaines fonctionnalités seront limitées

        # Lire la table bv_region
        try:
            self.logger.info(f"Lecture de la table {self.table_bv_region}")
            self.df.bv_region = self.read_table(self.db_ct3, self.table_bv_region).select(*bv_region_col)
            self.logger.info(f"Table {self.table_bv_region} lue avec succès")
        except Exception as e:
            self.logger.error(f"Erreur lors de la lecture de la table {self.table_bv_region}: {e}")
            # Nous pouvons continuer sans cette table, mais certaines fonctionnalités seront limitées

    def write_table(self, df, hive_database, table_name, table_path, drop_and_recreate=False, partitionByColumns=None, hive_format="parquet"):
        """
        Écrit une table dans Hive en gérant les erreurs.
        
        Args:
            df: DataFrame à écrire
            hive_database: Base Hive cible
            table_name: Nom de la table
            table_path: Chemin de la table
            drop_and_recreate: Si True, supprime et recrée la table
            partitionByColumns: Colonnes de partition
            hive_format: Format Hive
        """
        self.logger.info(
            f"Debut fonction hive_overwrite_table - avec les parms : \n \
                                hive_database={hive_database}  \n \
                                table_name={table_name}  \n \
                                table_path={table_path}  \n \
                                drop_and_recreate_table={drop_and_recreate}\n \
                                partitionByColumns={partitionByColumns}\n \
                                hive_format={hive_format}  \n \
                                overwrite_only_partitions=False  "
        )
        
        try:
            # Vérifier si le DataFrame est valide avant d'écrire
            if df is None:
                self.logger.error(f"DataFrame est None pour la table {table_name}")
                return
                
            # Vérifier si le DataFrame est vide
            nb_lignes_traitees = df.count()
            if nb_lignes_traitees == 0:
                self.logger.warning(f"DataFrame vide pour la table {table_name}")
                # On peut quand même continuer dans certains cas pour les tables vides
            
            # Écrire la table
            self.hive_utils.hive_overwrite_table(
                df,
                hive_database,
                table_name,
                table_path,
                drop_and_recreate,
                partitionByColumns,
                hive_format,
                False,
            )
            
            # Essayer de synchroniser avec Impala seulement si impala_utils est disponible
            try:
                if hasattr(self, 'impala_utils') and self.impala_utils is not None:
                    self.impala_utils.impala_synchro_table(hive_database, table_name, True, True)
                    self.impala_utils.impala_compute_sats(hive_database, table_name)
                else:
                    self.logger.warning(f"impala_utils non disponible, synchronisation et calcul stats ignorés pour {table_name}")
            except Exception as impala_error:
                self.logger.warning(f"Erreur lors de la synchronisation Impala (non bloquante): {impala_error}")
            
            return nb_lignes_traitees
        except Exception as e:
            self.logger.error(f"unable to continue due to issues related to : {e}")
            return 0

    def _write_tables(self, dataframes):
        """
        Écrit les tables du modèle en étoile dans Hive.
        Version simplifiée qui se concentre uniquement sur les tables individuelles.
        
        Args:
            dataframes: Dictionnaire des DataFrames à écrire
        """
        # Écrire les dimensions et la table de faits individuelles
        self.logger.info(f"Écriture des tables dans {self.db_lac}")
        
        # Compter le total des lignes pour le suivi
        total_lignes = 0
        
        # Vérifier et écrire les dimensions individuelles
        dim_tables = [
            ("dim_pm_bdt", self.dim_pm_bdt_table),
            ("dim_temps", self.dim_temps_table),
            ("dim_localisation", self.dim_localisation_table),
            ("ft_qualif_donnees_usage", self.ft_qualif_donnees_usage_table)
        ]
        
        # Écrire chaque table individuellement
        for key, table in dim_tables:
            try:
                df = dataframes.get(key)
                if df is not None:
                    # Compter les lignes et journaliser
                    row_count = df.count()
                    total_lignes += row_count  # Additionner pour le suivi global
                    self.logger.info(f"Table {table} contient {row_count} lignes")
                    
                    # Écrire la table
                    self.hive_utils.hive_overwrite_table(
                        df,
                        self.db_lac,
                        table,
                        f"{self.db_lac_path}/{table}",
                        False,
                        None,
                        "parquet",
                        False,
                    )
                    self.logger.info(f"Table {table} écrite avec succès dans {self.db_lac}")
                else:
                    self.logger.warning(f"DataFrame {key} est None, table {table} non créée")
            except Exception as e:
                self.logger.error(f"Erreur lors de l'écriture de la table {table}: {e}")
                self.logger.error(traceback.format_exc())
        
        # Retourner le nombre total de lignes écrites
        return total_lignes

    def process(self):
        """Point d'entrée principal pour le traitement complet.
        Suit exactement le même modèle que les autres modules (qualification_ols, service_innovant, etc.)
        """
        self.logger.info("Computing NB_LIGN_TRT...")
        
        # Obtenir tous les dataframes nécessaires
        dataframes = self.submit()
        
        # Écrire les tables dimensionnelles et la table de faits
        try:
            self._write_tables(dataframes)
            
            # Pour la table finale du modèle (si elle existe)
            model_df = dataframes.get("model")
            if model_df is not None:
                NB_LIGN_TRT = model_df.count()
                self.logger.info(f"Modèle qualification_ols_star_model contient {NB_LIGN_TRT} lignes")
                
                # Générer TX_EXPLOIT avec statut OK
                self.settings.generate_tx_exploit(
                    self.spark,  # Utiliser sparkUtils directement
                    self.hive_utils,
                    self.DATE_DEB_ALIM,
                    self.table_mart,
                    "OK",
                    NB_LIGN_TRT,
                    self.app_name,
                )
            else:
                self.logger.warning("La table finale du modèle n'a pas été générée")
                
                # Compter au moins le nombre de lignes dans la table de faits
                facts_df = dataframes.get("ft_qualif_donnees_usage")
                if facts_df is not None:
                    NB_LIGN_TRT = facts_df.count()
                else:
                    NB_LIGN_TRT = 0
                    
                # Générer TX_EXPLOIT avec le statut approprié
                self.settings.generate_tx_exploit(
                    self.spark,  # Utiliser sparkUtils directement
                    self.hive_utils,
                    self.DATE_DEB_ALIM,
                    self.table_mart,
                    "KO" if NB_LIGN_TRT == 0 else "OK",
                    NB_LIGN_TRT,
                    self.app_name,
                )
                
        except Exception as e:
            self.logger.error(f"Erreur lors du traitement: {e}")
            
            # Gestion de l'erreur comme dans qualification_ols
            facts_df = dataframes.get("ft_qualif_donnees_usage")
            if facts_df is not None:
                NB_LIGN_TRT = facts_df.count()
            else:
                NB_LIGN_TRT = 0
                
            # Générer TX_EXPLOIT même en cas d'erreur
            if NB_LIGN_TRT > 0:
                self.settings.generate_tx_exploit(
                    self.spark,  # Utiliser sparkUtils directement
                    self.hive_utils,
                    self.DATE_DEB_ALIM,
                    self.table_mart,
                    "OK",
                    NB_LIGN_TRT,
                    self.app_name,
                )
            else:
                self.settings.generate_tx_exploit(
                    self.spark,  # Utiliser sparkUtils directement
                    self.hive_utils,
                    self.DATE_DEB_ALIM,
                    self.table_mart,
                    "KO",
                    NB_LIGN_TRT,
                    self.app_name,
                )
            
            # Ne pas propager l'exception
            return False
            
        return True

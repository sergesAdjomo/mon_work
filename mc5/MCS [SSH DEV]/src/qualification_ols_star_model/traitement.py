# qualification_ols_star_model/traitement.py
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
        self.db_lac_path = self.conf.get("HIVE", "DB_HIVE_LAC_PATH")
        self.db_lac = self.conf.get("HIVE", "DB_LAC")
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

        # Lecture des tables avec optimisation des colonnes
        self.df.bv_pm = self.read_table(self.db_ct3, self.table_bv_pm).select(*bv_pm_col)
        self.df.bv_tiers = self.read_table(self.db_ct3, self.table_bv_tiers).select(*bv_tiers_col)
        self.df.bv_coord_postales = self.read_table(
            self.db_ct3, self.table_bv_coord_postales
        ).select(*bv_coord_postales_col)
        self.df.bv_departement = self.read_table(
            self.db_ct3, self.table_bv_departement
        ).select(*bv_departement_col)
        self.df.bv_region = self.read_table(
            self.db_ct3, self.table_bv_region
        ).select(*bv_region_col)

    def _write_tables(self, dataframes):
        """
        Écrit les tables du modèle en étoile dans Hive.
        
        Args:
            dataframes: Dictionnaire des DataFrames à écrire
        """
        # Écriture des dimensions et de la table de faits
        self.write_table(
            dataframes["dim_pm_bdt"],
            self.db_lac,
            self.dim_pm_bdt_table,
            f"{self.db_lac_path}/{self.dim_pm_bdt_table}",
        )
        
        self.write_table(
            dataframes["dim_temps"],
            self.db_lac,
            self.dim_temps_table,
            f"{self.db_lac_path}/{self.dim_temps_table}",
        )
        
        self.write_table(
            dataframes["dim_localisation"],
            self.db_lac,
            self.dim_localisation_table,
            f"{self.db_lac_path}/{self.dim_localisation_table}",
        )
        
        self.write_table(
            dataframes["ft_qualif_donnees_usage"],
            self.db_lac,
            self.ft_qualif_donnees_usage_table,
            f"{self.db_lac_path}/{self.ft_qualif_donnees_usage_table}",
        )
        
        # Écriture de la vue finale du modèle en étoile
        self.write_table(
            dataframes["qualification_ols_star_model"],
            self.db_lac,
            self.table_mart,
            f"{self.db_lac_path}/{self.table_mart}",
        )

    def process(self):
        """Point d'entrée principal pour le traitement complet."""
        self.logger.info("STARTING QUALIFICATION OLS STAR MODEL PROCESS")
        start_time = time.time()
        
        try:
            # Exécution du traitement principal
            dataframes = self.submit()
            
            # Compter les lignes pour le monitoring
            nb_lignes = dataframes["qualification_ols_star_model"].count() if "qualification_ols_star_model" in dataframes else 0
            self.logger.info(f"QUALIFICATION OLS STAR MODEL GENERATED WITH {nb_lignes} ROWS")
            
            # Écriture des tables
            self._write_tables(dataframes)
            
            # Génération du TX_EXPLOIT avec statut OK
            self._generate_tx_exploit("OK", nb_lignes)
            
            # Log de performance
            execution_time = time.time() - start_time
            self.logger.info(f"QUALIFICATION OLS STAR MODEL SUCCESSFULLY COMPLETED IN {execution_time:.2f} SECONDS")
            
        except Exception as e:
            # Log de l'erreur
            self.logger.error(f"ERROR IN QUALIFICATION OLS STAR MODEL PROCESS: {e}")
            self.logger.error(traceback.format_exc())
            
            # Génération du TX_EXPLOIT avec statut KO
            self._generate_tx_exploit("KO", 0)
            
            # Relancer l'exception pour signaler l'échec
            raise

    def _generate_tx_exploit(self, status, nb_lignes):
        """
        Génère l'entrée TX_EXPLOIT pour le suivi du traitement.
        Version simplifiée qui ne dépend pas de settings.py.
        
        Args:
            status: Statut du traitement (OK/KO)
            nb_lignes: Nombre de lignes traitées
        """
        self.logger.info(f"GENERATING TX_EXPLOIT WITH STATUS: {status}, ROWS: {nb_lignes}")
        
        try:
            # Récupérer la session Spark
            spark_session = self.spark.sparkSession if hasattr(self.spark, 'sparkSession') else self.spark
            
            # Préparer les données pour TX_EXPLOIT
            from pyspark.sql import Row
            import datetime
            
            # Format de date pour la fin
            date_fin = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
            
            # Préparer la ligne pour TX_EXPLOIT
            row = Row(
                code_appli="mc5",
                date_debut=self.DATE_DEB_ALIM,
                date_fin=date_fin,
                id_job=spark_session.sparkContext.applicationId,
                nb_lignes_traitees=nb_lignes,
                nom_app=self.app_name,
                nom_job=self.app_name,
                statut=status,
                table_traitee=self.table_mart
            )
            
            # Créer un DataFrame avec cette ligne
            df = spark_session.createDataFrame([row])
            
            # Afficher pour le log
            df.show()
            
            # Écrire dans la table TX_EXPLOIT si possible
            try:
                tx_exploit_table = self.conf.get("HIVE", "tx_exploit") 
                db_travail = self.conf.get("HIVE", "db_travail")
                
                # Écrire dans la table TX_EXPLOIT
                df.write.format("hive").mode("append").saveAsTable(f"{db_travail}.{tx_exploit_table}")
                
                self.logger.info(f"TX_EXPLOIT écrit avec succès dans {db_travail}.{tx_exploit_table}")
                
            except Exception as e:
                self.logger.error(f"Impossible d'écrire dans TX_EXPLOIT: {e}")
                # Ne pas faire échouer le job si TX_EXPLOIT échoue
        
        except Exception as e:
            self.logger.error(f"ERREUR LORS DE LA GÉNÉRATION DU TX_EXPLOIT: {e}")
            self.logger.error(traceback.format_exc())
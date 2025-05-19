# qualification_ols_star_model/main.py
import logging

from icdc.hdputils.configuration import confUtils
from icdc.hdputils.hdfs import hdfsUtils
from icdc.hdputils.hive import hiveUtils
from icdc.hdputils.spark import sparkUtils

from qualification_ols_star_model.traitement import TraitementQualificationOLSStarModel

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def execute_star_model_pipeline():
    """
    Fonction principale pour exécuter le pipeline de création du modèle en étoile.
    """
    logger.info("Démarrage du pipeline qualification_ols_star_model")
    
    # Initialisation des utilitaires
    conf_utils = confUtils()
    hdfs_utils = hdfsUtils()
    spark_utils = sparkUtils()
    hive_utils = hiveUtils(spark_utils)

    # Exécution du traitement
    qualification_ols_star = TraitementQualificationOLSStarModel(spark_utils, conf_utils).process
    qualification_ols_star()
    
    logger.info("Pipeline qualification_ols_star_model terminé avec succès")

if __name__ == "__main__":
    logger.info("demarrage du job qualification_ols_star_model")

    # Initialisation des utilitaires
    conf_utils = confUtils()
    hdfs_utils = hdfsUtils()
    spark_utils = sparkUtils()
    hive_utils = hiveUtils(spark_utils)

    # Exécution du traitement
    qualification_ols_star = TraitementQualificationOLSStarModel(spark_utils, conf_utils).process
    qualification_ols_star()

    logger.info("Job qualification_ols_star_model terminé avec succès")
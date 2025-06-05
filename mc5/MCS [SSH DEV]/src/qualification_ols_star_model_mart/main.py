import logging

from icdc.hdputils.configuration import confUtils
from icdc.hdputils.hdfs import hdfsUtils
from icdc.hdputils.hive import hiveUtils
from icdc.hdputils.spark import sparkUtils

from qualification_ols_star_model_mart.traitement import TraitementQualificationOLSStarModelMart

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    logger.info("Démarrage du job qualification_ols_star_model_mart")

    # Initialisation des utilitaires
    conf_utils = confUtils()
    hdfs_utils = hdfsUtils()
    spark_utils = sparkUtils()
    hive_utils = hiveUtils(spark_utils)

    # Exécution du traitement
    qualification_ols_mart = TraitementQualificationOLSStarModelMart(spark_utils, conf_utils)
    result = qualification_ols_mart.process()
    
    if result:
        logger.info("Job qualification_ols_star_model_mart terminé avec succès")
    else:
        logger.error("Job qualification_ols_star_model_mart terminé avec des erreurs")
        exit(1)  # Code d'erreur si le traitement a échoué

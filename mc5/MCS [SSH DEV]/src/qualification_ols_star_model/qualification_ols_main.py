"""
Point d'entrée principal pour l'exécution du modèle en étoile qualification_ols.

Ce fichier contient la fonction principale qui orchestre l'exécution
du pipeline de création du modèle en étoile.
"""

import logging
from icdc.hdputils.configuration import confUtils
from icdc.hdputils.spark import sparkUtils
from icdc.hdputils.hive import hiveUtils
from pyspark.sql import SparkSession
from typing import Any, Dict, Optional

from .qualification_ols_star_schema import QualificationOLSStarSchema

def execute_star_model_pipeline(spark=None, conf=None):
    """
    Fonction principale d'exécution du pipeline de modèle en étoile.
    
    Args:
        spark: Session Spark (créée si non fournie)
        conf: Configuration (créée si non fournie)
        
    Returns:
        bool: True si l'exécution a réussi, False sinon
    """
    if spark is None:
        from icdc.hdputils.spark import sparkUtils
        spark = sparkUtils()
        
    if conf is None:
        from icdc.hdputils.configuration import confUtils
        conf = confUtils()
    
    logger = conf.logger
    logger.info("Démarrage du pipeline de modèle en étoile qualification_ols")
    
    star_schema = QualificationOLSStarSchema(spark, conf)
    
    try:
        success = star_schema.execute()
        
        if success:
            logger.info("Pipeline terminé avec succès")
        else:
            logger.error("Échec du pipeline")
            
        return success
    except Exception as e:
        logger.error(f"Erreur lors de l'exécution du pipeline: {str(e)}")
        raise e

if __name__ == "__main__":
    # Configuration pour l'exécution directe
    conf_utils = confUtils()
    hdfs_utils = hdfsUtils()
    spark_utils = sparkUtils()
    hive_utils = hiveUtils(spark_utils)
    
    # Exécution du pipeline
    execute_star_model_pipeline(spark_utils, conf_utils)
    
    print("Traitement du modèle en étoile terminé")

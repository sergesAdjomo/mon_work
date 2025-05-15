# qualification_ols_star_model/main.py
import logging

from icdc.hdputils.configuration import confUtils
from icdc.hdputils.hdfs import hdfsUtils
from icdc.hdputils.hive import hiveUtils
from icdc.hdputils.spark import sparkUtils

from qualification_ols_star_model.traitement import TraitementQualificationOLSStarModel

if __name__ == "__main__":
    logging.info("demarrage du job qualification_ols_star_model")

    conf_utils = confUtils()
    hdfs_utils = hdfsUtils()
    spark_utils = sparkUtils()
    hive_utils = hiveUtils(spark_utils)

    qualification_ols_star = TraitementQualificationOLSStarModel(spark_utils, conf_utils).process
    qualification_ols_star()

    print("Job qualification_ols_star_model terminé avec succès")
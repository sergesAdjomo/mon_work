# qualification_ols_star_model/main.py
import logging

from icdc.hdputils.configuration import confUtils
from icdc.hdputils.hdfs import hdfsUtils
from icdc.hdputils.hive import hiveUtils
from icdc.hdputils.spark import sparkUtils

from qualification_ols_star_model.traitement import TraitementQualificationOLSStarModel

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def execute_star_model_pipeline():
    conf_utils = confUtils()
    hdfs_utils = hdfsUtils()
    spark_utils = sparkUtils()
    hive_utils = hiveUtils(spark_utils)

    qualification_ols_star = TraitementQualificationOLSStarModel(spark_utils, conf_utils).process
    qualification_ols_star()

if __name__ == "__main__":
    conf_utils = confUtils()
    hdfs_utils = hdfsUtils()
    spark_utils = sparkUtils()
    hive_utils = hiveUtils(spark_utils)

    qualification_ols_star = TraitementQualificationOLSStarModel(spark_utils, conf_utils).process
    qualification_ols_star()
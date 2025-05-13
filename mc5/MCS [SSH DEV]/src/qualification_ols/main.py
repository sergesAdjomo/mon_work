 
import logging

from icdc.hdputils.configuration import confUtils
from icdc.hdputils.hdfs import hdfsUtils
from icdc.hdputils.hive import hiveUtils
from icdc.hdputils.spark import sparkUtils

from qualification_ols.qualification_ols import TraitementQualificationOLS

if __name__ == "__main__":
    logging.info("demarrage du job²")

    conf_utils = confUtils()
    hdfs_utils = hdfsUtils()
    spark_utils = sparkUtils()
    hive_utils = hiveUtils(spark_utils)

    qualification_ols = TraitementQualificationOLS(spark_utils, conf_utils).process
    qualification_ols()

    print("After calling func()")

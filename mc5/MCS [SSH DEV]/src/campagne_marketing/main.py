 
import logging

from icdc.hdputils.configuration import confUtils
from icdc.hdputils.hdfs import hdfsUtils
from icdc.hdputils.hive import hiveUtils
from icdc.hdputils.spark import sparkUtils

from campagne_marketing.campagne_marketing import TraitementCampagneMarketing

if __name__ == "__main__":
    logging.info("demarrage du job²")

    conf_utils = confUtils()
    hdfs_utils = hdfsUtils()
    spark_utils = sparkUtils()
    hive_utils = hiveUtils(spark_utils)

    campagne_marketing = TraitementCampagneMarketing(spark_utils, conf_utils).process
    campagne_marketing()

    print("After calling func()")

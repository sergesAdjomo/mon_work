 
import logging

from icdc.hdputils.configuration import confUtils
from icdc.hdputils.hdfs import hdfsUtils
from icdc.hdputils.hive import hiveUtils
from icdc.hdputils.spark import sparkUtils

from service_innovant.service_innovant import TraitementServiceInnovant

if __name__ == "__main__":
    logging.info("Start processing datamart: service innovant")

    conf_utils = confUtils()
    hdfs_utils = hdfsUtils()
    spark_utils = sparkUtils()
    hive_utils = hiveUtils(spark_utils)

    service_innovant = TraitementServiceInnovant(spark_utils, conf_utils).process
    service_innovant()

    print("End of processing")

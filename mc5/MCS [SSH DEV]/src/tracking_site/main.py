 
import logging

from icdc.hdputils.configuration import confUtils
from icdc.hdputils.hdfs import hdfsUtils
from icdc.hdputils.hive import hiveUtils
from icdc.hdputils.spark import sparkUtils

from tracking_site.tracking_site import TraitementTrackingSite
from tracking_site.tracking_site_url import TraitementTrackingSiteUrl

if __name__ == "__main__":
    logging.info("Start processing datamart: tracking site")

    conf_utils = confUtils()
    hdfs_utils = hdfsUtils()
    spark_utils = sparkUtils()
    hive_utils = hiveUtils(spark_utils)

    tracking_site = TraitementTrackingSite(spark_utils, conf_utils).process
    tracking_site()
    tracking_site_url = TraitementTrackingSiteUrl(spark_utils, conf_utils).process
    tracking_site_url()

    print("End of processing")

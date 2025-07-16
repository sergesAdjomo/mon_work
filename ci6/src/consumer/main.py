import logging
import sys
from icdc.hdputils.configuration import confUtils
from icdc.hdputils.hdfs import hdfsUtils
from icdc.hdputils.spark import sparkUtils

from consumer.consumer import ConsumerCiam


if __name__ == "__main__":

    conf_utils = confUtils()
    hdfs_utils = hdfsUtils()
    spark_utils = sparkUtils()
    # Recuperation du parametre flux depuis les arguments
    flux = sys.argv[4][len("--flux=") :]
    logging.info(f"Flux specifier : {flux}")

    logging.info("Start Consumer")
    # Kafka -> Zone brute
    ConsumerCiam(spark_utils, conf_utils, flux).submit()    
    
    print("End of processing")
#-*- coding: utf-8 -*-
import sys

import os
import os.path
import time

import time
import json
import logging
from datetime import datetime
import socket

# contient l'ensemble des librairies presentes dans les differents composants applicatifs zipés par le shell d'execution spark_submit.sh
sys.path.append('job.zip')

from icdc.hdputils.configuration import confUtils
from icdc.hdputils.hive import hiveUtils
from icdc.hdputils.spark import sparkUtils
from icdc.hdputils.hdfs import hdfsUtils 
from traitement_spark.code.settings import *
#Lancement du processus metier
conf_utils = confUtils()
hdfs_utils = hdfsUtils()
spark_utils = sparkUtils()
hive_utils = hiveUtils(spark_utils)


if __name__ == "__main__":
    """
    ceci est un job très basique pour juste faire un exemple de lancement de spark submit et lancer des TU
    
    """
    #Lancement du processus metier
    conf_utils = confUtils()
    hdfs_utils = hdfsUtils()
    spark_utils = sparkUtils()
    hive_utils = hiveUtils(spark_utils)
    sys.excepthook = conf_utils.handle_exception
    
    # Creation du logger
    logger = conf_utils.logger
    # Creation de la variable global config et chargement de la configuration du projet
    logger.info("INFO: recuperation de la configuration")
    config = conf_utils.config    
    #recupération de date ctrlm
    date_ctrlm= conf_utils.dateCrtlm()
    
    #initialisation de variables
    start_time = time.time()
    timestamp=datetime.now().strftime("%Y%m%d-%H%M%S") 
        
    logger.info("DEBUT: execution du job")
    logger.info("INFO: recuperation des logs")
    
    #recuperation des parametres de configuration
    app_name=config.get('DEFAULT', 'APP_NAME')
    log_local_name=config.get('LOG', 'LOG_FILENAME')
    log_hdfs_name=timestamp+'_'+log_local_name
    log_hdfs_path=config.get('LOG', 'HDFS_LOG_PATH')+log_hdfs_name
    log_local_path=config.get('LOG', 'LOCAL_LOG_PATH')+log_local_name
    
    logger.info("INFO: recuperation des variables de configuration")
    logger.info("INFO: app_name :"+app_name)
    logger.info("INFO: DRIVER : "+socket.gethostname())
    logger.info("INFO: fichier de log :"+log_local_path)
    
    logger.info("Date Ctrlm : %s",date_ctrlm)
    spark = spark_utils.getSparkInstance()
    spark.conf.set("spark.es.net.ssl", "true")
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    def get_credential(credential_provider_path, credential_alias):
        breakpoint()
        conf = spark.sparkContext._jsc.hadoopConfiguration()
        conf.set('hadoop.security.credential.provider.path', f"jceks://hdfs{credential_provider_path}")

        try:
            credential_raw = conf.getPassword(credential_alias)
        except Exception as e:
            logger.error(f"error: {e}")
        return credential_raw


    kafka_password = get_credential("/dev/ep/flux_entrant/ci6/credentials/svckf2-ci6-in02-ciam.password.jceks", "svckf2-ci6-in02-ciam.password.alias")

    logger.info(f"PASSWORD : {kafka_password}")
    
    #Lancer l'exemple
    logger.info("FIN: ####### main: Fin du job  : {0} s".format((time.time() - start_time )))
    #************************************************************************************************************************
    logger.info(">>> DONE  line_number ")

    
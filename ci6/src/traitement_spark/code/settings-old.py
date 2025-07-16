#!/usr/hdp/current/spark2-client python
#-*- coding: utf-8 -*-
import time
import datetime
from datetime import datetime
import re
import unicodedata
import numpy as np
import logging
from time import strftime,localtime
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import sum,avg,max,count
from  icdc.kafkalogginghandler import KafkaLoggingHandler

from icdc.hdputils.configuration import confUtils
conf_utils = confUtils()

#####################     INIT des Conf Utils  ######################
logger = conf_utils.logger
config = conf_utils.config
date_ctrlm= conf_utils.dateCrtlm()


#####################     INIT SPARK CONTEXT  ######################
sc = SparkContext.getOrCreate()
#SPARK_NB_EXECUTOR = int(sc.getConf().get('spark.executor.instances') )
#SPARK_NB_CORES_EXECUTOR = int(sc.getConf().get('spark.executor.cores') )



##################################################################################################
#####################                      Variable appli                   ######################
##################################################################################################
app_name=config.get('DEFAULT', 'APP_NAME')
log_local_path=config.get('LOG', 'LOCAL_LOG_PATH')
hdfs_path_brute = str(config.get('HDFS' , 'HDFS_PATH_BRUTE' )).lower()


##################################################################################################
#####################                          HIVE                         ######################
##################################################################################################
hive_database_brute = str(config.get('HIVE' , 'DB_HIVE_BRUTE' )).lower()
hive_path_brute = str(config.get('HIVE' , 'DB_HIVE_BRUTE_PATH' )).lower()
hive_database_travail = str(config.get('HIVE' , 'DB_HIVE_TRAVAIL' )).lower()
hive_path_traitement = str(config.get('HIVE' , 'DB_HIVE_TRAVAIL_PATH' )).lower()
hive_database_lac = str(config.get('HIVE' , 'DB_HIVE_LAC' )).lower()
hive_path_lac = str(config.get('HIVE' , 'DB_HIVE_LAC_PATH' )).lower()

##################################################################################################
#####################                          KAFKA LOGGING                ######################
##################################################################################################
try:
    kafka_log_is_active = config.getboolean('KAFKA_LOG', 'KAFKA_LOG_IS_ACTIVE')
except:
    kafka_log_is_active = False
kafka_log_bootstrap_servers = config.get('KAFKA_LOG', 'KAFKA_LOG_BOOTSTRAP_SERVERS').lower()
kafka_log_topic = config.get('KAFKA_LOG', 'KAFKA_LOG_TOPIC').lower()
kafka_user = config.get('KAFKA_LOG', 'KAFKA_USER').lower()
schema_registry_url = config.get("KAFKA_LOG", "SCHEMA_REGISTRY_URL").lower()
certificat = config.get('KAFKA_LOG', 'CERTIFICAT').lower()
credential_path = str(config.get('KAFKA_LOG', 'CREDENTIAL_PATH')).lower()
credential_provider_alias = str(config.get('KAFKA_LOG', 'CREDENTIAL_PROVIDER_ALIAS')).lower()
kafka_log_on_thread = config.getboolean('KAFKA_LOG', 'KAFKA_LOG_ON_THREAD')

logger.info("======================> kafka_log_is_active : {0}".format(str(kafka_log_is_active)))
logger.info("======================> kafka_log_on_thread : {0}".format(str(kafka_log_on_thread)))

def get_msg_kafka_logger(id_msg = "",
                         code_appli = app_name,
                         date_ctrlm = date_ctrlm, 
                         date_fonctionnelle = "", 
                         cat_app_fonc = "", 
                         module_name = "ingesteur_csv", 
                         process_id = "", 
                         process_desc = "", 
                         state = "", 
                         process_time_start = "", 
                         process_time_end = "", 
                         data_input = "",
                         data_output = "", 
                         champ_1 = "", 
                         champ_2 = "", 
                         champ_3 = "", 
                         value = "" ):
    dic = { "id_msg" : id_msg, 
            "code_appli" : code_appli, 
            "date_ctrlm" : date_ctrlm, 
            "date_fonctionnelle" : date_fonctionnelle,
            "cat_app_fonc" : cat_app_fonc, 
            "module_name" : module_name,
            "process_id" : process_id, 
            "process_desc" : process_desc, 
            "state" : state, 
            "process_time_start" : process_time_start, 
            "process_time_end" : process_time_end, 
            "data_input"  : data_input,
            "data_output" : data_output,
            "champ_1" : champ_1, 
            "champ_2" : champ_2, 
            "champ_3" : champ_3, 
            "value" : value 
        }
    
    return(dic)

def get_credential(credential_provider_path,credential_name):
    spark = SparkSession.builder.getOrCreate()
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    conf.set('hadoop.security.credential.provider.path',credential_provider_path)
    credential_raw = conf.getPassword(credential_name)
    credential_str = ''
    for i in range(credential_raw.__len__()):
        credential_str = credential_str + str(credential_raw.__getitem__(i))
    return credential_str

#Logger sur kafka ou sur logger standard
if (kafka_log_is_active) :
    kh = KafkaLoggingHandler(bootstrap_servers = kafka_log_bootstrap_servers,
        topic = kafka_log_topic,
        kafka_user = kafka_user,
        kafka_password = get_credential(credential_path,credential_provider_alias),
        schema_registry_url = schema_registry_url,
        certificats = certificat,
        key_col = None,
        acks = 1, 
        on_thread = kafka_log_on_thread
    )
        
    kafkaLogger = logging.getLogger("KafkaLogger")
    kafkaLogger.setLevel(logging.DEBUG)
    kafkaLogger.addHandler(kh)
    logger.info("utilisation du kafkaLogger")
else:
    kafkaLogger=logger # default value:    
    

def generate_tx_exploit(spark, hive_utils, date_debut, table_traitee, statut, nb_lignes_traitees):
    """
        Cette fonction permet d'inserer une ligne d'exploitation dans la table tx_exploit du code appli concerne.
    """
       
    tx_exploit_data = {
            'id_job':str(spark._jsc.sc().applicationId()),
            'nom_job':str(app_name),
            'nom_app':str(app_name),
            'code_appli':str(app_name),
            'table_traitee':str(table_traitee),
            'date_debut':date_debut,
            'statut':str(statut),
            'date_fin':datetime.now().strftime("%Y%m%d%H%M%S"),
            'nb_lignes_traitees':int(nb_lignes_traitees)
    }
    
    df_txexploit = spark.createDataFrame([tx_exploit_data])
    df_txexploit.show(100,True)
   
    hive_utils.hive_append_table(df_txexploit, hive_database_travail,"tx_exploit", table_path=hive_path_traitement+"/{0}".format("tx_exploit"))
    return True

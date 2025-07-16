#-*- coding: utf-8 -*-
import sys

import os.path

import time
import json
import logging
from datetime import datetime
import socket
from wa7.producerMatomo.producer import common
from wa7.producerMatomo.producer import aspirateur 
from wa7.producerMatomo.producer import parameter as param 
import wa7.producerMatomo.tools.configuration as cfg 
from wa7.producerMatomo.producer import kafkasender 

#logger = logging.getLogger(__name__)
logger = cfg.loggerInstance()

if __name__ == "__main__":

    #initialisation de variables
    start_time = time.time()
    
    logger.info("DEBUT: execution du job")
    
    # initiation kafka 
    kafka_sender = kafkasender.KafkaSender()
    
    # Pour chaque idsite on reccupère 
    for idsite in param.matomo_list_id_site:
        # Reccupération du custom dimension 
        jsonresponse=aspirateur.request_custom_dimension(idsite)
        for jsonrow in jsonresponse:
            kafka_sender.send(jsonrow)
        #reccupération des goals
        jsonresponse=aspirateur.request_goal(idsite)
        for jsonrow in jsonresponse:
            kafka_sender.send(jsonrow)
        # Reccupération des log_visit. 
        aspirateur.aspirateur(idsite, kafka_sender)

    logger.info("====> FIN : run_ingestion - temps(s) : {0} ".format(str(time.time() - start_time)))
    logger.info("FIN: éxecution du job")

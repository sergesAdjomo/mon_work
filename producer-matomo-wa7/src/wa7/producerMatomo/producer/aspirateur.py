 
import logging
import hashlib
import time
import re
import subprocess
from json.decoder import JSONDecodeError
from csv import DictReader
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import requests
import os 
import os.path
import sys
from urllib3.exceptions import InsecureRequestWarning

import wa7.producerMatomo.tools.configuration as cfg 
from wa7.producerMatomo.producer import parameter as param 
from wa7.producerMatomo.producer import common

logger = cfg.loggerInstance()
#logger = logging.getLogger(__name__)

def aspirateur(idsite, kafka_sender):
    logger.info("debut : job aspirateur de l'idsite {}".format(idsite))
    
    # initiation paramètre de pagination 
    Condition_sortit=0
    #max_date=set()
    #max_date.add(common.read_filterdate(param.file_path_filterdate+str(idsite)+'.txt',START_DATE=param.start_date))
    
    while Condition_sortit == 0:
        
        # lecture des params pour la lecture des données 
        logger.info("lecture des params pour la lecture des données ")
        LAST_FILTER_OFFSET=common.read_lastoffset(param.file_path_last_offset+str(idsite)+'.txt')
        FILTER_DATE=common.read_filterdate(param.file_path_filterdate+str(idsite)+'.txt',START_DATE=param.start_date)
        
        # réccupération des données depuis l'api matomo
        jsonresponse=request_matomo(idsite,FILTER_DATE,LAST_FILTER_OFFSET)
        
        #envoie les données vers kafka et reccupération du nombre d'offset
        FILTER_OFFSET_RECUP=handle_jsonresponse(idsite,kafka_sender,jsonresponse)
        
        LAST_FILTER_OFFSET=int(LAST_FILTER_OFFSET)+FILTER_OFFSET_RECUP
        # enrégistrement de la dernière valeur de LAST_FILTER_OFFSET
        common.write_on_filesystem(str(LAST_FILTER_OFFSET), param.file_path_last_offset+str(idsite)+'.txt')
        
        #condition de sortie 
        if FILTER_OFFSET_RECUP < param.matomo_filter_limit:
            # enrégistrement de la prochaine date serverDatePretty  pour servir de FILTER_DATE.
            nextdate=common.nextDate(FILTER_DATE,1 )
            if FILTER_DATE < param.yesterday: 
                common.write_on_filesystem(nextdate, param.file_path_filterdate+str(idsite)+'.txt')
                common.write_on_filesystem('0', param.file_path_last_offset+str(idsite)+'.txt')
            else:
                Condition_sortit=1
    
    logger.info("fin : job aspirateur de l'idsite {}".format(idsite))
    
    
def request_matomo(idsite,FILTER_DATE,LAST_FILTER_OFFSET):
    
    logger.info("info : job request_matomo de l'idsite {0} avec filterdate {1} et le dernier offset {2}".format(idsite,FILTER_DATE,LAST_FILTER_OFFSET))
    
    headers = {
            'https': '//sps-support-interne-rec.serv.cdc.fr/xxx/xxx/?module=API&method=Live.getLastVisitsDetails&idSite=xx&period=range&date=xxxx-xx-xx,xxxx-xx-xx&format=JSON&filter_limit=xxx&filter_offset=z&token_auth=yyyyyyyyyyyy',
        }
    
    params = (
                ('module', 'API'),
                ('method', 'Live.getLastVisitsDetails'),
                ('idSite', idsite),
                ('period', 'range'),
                ('date', '{0},{1}'.format(FILTER_DATE, FILTER_DATE)),
                ('format', 'JSON'),
                ('filter_limit', param.matomo_filter_limit),
                ('filter_offset', LAST_FILTER_OFFSET),
                ('token_auth', param.matomo_api_token),
            )
    
    requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)
    response = requests.get(param.matomo_api_url, headers=headers, params=params, verify=False, auth=(param.matomo_sps_user, param.matomo_sps_mdp))
    jsonresponse=response.json()
    
    return jsonresponse

def handle_jsonresponse(idsite,kafka_sender,jsonresponse): 
    
    try:
        jsonresponse['result']=='error'
        sys.exit(jsonresponse['message'])
    except:
        #on a pas d'erreur 
        pass
        
    # initialisation du nombre d'offset reccupéré
    FILTER_OFFSET_RECUP=0
        
    for jsonrow in jsonresponse:
        
        try:
            if str(jsonrow["idSite"])==str(idsite):
                FILTER_OFFSET_RECUP=FILTER_OFFSET_RECUP+1

                # publication vers kafka 
                kafka_sender.send(jsonrow)
                #max_date.add(common.format_date(jsonrow['serverDatePretty']))
        except Exception as e: 
            logger.error(e)
            sys.exit(jsonresponse)
              
        
    logger.info("nombre de offset reccupéré {}".format(FILTER_OFFSET_RECUP))
    
    return FILTER_OFFSET_RECUP


def request_custom_dimension(idsite):
    
    logger.info("info : job request_custom_dimension de l'idsite {0}".format(idsite))
    
    headers = {
            'https': '//sps-support-interne-rec.serv.cdc.fr/xxx/xxx/?module=API&method=CustomDimensions.getConfiguredCustomDimensions&idSite=xx&format=JSON&filter_limit=xxx&filter_offset=z&token_auth=yyyyyyyyyyyy',
        }
    
    params = (
                ('module', 'API'),
                ('method', 'CustomDimensions.getConfiguredCustomDimensions'),
                ('idSite', idsite),
                ('format', 'JSON'),
                ('token_auth', param.matomo_api_token),
            )
    
    requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)
    response = requests.get(param.matomo_api_url, headers=headers, params=params, verify=False, auth=(param.matomo_sps_user, param.matomo_sps_mdp))
    jsonresponse=response.json()
    
    return jsonresponse


def request_goal(idsite):
    
    logger.info("info : job request_custom_dimension de l'idsite {0}".format(idsite))
    
    headers = {
            'https': '//sps-support-interne-rec.serv.cdc.fr/xxx/xxx/?module=API&method=Goals.getGoals&idSite=xx&format=JSON&token_auth=yyyyyyyyyyyy',
        }
    
    params = (
                ('module', 'API'),
                ('method', 'Goals.getGoals'),
                ('idSite', idsite),
                ('format', 'JSON'),
                ('token_auth', param.matomo_api_token),
            )
    
    requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)
    response = requests.get(param.matomo_api_url, headers=headers, params=params, verify=False, auth=(param.matomo_sps_user, param.matomo_sps_mdp))
    jsonresponse=response.json()
    
    return jsonresponse

import sys
import os.path

import requests
import base64
import json
from datetime import datetime, date, time
import calendar

import time
import logging
import logging.config
import subprocess
import argparse
import configparser # ConfigParser in python2 to configparser in python 3


# Declaration de quelques variables globales
name = None
datectrlm = None
config = None
logger = None
parser = None
conn = None
curs = None
ctrlm = None
jsonFilePath = None

log_filename = None

#logger = logging.getLogger(__name__)


def configInstance():
    global config
    if config==None:
        args = checkParameters()
        config = loadJobConfiguration(args.jobconfiguration)
    return config

def loadJobConfiguration(configFilename):
    """
        Chargement de la configuration propre au job et affichage des
        differentes sections et options
    """
    global config
    config = configparser.ConfigParser() # ConfigParser in python2 to configparser in python 3
    config.read(configFilename)

    logger.debug('>>> Banniere lancement du job...')
    logger.debug('>>> Python version : {}'.format(sys.version_info))
    logger.debug('\tConfiguration applicative job...')
    logger.debug("\t\tConfiguration File Sections : {0} : ".format(config.sections()))

    for section in config.sections():
        logger.debug("\t\tListe des options pour la section {0} : {1}".format(section, config.options(section)))
        for option in config.options(section):
            logger.debug("\t\t\t{0} : {1}".format(option, config.get(section, option)))
            print("\t\t\t{0} : {1}".format(option, config.get(section, option)))
    return config

def loggerInstance():
    global logger
    args = checkParameters()
    if logger==None:
        subprocess.Popen(['chmod', 'u+w', 'batch.log'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        logger = logging.config.fileConfig(args.logconfiguration)
        logger = logging.getLogger('WA7')
    logger.info("log filename : {0} - conf filename : {1}".format(args.logconfiguration, args.jobconfiguration))
    return logger

def checkParameters():
    """
        Verification des parametres fournis au job
    """
    global parser, name, datectrlm
    parser = argparse.ArgumentParser('batch-app')

    # required parameters
    requiredNamed = parser.add_argument_group('required named arguments')
    conf_file_arg = requiredNamed.add_argument("-j", "--jobconfiguration", help="configuration job filename",
                                               required=False)
    log_file_arg = requiredNamed.add_argument("-l", "--logconfiguration", help="configuration logging filename",
                                              required=False)

    ctrl_m_arg = requiredNamed.add_argument("-m", "--datectrlm", help="Date crtlM",
                                              required=True)
    
    matomo_id_site = requiredNamed.add_argument("-i", "--matomo_id_site", help="matomo_id_site",
                                              required=True)
    
    matomo_api_url = requiredNamed.add_argument("-a", "--matomo_api_url", help="matomo_api_url",
                                              required=True)    
    
    matomo_api_token = requiredNamed.add_argument("-t", "--matomo_api_token", help="matomo_api_token",
                                              required=True)   
    
    matomo_sps_user = requiredNamed.add_argument("-s", "--matomo_sps_user", help="matomo_sps_user",
                                              required=True) 
    
    matomo_sps_mdp = requiredNamed.add_argument("-p", "--matomo_sps_mdp", help="matomo_sps_mdp",
                                              required=True) 
    
    # lancement des exceptions si les parametres obligatoires ne sont pas renseigne
    args = parser.parse_args()
    
    # controles fichier de logging
    if not str(args.logconfiguration):
        raise argparse.ArgumentError(log_file_arg, "Le fichier de configuration du logger est requis !")

    if (not os.path.exists(str(args.logconfiguration))) or (not os.path.isfile(str(args.logconfiguration))):
        raise argparse.ArgumentError(log_file_arg, "Le fichier de logging est introuvable !")

    # controles fichier des configuration job
    if not str(args.jobconfiguration):
        raise argparse.ArgumentError(conf_file_arg, "Le fichier de configuration du job est requis !")

    if (not os.path.exists(str(args.jobconfiguration))) or (not os.path.isfile(str(args.jobconfiguration))):
        raise argparse.ArgumentError(conf_file_arg, "Le fichier de configuration du job est introuvable !")


    return args



 
import logging
import hashlib
import time
from datetime import datetime, timedelta
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
import logging
import wa7.producerMatomo.tools.configuration as cfg 

#logger = logging.getLogger(__name__)
logger = cfg.loggerInstance()

def yesterdayDatectrlm(datectrlm):
    """
        Fonction qui renvoie la date contrlm -1
        
        @param datectrlm : date ctrlm fournit en paramètre. 
    """
    
    yesterday=datetime.strptime(datectrlm, '%Y%m%d') - timedelta(1)
    yesterday=datetime.strftime(yesterday, '%Y-%m-%d')
    
    return yesterday

def todayDatectrlm(datectrlm):
    """
        Fonction qui renvoie la date contrlm 
        
        @param datectrlm : date ctrlm fournit en paramètre. 
    """
    
    today=datetime.strptime(datectrlm, '%Y%m%d')
    today=datetime.strftime(today, '%Y-%m-%d')
    
    return today

def nextDate(date_st, nbre):
    """
        Fonction qui renvoie la date en string de la date en paramètre décalé du nombre de jour donnée
        
        @param date_st : date fournit en paramètre. 
        @param nbre : entier donne le nombre de jour après  
    """
    date=datetime.fromisoformat(date_st).date()+ timedelta(days=int(nbre))
    
    return date.strftime('%Y-%m-%d')
    

def read_lastoffset(file_path):
    """
        Fonction qui renvoie la valeur du dernier offset reccupéré 
        
        @param file_path : le chimin où la valeur est stocké. 
    """
    logger.info("debut: read_lastoffset ")

    if os.path.isfile(file_path):
        f = open(file_path, "r")
        s_output=f.read()
    else:
        s_output=0

    logger.info("fin: read_lastoffset ")
    return s_output

def read_filterdate(file_path,START_DATE='2010-01-01'):
    """
        Fonction qui renvoie la valeur de la date pour filtrer les données dans matomo
        
        @param file_path : le chimin où lire la valeur. 
    """
    logger.info("debut: read_filterdate ")
    if os.path.isfile(file_path):
        f = open(file_path, "r")
        s_output=f.read()
    else:
        s_output=START_DATE

    logger.info("fin read_filterdate ")
    return s_output.strip()

def write_on_filesystem(value, file_path):
    """
        Fonction qui ecrit dans un fichier
        
        @param value : valeur à stocker. 
        @param file_path : fichier où stocker.
    """
    logger.info("debut: write_on_filesystem ")
    f = open(file_path, "w")
    f.write(value)
    f.close()
    logger.info("fin write_on_filesystem ")
    return

def format_date(x):
    """
        Fonction qui prend en entrée un format de date verbeux et renvoie au format XXXX-XX-XX
        
        @param x : la date. 
    """
    logger.info("Début fonction format_date " )

    mois={'January':'01',
          'February':'02',
          'March':'03',
          'April':'04',
          'May':'05',
          'June':'06',
          'July':'07',
          'August':'08',
          'September':'09',
          'October':'10',
          'November':'11',
          'December':'12'}

    def func_jour(x):

        if len(x.split(",")[1].split(" ")[2])==2:
            return x.split(",")[1].split(" ")[2]
        else:
            return "0"+x.split(",")[1].split(" ")[2]

    parse_x= x.split(",")[2].strip() +"-"+mois[x.split(",")[1].split(" ")[1]]+"-"+func_jour(x)
    logger.info("fin fonction format_date resultat {0}".format(parse_x))
    return parse_x



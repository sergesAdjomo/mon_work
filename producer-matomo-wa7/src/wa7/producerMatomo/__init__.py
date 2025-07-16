Python
#-*- coding: utf-8 -*-
import logging.config

log = logging.getLogger(__name__)


def setup_loggers(filename):
    try:
        logging.config.fileConfig(filename, disable_existing_loggers=False)
    except FileNotFoundError as e:
        log.error("Destination des logs inaccessible : %s", e.filename)
    else:
        log.info("Initialisation du logger réussie !")
        log.info("\t Configuration des logs : %s", filename)
        # TODO : parcourir l'ensemble de l'arbre des loggers pour être exhaustif.
        for handler in logging.getLogger().handlers:
            if type(handler) is logging.FileHandler:
                log.info("\t Destination des logs : %s", handler.baseFilename)
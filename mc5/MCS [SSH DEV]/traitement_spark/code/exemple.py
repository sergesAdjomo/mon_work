 
# -*- coding: utf-8 -*-

###Fonctions
from traitement_spark.code.settings import *

logger = conf_utils.logger


def jobexemple_agg(hive_utils, df, tabledest):
    try:
        t_start = int(time.time())
        now = strftime("%Y%m%d%H%M%S", localtime())
        logger.info("Lancement du job jobexemple_gg")
        df_agg = df.groupBy("dep").agg(count("*").alias("nbr"))
        hive_utils.hive_overwrite_table(
            df_agg,
            hive_database_lac,
            tabledest,
            table_path=hive_path_lac + "/{0}".format(tabledest),
            hive_format="parquet",
            drop_and_recreate_table=True,
        )
        t_end = int(time.time())
    except Exception as e:
        logger.error("erreur dans la creation de la table d'aggreation", str(e))

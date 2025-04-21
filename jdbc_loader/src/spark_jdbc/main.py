# ingesteur_spark_JDBC/src/spark_jdbc/main.py
import time

import sys

from spark_jdbc.code.settings import Settings

from spark_jdbc.code.params import Params

from spark_jdbc.code.runner import Runner

if __name__ == "__main__": #pragma: no cover
    start_time = time.time()
    
    settings = Settings()
    
    sys.excepthook = settings.conf_utils.handle_exception
    
    params = Params(settings)
    
    runner = Runner(params)
    
    runner.run()
    
    t_end=time.time()

#ingestion des logs presents sur le host du driver vers HDFS

#settings.conf_utils.put_log_hdfs()


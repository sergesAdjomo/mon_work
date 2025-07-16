# from pyspark.sql import SparkSession
# import pytest
# import sys
# import pkg_resources
# from datetime import datetime
# import shutil
# import uuid
# from pathlib import Path
# from jinja2 import Environment, FileSystemLoader


 
# @pytest.fixture(scope="session")
# def spark_session():

#    random_uuid = str(uuid.uuid4())
#    warehouse_path = Path('spark-warehouse', random_uuid).absolute()
#    metastore_path = Path('metastore_db', random_uuid).absolute()

#    print(metastore_path)

#    connection_url = f'jdbc:derby:;databaseName={str(metastore_path)};create=true'

#    spark = SparkSession \
#             .builder \
#             .appName("pytest") \
#             .master("local[2]") \
#             .config('spark.ui.enabled', False) \
#             .config('spark.ui.showConsoleProgress', False) \
#             .config('spark.executor.instances', 2) \
#             .config('spark.executor.cores', 2) \
#             .config('spark.sql.warehouse.dir', warehouse_path) \
#             .config('javax.jdo.option.ConnectionURL', connection_url) \
#             .enableHiveSupport() \
#             .getOrCreate()
#    spark.conf.set("spark.es.net.ssl", "true")

#    setup_hive_db(spark, warehouse_path)
#    setup_conf_util(warehouse_path)

#    yield spark
#    spark.sql("DROP DATABASE IF EXISTS db_dev_ci6 CASCADE")
#    spark.sql("DROP DATABASE IF EXISTS db_dev_ci6_param CASCADE")
#    spark.sql("DROP DATABASE IF EXISTS db_dev_ci6_travail CASCADE")
#    spark.stop()
#    shutil.rmtree(path=warehouse_path, ignore_errors=True)
#    shutil.rmtree(path=metastore_path, ignore_errors=True)
 

# def setup_hive_db(spark_session,warehouse_path):

#    spark_session.sql(f"CREATE DATABASE IF NOT EXISTS db_dev_ci6 LOCATION '{warehouse_path}/db_dev_ci6.db'")   
#    spark_session.sql(f"CREATE DATABASE IF NOT EXISTS db_dev_ci6_brute LOCATION '{warehouse_path}/db_dev_ci6_brute.db'")   
#    spark_session.sql(f"CREATE DATABASE IF NOT EXISTS db_dev_ci6_travail LOCATION '{warehouse_path}/db_dev_ci6_travail.db'")


# def setup_conf_util(warehouse_path):
#    env = Environment(loader=FileSystemLoader(Path('test/resources').absolute()))
#    template = env.get_template('test_ci6.conf.ini')
#    output = template.render(warehouse_path=warehouse_path)
#    path_test_resource = 'test.resources'


#    with open(pkg_resources.resource_filename(path_test_resource,'test_ci6.conf'), 'w') as f:
#       f.write(output)

#    name = "batch-app-ci6"
#    logconfiguration = pkg_resources.resource_filename(path_test_resource,'test_ci6_logging.conf')
#    jobconfiguration = pkg_resources.resource_filename(path_test_resource,'test_ci6.conf')
#    datectrlm = datetime.now().strftime('%Y%m%d')
#    sys.argv = [name, f"-j{jobconfiguration}", f"-l{logconfiguration}",f"-m{datectrlm}"]

import pytest
import sys
import pkg_resources
from datetime import datetime
import uuid
from pathlib import Path
from jinja2 import Environment, FileSystemLoader
from icdc.hdputils.spark import sparkUtils


@pytest.fixture(scope="session")
def spark_session():

   random_uuid = str(uuid.uuid4())
   arehouse_path = Path("spark-warehouse", random_uuid).absolute()
   metastore_path = Path("metastore_db", random_uuid).absolute()
   print(metastore_path)
    # connection_url = f'jdbc:derby:;databaseName={str(metastore_path)};create=true'
    # import pdb;pdb.set_trace()
   spark = sparkUtils()
    # spark.setSparkConf([("spark.es.net.ssl", True)]).config("spark.ui.enabled","false")
   yield spark
   """spark = SparkSession \
            .builder \
            .appName("pytest") \
            .master("local[2]") \
            .config('spark.ui.enabled', False) \
            .config('spark.ui.showConsoleProgress', False) \
            .config('spark.executor.instances', 2) \
            .config('spark.executor.cores', 2) \
            .config('spark.sql.warehouse.dir', warehouse_path) \
            .enableHiveSupport() \
            .getOrCreate()
            
            .config("spark.es.net.ssl", "true") \
   .config('javax.jdo.option.ConnectionURL', connection_url) \
   spark.setSparkConf("spark.es.net.ssl", "true")
   spark.setSparkConf("spark.hadoop.hive.exec.dynamic.partition", "true")
   spark.setSparkConf("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict")

   setup_hive_db(spark, warehouse_path)
   setup_conf_util(warehouse_path)

   yield spark
   spark.sql("DROP DATABASE IF EXISTS db_dev_mc5 CASCADE")
   spark.sql("DROP DATABASE IF EXISTS db_dev_mc5_param CASCADE")
   spark.sql("DROP DATABASE IF EXISTS db_dev_mc5_travail CASCADE")"""
   spark.spark.stop()
    # shutil.rmtree(path=warehouse_path, ignore_errors=True)
    # shutil.rmtree(path=metastore_path, ignore_errors=True)


"""def setup_hive_db(spark_session,warehouse_path):
   spark_session.sql(f"CREATE DATABASE IF NOT EXISTS db_dev_mc5 LOCATION '{warehouse_path}/db_dev_mc5.db'")   
   spark_session.sql(f"CREATE DATABASE IF NOT EXISTS db_dev_mc5_brute LOCATION '{warehouse_path}/db_dev_mc5_brute.db'")   
   spark_session.sql(f"CREATE DATABASE IF NOT EXISTS db_dev_mc5_travail LOCATION '{warehouse_path}/db_dev_mc5_travail.db'")"""


@pytest.fixture(scope="session")
def setup_conf_util():
   random_uuid = str(uuid.uuid4())
   warehouse_path = Path("spark-warehouse", random_uuid).absolute()
   env = Environment(loader=FileSystemLoader(Path("test/resources").absolute()))
   template = env.get_template("test_mc5.conf.ini")
   output = template.render(warehouse_path=warehouse_path)
   path_test_resource = "test.resources"

   with open(
        pkg_resources.resource_filename(path_test_resource, "test_mc5.conf"), "w"
   ) as f:
     f.write(output)

   name = "batch-app-mc5"
   logconfiguration = pkg_resources.resource_filename(
        path_test_resource, "test_mc5_logging.conf"
    )
   jobconfiguration = pkg_resources.resource_filename(
        path_test_resource, "test_mc5.conf"
    )
   datectrlm = datetime.now().strftime("%Y%m%d")

   sys.argv = [
        name,
        f"-j{jobconfiguration}",
        f"-l{logconfiguration}",
        f"-m{datectrlm}",
    ]

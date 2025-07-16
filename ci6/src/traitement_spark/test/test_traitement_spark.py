import pytest 
import sys
import pkg_resources 
from pyspark.sql.functions import col, to_date
from test.utils import read_csv_to_tmp_table, get_content_filtred
from icdc.hdputils.hive import hiveUtils, sparkUtils

sys.path.insert(1, "./src/")

def test_setup_tables(spark_session):
    ressource_dir_input = 'test.resources.traitement_spark.input'
    db_dev_ci6_brute = "db_dev_ci6_brute"
    db_dev_ci6 = "db_dev_ci6"
    #input
    read_csv_to_tmp_table(spark=spark_session, database="db_dev_ci6_brute", tablename="villes", path_file=pkg_resources.resource_filename(ressource_dir_input,'villes.csv'))
    ressource_dir_expected = 'test.resources.traitement_spark.expected'
    db_dev_ci6 = "db_dev_ci6"
    #expected
    read_csv_to_tmp_table(spark=spark_session, database="db_dev_ci6", tablename="ville_agg_exp", path_file=pkg_resources.resource_filename(ressource_dir_expected,'ville_agg.csv'))
    

def test_jobexemple_agg(spark_session):
    from src.traitement_spark.code.exemple import jobexemple_agg
    spark_utils = sparkUtils()
    hive_utils = hiveUtils(spark_utils)
    input=spark_session.table("db_dev_ci6_brute.villes")
    #expected = sorted(spark_session.table("db_dev_ci6.ville_agg_exp").collect())
    jobexemple_agg(hive_utils,input,"ville_agg")
    expected=spark_session.table("db_dev_ci6.ville_agg_exp")
    result = spark_session.sql("select * from db_dev_ci6.ville_agg")
    result=result.select([col(c).cast("string") for c in result.columns])
    #result=sorted(result.collect())
    #assert result == expected
    assert  expected.exceptAll(result).count() ==0 and  result.exceptAll(expected).count() ==0
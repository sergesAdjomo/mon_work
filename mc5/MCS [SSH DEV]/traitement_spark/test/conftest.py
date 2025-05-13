 
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
    warehouse_path = Path("spark-warehouse", random_uuid).absolute()
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


from pyspark.sql import Row


@pytest.fixture(scope="function")
def raw_matomo_wa7():
    yield [
        Row(
            xx_jour=100,
            idvisit="001",
            type="action",
            type_de_contenu="offer",
            tag_neva="offre transverse",
            categorie_de_contenu="Produits et services",
            programme_gouvernemental="non",
            url="https://www.banquedesterritoires.fr/professions-juridiques",
        ),
        Row(
            xx_jour=110,
            idvisit="002",
            type="action",
            type_de_contenu="offer",
            tag_neva="offre transverse",
            categorie_de_contenu=None,
            programme_gouvernemental="",
            url="https://www.banquedesterritoires.fr/professions-juridiques",
        ),
        Row(
            xx_jour=120,
            idvisit="003",
            type="action",
            type_de_contenu="offer",
            tag_neva="offre transverse",
            categorie_de_contenu="",
            programme_gouvernemental="",
            url="https://www.banquedesterritoires.fr/professions-juridiques",
        ),
        Row(
            xx_jour=130,
            idvisit="004",
            type="action",
            type_de_contenu="offer",
            tag_neva="non offre transverse",
            categorie_de_contenu="Produits et services",
            programme_gouvernemental="",
            url="",
        ),
        Row(
            xx_jour=140,
            idvisit="005",
            type="action",
            type_de_contenu="nonoffer",
            tag_neva="offre non transverse",
            categorie_de_contenu="Produits et services",
            programme_gouvernemental="",
            url="https://www.banquedesterritoires.fr/professions-juridiques",
        ),
        Row(
            xx_jour=150,
            idvisit="006",
            type="action",
            type_de_contenu="nonoffer",
            tag_neva="offre non transverse",
            categorie_de_contenu="Produits et services",
            programme_gouvernemental="",
            url="https://www.banquedesterritoires.fr/professions-juridiques",
        ),
        Row(
            xx_jour=160,
            idvisit="007",
            type="action",
            type_de_contenu="offer",
            tag_neva="Produit",
            categorie_de_contenu="Produits et services",
            programme_gouvernemental="",
            url="https://www.banquedesterritoires.fr/professions-juridiques",
        ),
        Row(
            xx_jour=170,
            idvisit="008",
            type="action",
            type_de_contenu="Page intermediaire",
            tag_neva="offre transverse",
            categorie_de_contenu="Produits et services",
            programme_gouvernemental="",
            url="",
        ),
        Row(
            xx_jour=180,
            idvisit="009",
            type="action",
            type_de_contenu="offer",
            tag_neva="offre transverse",
            categorie_de_contenu="Produits et services",
            programme_gouvernemental="",
            url="https://www.banquedesterritoires.fr/professions-juridiques",
        ),
        Row(
            xx_jour=190,
            idvisit="010",
            type="action",
            type_de_contenu="nonoffre",
            tag_neva="offre non transverse",
            categorie_de_contenu="Produits et services",
            programme_gouvernemental="",
            url="https://www.banquedesterritoires.fr/professions-juridiques",
        ),
        Row(
            xx_jour=200,
            idvisit="011",
            type="action",
            type_de_contenu="nonoffre",
            tag_neva="offre non transverse",
            categorie_de_contenu="Produits et services",
            programme_gouvernemental="",
            url="https://www.banquedesterritoires.fr/professions-juridiques",
        ),
        Row(
            xx_jour=210,
            idvisit="012",
            type="action",
            type_de_contenu="nonoffre",
            tag_neva="offre non transverse",
            categorie_de_contenu="Produits et services",
            programme_gouvernemental="",
            url="https://www.banquedesterritoires.fr/professions-juridiques",
        ),
        Row(
            xx_jour=220,
            idvisit="013",
            type="action",
            type_de_contenu="Page intermediaire",
            tag_neva="Perspectives",
            categorie_de_contenu="Perspectives",
            programme_gouvernemental="",
            url="",
        ),
        Row(
            xx_jour=230,
            idvisit="014",
            type="action",
            type_de_contenu="Articles",
            tag_neva="Perspectives",
            categorie_de_contenu="Perspectives",
            programme_gouvernemental="",
            url="https://www.banquedesterritoires.fr/professions-juridiques",
        ),
        Row(
            xx_jour=240,
            idvisit="015",
            type="action",
            type_de_contenu="Appels à projet",
            tag_neva="Appels à projet",
            categorie_de_contenu="Appels à projet",
            programme_gouvernemental="",
            url="",
        ),
        Row(
            xx_jour=250,
            idvisit="016",
            type="action",
            type_de_contenu="Solutions",
            tag_neva="Programme",
            categorie_de_contenu="Dispositifs nationaux",
            programme_gouvernemental="",
            url="https://www.banquedesterritoires.fr/professions-juridiques",
        ),
        Row(
            xx_jour=260,
            idvisit="017",
            type="action",
            type_de_contenu="Besoin",
            tag_neva="",
            categorie_de_contenu="",
            programme_gouvernemental="non",
            url="https://www.banquedesterritoires.fr/professions-juridiques",
        ),
        Row(
            xx_jour=270,
            idvisit="018",
            type="other",
            type_de_contenu="Besoin",
            tag_neva="",
            categorie_de_contenu="",
            programme_gouvernemental=None,
            url="",
        ),
        Row(
            xx_jour=270,
            idvisit="018",
            type="other",
            type_de_contenu="Besoin",
            tag_neva="",
            categorie_de_contenu="",
            programme_gouvernemental="non",
            url="https://www.banquedesterritoires.fr/professions-juridiques",
        ),
    ]


@pytest.fixture(scope="function")
def localtif_dataframe():
    yield [
        Row(
            xx_jour=100,
            idvisit="001",
            url="https://www.banquedesterritoires.fr/1-100/a",
            type_de_contenu="edition-localtis",
            type="action",
            tag_neva="Actualite Localtis",
        ),
        Row(
            xx_jour=110,
            idvisit="002",
            url="https://www.banquedesterritoires.fr/2-200/b",
            type_de_contenu="edition-localtis",
            type="action",
            tag_neva="Actualite Localtis",
        ),
        Row(
            xx_jour=120,
            idvisit="003",
            url="https://www.banquedesterritoires.fr/3-300/c",
            type_de_contenu="edition",
            type="action",
            tag_neva="offre transverse",
        ),
        Row(
            xx_jour=130,
            idvisit="004",
            url="https://www.banquedesterritoires.fr/4-400/d",
            type_de_contenu="autre",
            type="nonaction",
            tag_neva="non offre transverse",
        ),
        Row(
            xx_jour=140,
            idvisit="005",
            url="https://www.banquesdesteritoire.com/5-500/e",
            type_de_contenu="edition-localtis",
            type="action",
            tag_neva="Actualite Localtis",
        ),
    ]


@pytest.fixture(scope="function")
def autre_page_dataframe():
    yield

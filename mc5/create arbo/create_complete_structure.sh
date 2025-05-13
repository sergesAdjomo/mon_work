#!/bin/bash

# Script complet pour créer l'arborescence mc5
# Auteur: Cascade
# Date: 2025-04-27

# Emplacement racine
BASE_DIR="MCS [SSH: DEV]"

# Supprimer le dossier existant s'il y en a un
rm -rf "$BASE_DIR"

echo "Création de l'arborescence complète de MC5..."

# Création de la structure de base
mkdir -p "$BASE_DIR"/{.pytest_cache,assets,bin,data,logapp,logctm,parm,shell,shellapp,sql,src,tmp}

# Création des fichiers racine
touch "$BASE_DIR"/{.gitignore,Jenkinsfile,README.md,setup.py}

# Structure data/
touch "$BASE_DIR/data/Services_Inno_Historique_rowdetail.csv"

# Structure parm/
touch "$BASE_DIR/parm/json_schema.json"
touch "$BASE_DIR/parm/mc5_conf_Postgre.conf.ini"
touch "$BASE_DIR/parm/mc5_logging.conf"
touch "$BASE_DIR/parm/mc5_postgre.json"
touch "$BASE_DIR/parm/mc5_sqlserver.conf"
touch "$BASE_DIR/parm/mc5_sqlserver.conf.ini"
touch "$BASE_DIR/parm/mc5_sqlserver.json"
touch "$BASE_DIR/parm/mc5.conf"
touch "$BASE_DIR/parm/mc5.conf.ini"
touch "$BASE_DIR/parm/mc5.env"
touch "$BASE_DIR/parm/mc5.fonctions.env"

# Structure shell/
touch "$BASE_DIR/shell/DDMCSA1A1_SERVICES_INNO.KSH"
touch "$BASE_DIR/shell/DDMCSADA0_SERV_INNO_HIST.KSH"
touch "$BASE_DIR/shell/DDMCSA1A2_TRACKING_SITE.KSH"
touch "$BASE_DIR/shell/DDMCSA1A3_TRACK_STE_DET.KSH"
touch "$BASE_DIR/shell/DDMCSA1A5_PY_JDBC_SQLSERVER.KSH"
touch "$BASE_DIR/shell/DDMCSA1B1_CAMPAGNE_MARK.KSH"

# Structure shellapp/
touch "$BASE_DIR/shellapp/common.KSH"
touch "$BASE_DIR/shellapp/packaging.KSH"
touch "$BASE_DIR/shellapp/set_droits_fs.KSH"

# Structure sql/
touch "$BASE_DIR/sql/create_service_innovant_historique.hql"
touch "$BASE_DIR/sql/exemple_hive_2.sql"
touch "$BASE_DIR/sql/exemple_hive.sql"

# Structure src/ principale
mkdir -p "$BASE_DIR/src"/{campagne_marketing,mc5.egg-info,service_innovant,"spark-jdbc",tracking_site,traitement_spark,tmp}
touch "$BASE_DIR/src/.gitignore"

# Structure campagne_marketing/
mkdir -p "$BASE_DIR/src/campagne_marketing/__pycache__"
touch "$BASE_DIR/src/campagne_marketing/campagne_marketing_dataframe.py"
touch "$BASE_DIR/src/campagne_marketing/campagne_marketing_fields.py"
touch "$BASE_DIR/src/campagne_marketing/campagne_marketing.py"
touch "$BASE_DIR/src/campagne_marketing/main.py"

# Structure service_innovant/
mkdir -p "$BASE_DIR/src/service_innovant/__pycache__"
touch "$BASE_DIR/src/service_innovant/main.py"
touch "$BASE_DIR/src/service_innovant/service_innovant_dataframe.py"
touch "$BASE_DIR/src/service_innovant/service_innovant_fields.py"
touch "$BASE_DIR/src/service_innovant/service_innovant.py"

# Structure spark-jdbc/
touch "$BASE_DIR/src/spark-jdbc/main.py"

# Structure tracking_site/
touch "$BASE_DIR/src/tracking_site/main.py"
touch "$BASE_DIR/src/tracking_site/tracking_site_dataframe.py"
touch "$BASE_DIR/src/tracking_site/tracking_site_fields.py"
touch "$BASE_DIR/src/tracking_site/tracking_site_url.py"
touch "$BASE_DIR/src/tracking_site/tracking_site.py"

# Structure traitement_spark/
mkdir -p "$BASE_DIR/src/traitement_spark"/{__pycache__,code,test}
touch "$BASE_DIR/src/traitement_spark/_init_.py"

# Structure code/
mkdir -p "$BASE_DIR/src/traitement_spark/code/__pycache__"
touch "$BASE_DIR/src/traitement_spark/code/__init__.py"
touch "$BASE_DIR/src/traitement_spark/code/exemple.py"
touch "$BASE_DIR/src/traitement_spark/code/helper.py"
touch "$BASE_DIR/src/traitement_spark/code/settings.py"
touch "$BASE_DIR/src/traitement_spark/code/utils.py"

# Structure test/
mkdir -p "$BASE_DIR/src/traitement_spark/test"/{__pycache__,resources,test_campagne_marketing,test_service_innovant,test_tracking}
touch "$BASE_DIR/src/traitement_spark/test/__init__.py"

# Structure test_campagne_marketing/
mkdir -p "$BASE_DIR/src/traitement_spark/test/test_campagne_marketing/__pycache__"
touch "$BASE_DIR/src/traitement_spark/test/test_campagne_marketing/test_campagne_marketing_dataframe.py"
touch "$BASE_DIR/src/traitement_spark/test/test_campagne_marketing/test_campagne_marketing.py"

# Structure test_service_innovant/
mkdir -p "$BASE_DIR/src/traitement_spark/test/test_service_innovant/__pycache__"
touch "$BASE_DIR/src/traitement_spark/test/test_service_innovant/test_service_innovant_dataframe.py"
touch "$BASE_DIR/src/traitement_spark/test/test_service_innovant/test_service_innovant.py"

# Structure test_tracking/
touch "$BASE_DIR/src/traitement_spark/test/test_tracking/test_tracking_site_dataframe.py"
touch "$BASE_DIR/src/traitement_spark/test/test_tracking/test_tracking_site_url.py"
touch "$BASE_DIR/src/traitement_spark/test/test_tracking/test_tracking_site.py"
touch "$BASE_DIR/src/traitement_spark/test/test_tracking/__init__.py"
touch "$BASE_DIR/src/traitement_spark/test/test_tracking/conftest.py"
touch "$BASE_DIR/src/traitement_spark/test/test_tracking/perfo_test.py"
touch "$BASE_DIR/src/traitement_spark/test/test_tracking/utils.py"

# Structure resources/
mkdir -p "$BASE_DIR/src/traitement_spark/test/resources/traitement_spark"/{expected,input}

# Fichiers de configuration dans resources
touch "$BASE_DIR/src/traitement_spark/test/resources/test_mc5_logging.conf"
touch "$BASE_DIR/src/traitement_spark/test/resources/test_mc5.conf"
touch "$BASE_DIR/src/traitement_spark/test/resources/test_mc5.conf.ini"
touch "$BASE_DIR/src/traitement_spark/test/resources/__init__.py"

# Fichiers CSV dans resources (directement)
CSV_FILES=(
    "Brut.csv"
    "bv_diffusion_id_delivery_111494871.csv"
    "bv_log_message_sample.csv"
    "bv_operation_marketing.csv"
    "bv_recipient_function_sample.csv"
    "details.csv"
    "details2.csv"
    "details3.csv"
    "francefoncier.csv"
    "matomo_sample.csv"
    "perfo_test.csv"
    "pv_diffusion_log_id_delivery_111494871.csv"
    "spark_add_inno.csv"
    "spark_add_page.csv"
    "spark_empty_header.csv"
    "spark_empty.csv"
    "spark_energy_comparator.csv"
    "spark_filter.csv"
    "spark_test.csv"
    "territoires_fertiles.csv"
    "test_ingest_csv_20220101.csv"
)

for file in "${CSV_FILES[@]}"; do
    touch "$BASE_DIR/src/traitement_spark/test/resources/$file"
done

# Fichiers dans traitement_spark/expected/
touch "$BASE_DIR/src/traitement_spark/test/resources/traitement_spark/expected/__init__.py"
touch "$BASE_DIR/src/traitement_spark/test/resources/traitement_spark/expected/ville_agg.csv"

# Fichiers dans traitement_spark/input/
touch "$BASE_DIR/src/traitement_spark/test/resources/traitement_spark/input/__init__.py"
touch "$BASE_DIR/src/traitement_spark/test/resources/traitement_spark/input/villes.csv"

echo "Arborescence complète du projet MC5 créée avec succès!"
echo "Le répertoire racine est: $BASE_DIR"

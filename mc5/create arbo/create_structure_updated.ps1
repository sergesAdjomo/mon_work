# Script PowerShell pour créer la structure MC5 basée sur les images

# Répertoire racine
$rootDir = "d:\workspace2\MON WORK\mc5\MCS [SSH DEV]"

# Créer la structure de base
New-Item -Path $rootDir -ItemType Directory -Force
New-Item -Path "$rootDir\data" -ItemType Directory -Force
New-Item -Path "$rootDir\src" -ItemType Directory -Force
New-Item -Path "$rootDir\shell" -ItemType Directory -Force
New-Item -Path "$rootDir\shellapp" -ItemType Directory -Force
New-Item -Path "$rootDir\sql" -ItemType Directory -Force
New-Item -Path "$rootDir\bin" -ItemType Directory -Force
New-Item -Path "$rootDir\assets" -ItemType Directory -Force
New-Item -Path "$rootDir\.pytest_cache" -ItemType Directory -Force

# Structure data/ (d'après l'image 4)
New-Item -Path "$rootDir\data\Services_Inno_Historique_rowdetail.csv" -ItemType File -Force
New-Item -Path "$rootDir\data\logapp" -ItemType Directory -Force
New-Item -Path "$rootDir\data\logctm" -ItemType Directory -Force
New-Item -Path "$rootDir\data\parm" -ItemType Directory -Force

# Fichiers de configuration dans data/
New-Item -Path "$rootDir\data\json_schema.json" -ItemType File -Force
New-Item -Path "$rootDir\data\mc5_conf_Postgre.conf.ini" -ItemType File -Force
New-Item -Path "$rootDir\data\mc5_logging.conf" -ItemType File -Force
New-Item -Path "$rootDir\data\mc5_postgre.json" -ItemType File -Force
New-Item -Path "$rootDir\data\mc5_sqlserver.conf" -ItemType File -Force
New-Item -Path "$rootDir\data\mc5_sqlserver.conf.ini" -ItemType File -Force
New-Item -Path "$rootDir\data\mc5_sqlserver.json" -ItemType File -Force
New-Item -Path "$rootDir\data\mc5.conf" -ItemType File -Force
New-Item -Path "$rootDir\data\mc5.conf.ini" -ItemType File -Force
New-Item -Path "$rootDir\data\mc5.env" -ItemType File -Force
New-Item -Path "$rootDir\data\mc5.fonctions.env" -ItemType File -Force

# Structure shell/ (d'après l'image 4)
New-Item -Path "$rootDir\shell\DDMCSA1A1_SERVICES_INNO.KSH" -ItemType File -Force
New-Item -Path "$rootDir\shell\DDMCSADA0_SERV_INNO_HIST.KSH" -ItemType File -Force
New-Item -Path "$rootDir\shell\DDMCSA1A2_TRACKING_SITE.KSH" -ItemType File -Force
New-Item -Path "$rootDir\shell\DDMCSA1A3_TRACK_STE_DET.KSH" -ItemType File -Force
New-Item -Path "$rootDir\shell\DDMCSA1A5_PY_JDBC_SQLSERVER.KSH" -ItemType File -Force
New-Item -Path "$rootDir\shell\DDMCSA1B1_CAMPAGNE_MARK.KSH" -ItemType File -Force

# Structure shellapp/
New-Item -Path "$rootDir\shellapp\common.KSH" -ItemType File -Force
New-Item -Path "$rootDir\shellapp\packaging.KSH" -ItemType File -Force
New-Item -Path "$rootDir\shellapp\set_droits_fs.KSH" -ItemType File -Force

# Structure sql/ (d'après l'image 4)
New-Item -Path "$rootDir\sql\create_service_innovant_historique.hql" -ItemType File -Force
New-Item -Path "$rootDir\sql\exemple_hive_2.sql" -ItemType File -Force
New-Item -Path "$rootDir\sql\exemple_hive.sql" -ItemType File -Force

# Structure src/ principale
New-Item -Path "$rootDir\src\mc5.egg-info" -ItemType Directory -Force
New-Item -Path "$rootDir\src\campagne_marketing" -ItemType Directory -Force
New-Item -Path "$rootDir\src\service_innovant" -ItemType Directory -Force
New-Item -Path "$rootDir\src\tracking_site" -ItemType Directory -Force
New-Item -Path "$rootDir\src\traitement_spark" -ItemType Directory -Force
New-Item -Path "$rootDir\src\spark-jdbc" -ItemType Directory -Force

# Structure campagne_marketing/
New-Item -Path "$rootDir\src\campagne_marketing\__pycache__" -ItemType Directory -Force
New-Item -Path "$rootDir\src\campagne_marketing\campagne_marketing_dataframe.py" -ItemType File -Force
New-Item -Path "$rootDir\src\campagne_marketing\campagne_marketing_fields.py" -ItemType File -Force
New-Item -Path "$rootDir\src\campagne_marketing\campagne_marketing.py" -ItemType File -Force
New-Item -Path "$rootDir\src\campagne_marketing\main.py" -ItemType File -Force

# Structure service_innovant/
New-Item -Path "$rootDir\src\service_innovant\__pycache__" -ItemType Directory -Force
New-Item -Path "$rootDir\src\service_innovant\service_innovant_dataframe.py" -ItemType File -Force
New-Item -Path "$rootDir\src\service_innovant\service_innovant_fields.py" -ItemType File -Force
New-Item -Path "$rootDir\src\service_innovant\service_innovant.py" -ItemType File -Force
New-Item -Path "$rootDir\src\service_innovant\main.py" -ItemType File -Force

# Structure tracking_site/
New-Item -Path "$rootDir\src\tracking_site\main.py" -ItemType File -Force
New-Item -Path "$rootDir\src\tracking_site\tracking_site_dataframe.py" -ItemType File -Force
New-Item -Path "$rootDir\src\tracking_site\tracking_site_fields.py" -ItemType File -Force
New-Item -Path "$rootDir\src\tracking_site\tracking_site_url.py" -ItemType File -Force
New-Item -Path "$rootDir\src\tracking_site\tracking_site.py" -ItemType File -Force

# Structure spark-jdbc/
New-Item -Path "$rootDir\src\spark-jdbc\main.py" -ItemType File -Force

# Structure traitement_spark/ (basée sur l'image 3)
New-Item -Path "$rootDir\src\traitement_spark\__pycache__" -ItemType Directory -Force
New-Item -Path "$rootDir\src\traitement_spark\code" -ItemType Directory -Force
New-Item -Path "$rootDir\src\traitement_spark\code\__pycache__" -ItemType Directory -Force
New-Item -Path "$rootDir\src\traitement_spark\code\__init__.py" -ItemType File -Force
New-Item -Path "$rootDir\src\traitement_spark\code\exemple.py" -ItemType File -Force
New-Item -Path "$rootDir\src\traitement_spark\code\helper.py" -ItemType File -Force
New-Item -Path "$rootDir\src\traitement_spark\code\settings.py" -ItemType File -Force
New-Item -Path "$rootDir\src\traitement_spark\code\utils.py" -ItemType File -Force

# Structure de test (d'après images 1, 2, 3)
New-Item -Path "$rootDir\src\traitement_spark\test" -ItemType Directory -Force
New-Item -Path "$rootDir\src\traitement_spark\test\__pycache__" -ItemType Directory -Force
New-Item -Path "$rootDir\src\traitement_spark\test\resources" -ItemType Directory -Force

# Structure resources/
New-Item -Path "$rootDir\src\traitement_spark\test\resources\matomo_sample.csv" -ItemType File -Force
New-Item -Path "$rootDir\src\traitement_spark\test\resources\perfo_test.csv" -ItemType File -Force
New-Item -Path "$rootDir\src\traitement_spark\test\resources\pv_diffusion_log_id_delivery_111494871.csv" -ItemType File -Force
New-Item -Path "$rootDir\src\traitement_spark\test\resources\spark_add_inno.csv" -ItemType File -Force
New-Item -Path "$rootDir\src\traitement_spark\test\resources\spark_add_page.csv" -ItemType File -Force
New-Item -Path "$rootDir\src\traitement_spark\test\resources\spark_empty_header.csv" -ItemType File -Force
New-Item -Path "$rootDir\src\traitement_spark\test\resources\spark_empty.csv" -ItemType File -Force
New-Item -Path "$rootDir\src\traitement_spark\test\resources\spark_energy_comparator.csv" -ItemType File -Force
New-Item -Path "$rootDir\src\traitement_spark\test\resources\spark_filter.csv" -ItemType File -Force
New-Item -Path "$rootDir\src\traitement_spark\test\resources\spark_test.csv" -ItemType File -Force
New-Item -Path "$rootDir\src\traitement_spark\test\resources\territoires_fertiles.csv" -ItemType File -Force
New-Item -Path "$rootDir\src\traitement_spark\test\resources\test_ingest_csv_20220101.csv" -ItemType File -Force
New-Item -Path "$rootDir\src\traitement_spark\test\resources\bv_log_message_sample.csv" -ItemType File -Force
New-Item -Path "$rootDir\src\traitement_spark\test\resources\bv_operation_marketing.csv" -ItemType File -Force
New-Item -Path "$rootDir\src\traitement_spark\test\resources\bv_recipient_function_sample.csv" -ItemType File -Force
New-Item -Path "$rootDir\src\traitement_spark\test\resources\details.csv" -ItemType File -Force
New-Item -Path "$rootDir\src\traitement_spark\test\resources\details2.csv" -ItemType File -Force
New-Item -Path "$rootDir\src\traitement_spark\test\resources\details3.csv" -ItemType File -Force
New-Item -Path "$rootDir\src\traitement_spark\test\resources\francefoncier.csv" -ItemType File -Force

# Fichiers de configuration de test
New-Item -Path "$rootDir\src\traitement_spark\test\resources\test_mc5_logging.conf" -ItemType File -Force
New-Item -Path "$rootDir\src\traitement_spark\test\resources\test_mc5.conf" -ItemType File -Force
New-Item -Path "$rootDir\src\traitement_spark\test\resources\test_mc5.conf.ini" -ItemType File -Force

# Structure des modules de test
New-Item -Path "$rootDir\src\traitement_spark\test\test_campagne_marketing" -ItemType Directory -Force
New-Item -Path "$rootDir\src\traitement_spark\test\test_campagne_marketing\__pycache__" -ItemType Directory -Force
New-Item -Path "$rootDir\src\traitement_spark\test\test_campagne_marketing\test_campagne_marketing_dataframe.py" -ItemType File -Force
New-Item -Path "$rootDir\src\traitement_spark\test\test_campagne_marketing\test_campagne_marketing.py" -ItemType File -Force

New-Item -Path "$rootDir\src\traitement_spark\test\test_service_innovant" -ItemType Directory -Force
New-Item -Path "$rootDir\src\traitement_spark\test\test_service_innovant\__pycache__" -ItemType Directory -Force
New-Item -Path "$rootDir\src\traitement_spark\test\test_service_innovant\test_service_innovant_dataframe.py" -ItemType File -Force
New-Item -Path "$rootDir\src\traitement_spark\test\test_service_innovant\test_service_innovant.py" -ItemType File -Force

New-Item -Path "$rootDir\src\traitement_spark\test\test_tracking" -ItemType Directory -Force
New-Item -Path "$rootDir\src\traitement_spark\test\test_tracking\test_tracking_site_dataframe.py" -ItemType File -Force
New-Item -Path "$rootDir\src\traitement_spark\test\test_tracking\test_tracking_site_url.py" -ItemType File -Force
New-Item -Path "$rootDir\src\traitement_spark\test\test_tracking\test_tracking_site.py" -ItemType File -Force
New-Item -Path "$rootDir\src\traitement_spark\test\test_tracking\__init__.py" -ItemType File -Force
New-Item -Path "$rootDir\src\traitement_spark\test\test_tracking\conftest.py" -ItemType File -Force
New-Item -Path "$rootDir\src\traitement_spark\test\test_tracking\perfo_test.py" -ItemType File -Force
New-Item -Path "$rootDir\src\traitement_spark\test\test_tracking\utils.py" -ItemType File -Force
New-Item -Path "$rootDir\src\traitement_spark\test\test_tracking\__init__.py" -ItemType File -Force

# Structure resources/traitement_spark (comme dans l'image 3)
New-Item -Path "$rootDir\src\traitement_spark\resources" -ItemType Directory -Force
New-Item -Path "$rootDir\src\traitement_spark\resources\traitement_spark" -ItemType Directory -Force
New-Item -Path "$rootDir\src\traitement_spark\resources\traitement_spark\expected" -ItemType Directory -Force
New-Item -Path "$rootDir\src\traitement_spark\resources\traitement_spark\expected\__init__.py" -ItemType File -Force
New-Item -Path "$rootDir\src\traitement_spark\resources\traitement_spark\expected\ville_agg.csv" -ItemType File -Force

New-Item -Path "$rootDir\src\traitement_spark\resources\traitement_spark\input" -ItemType Directory -Force
New-Item -Path "$rootDir\src\traitement_spark\resources\traitement_spark\input\__init__.py" -ItemType File -Force
New-Item -Path "$rootDir\src\traitement_spark\resources\traitement_spark\input\villes.csv" -ItemType File -Force
New-Item -Path "$rootDir\src\traitement_spark\resources\traitement_spark\input\Brut.csv" -ItemType File -Force
New-Item -Path "$rootDir\src\traitement_spark\resources\traitement_spark\input\bv_diffusion_id_delivery_111494871.csv" -ItemType File -Force
New-Item -Path "$rootDir\src\traitement_spark\resources\traitement_spark\input\bv_log_message_sample.csv" -ItemType File -Force
New-Item -Path "$rootDir\src\traitement_spark\resources\traitement_spark\input\bv_operation_marketing.csv" -ItemType File -Force
New-Item -Path "$rootDir\src\traitement_spark\resources\traitement_spark\input\bv_recipient_function_sample.csv" -ItemType File -Force

# Fichiers à la racine
New-Item -Path "$rootDir\.gitignore" -ItemType File -Force
New-Item -Path "$rootDir\Jenkinsfile" -ItemType File -Force
New-Item -Path "$rootDir\README.md" -ItemType File -Force
New-Item -Path "$rootDir\setup.py" -ItemType File -Force

Write-Host "Structure MC5 créée avec succès basée sur les images!"

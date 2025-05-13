# Script PowerShell pour créer la structure MC5

# Créer la structure de base
New-Item -Path "d:\workspace2\MON WORK\mc5\MCS [SSH DEV]" -ItemType Directory -Force
New-Item -Path "d:\workspace2\MON WORK\mc5\MCS [SSH DEV]\data" -ItemType Directory -Force
New-Item -Path "d:\workspace2\MON WORK\mc5\MCS [SSH DEV]\src" -ItemType Directory -Force
New-Item -Path "d:\workspace2\MON WORK\mc5\MCS [SSH DEV]\test" -ItemType Directory -Force
New-Item -Path "d:\workspace2\MON WORK\mc5\MCS [SSH DEV]\resources" -ItemType Directory -Force
New-Item -Path "d:\workspace2\MON WORK\mc5\MCS [SSH DEV]\shell" -ItemType Directory -Force
New-Item -Path "d:\workspace2\MON WORK\mc5\MCS [SSH DEV]\sql" -ItemType Directory -Force
New-Item -Path "d:\workspace2\MON WORK\mc5\MCS [SSH DEV]\tmp" -ItemType Directory -Force

# Structure data/
New-Item -Path "d:\workspace2\MON WORK\mc5\MCS [SSH DEV]\data\Services_Inno_Historique_rowdetail.csv" -ItemType File -Force
New-Item -Path "d:\workspace2\MON WORK\mc5\MCS [SSH DEV]\data\logapp" -ItemType Directory -Force
New-Item -Path "d:\workspace2\MON WORK\mc5\MCS [SSH DEV]\data\logctm" -ItemType Directory -Force
New-Item -Path "d:\workspace2\MON WORK\mc5\MCS [SSH DEV]\data\parm" -ItemType Directory -Force
New-Item -Path "d:\workspace2\MON WORK\mc5\MCS [SSH DEV]\data\json_schema.json" -ItemType File -Force
New-Item -Path "d:\workspace2\MON WORK\mc5\MCS [SSH DEV]\data\mc5_conf_Postgre.conf.ini" -ItemType File -Force
New-Item -Path "d:\workspace2\MON WORK\mc5\MCS [SSH DEV]\data\mc5_logging.conf" -ItemType File -Force
New-Item -Path "d:\workspace2\MON WORK\mc5\MCS [SSH DEV]\data\mc5_postgre.json" -ItemType File -Force
New-Item -Path "d:\workspace2\MON WORK\mc5\MCS [SSH DEV]\data\mc5_sqlserver.conf" -ItemType File -Force
New-Item -Path "d:\workspace2\MON WORK\mc5\MCS [SSH DEV]\data\mc5_sqlserver.conf.ini" -ItemType File -Force
New-Item -Path "d:\workspace2\MON WORK\mc5\MCS [SSH DEV]\data\mc5_sqlserver.json" -ItemType File -Force
New-Item -Path "d:\workspace2\MON WORK\mc5\MCS [SSH DEV]\data\mc5.conf" -ItemType File -Force
New-Item -Path "d:\workspace2\MON WORK\mc5\MCS [SSH DEV]\data\mc5.conf.ini" -ItemType File -Force
New-Item -Path "d:\workspace2\MON WORK\mc5\MCS [SSH DEV]\data\mc5.env" -ItemType File -Force
New-Item -Path "d:\workspace2\MON WORK\mc5\MCS [SSH DEV]\data\mc5.fonctions.env" -ItemType File -Force

# Structure shell/
New-Item -Path "d:\workspace2\MON WORK\mc5\MCS [SSH DEV]\shell\shellapp" -ItemType Directory -Force
New-Item -Path "d:\workspace2\MON WORK\mc5\MCS [SSH DEV]\shell\sql" -ItemType Directory -Force
New-Item -Path "d:\workspace2\MON WORK\mc5\MCS [SSH DEV]\shell\DDMCSA1A1_SERVICES_INNOKSH" -ItemType File -Force
New-Item -Path "d:\workspace2\MON WORK\mc5\MCS [SSH DEV]\shell\DDMCSADA0_SERV_INNO_HIST.KSH" -ItemType File -Force
New-Item -Path "d:\workspace2\MON WORK\mc5\MCS [SSH DEV]\shell\DDMCSA1A2_TRACKING_STE.KSH" -ItemType File -Force
New-Item -Path "d:\workspace2\MON WORK\mc5\MCS [SSH DEV]\shell\DDMCSA1A3_TRACK_STE_DET.KSH" -ItemType File -Force
New-Item -Path "d:\workspace2\MON WORK\mc5\MCS [SSH DEV]\shell\DDMCSA1A5_PY_JDBC_SOLSERVER.KSH" -ItemType File -Force
New-Item -Path "d:\workspace2\MON WORK\mc5\MCS [SSH DEV]\shell\DDMCSA1B1_CAMPAGNE_MARK.KSH" -ItemType File -Force
New-Item -Path "d:\workspace2\MON WORK\mc5\MCS [SSH DEV]\shell\shellapp\common.KSH" -ItemType File -Force
New-Item -Path "d:\workspace2\MON WORK\mc5\MCS [SSH DEV]\shell\shellapp\packaging.KSH" -ItemType File -Force
New-Item -Path "d:\workspace2\MON WORK\mc5\MCS [SSH DEV]\shell\shellapp\set_droits_fs.KSH" -ItemType File -Force

# Structure sql/
New-Item -Path "d:\workspace2\MON WORK\mc5\MCS [SSH DEV]\sql\create_service_innovant_historique.hql" -ItemType File -Force
New-Item -Path "d:\workspace2\MON WORK\mc5\MCS [SSH DEV]\sql\exemple_hive_2.sql" -ItemType File -Force
New-Item -Path "d:\workspace2\MON WORK\mc5\MCS [SSH DEV]\sql\example_hive.sql" -ItemType File -Force

# Structure src/
New-Item -Path "d:\workspace2\MON WORK\mc5\MCS [SSH DEV]\src\campagne_marketing" -ItemType Directory -Force
New-Item -Path "d:\workspace2\MON WORK\mc5\MCS [SSH DEV]\src\service_innovant" -ItemType Directory -Force
New-Item -Path "d:\workspace2\MON WORK\mc5\MCS [SSH DEV]\src\tracking_site" -ItemType Directory -Force
New-Item -Path "d:\workspace2\MON WORK\mc5\MCS [SSH DEV]\src\traitement_spark" -ItemType Directory -Force

# Fichiers campagne_marketing
New-Item -Path "d:\workspace2\MON WORK\mc5\MCS [SSH DEV]\src\campagne_marketing\campagne_marketing_dataframe.py" -ItemType File -Force
New-Item -Path "d:\workspace2\MON WORK\mc5\MCS [SSH DEV]\src\campagne_marketing\campagne_marketing_fields.py" -ItemType File -Force
New-Item -Path "d:\workspace2\MON WORK\mc5\MCS [SSH DEV]\src\campagne_marketing\campagne_marketing.py" -ItemType File -Force
New-Item -Path "d:\workspace2\MON WORK\mc5\MCS [SSH DEV]\src\campagne_marketing\main.py" -ItemType File -Force

# Fichiers service_innovant
New-Item -Path "d:\workspace2\MON WORK\mc5\MCS [SSH DEV]\src\service_innovant\service_innovant_dataframe.py" -ItemType File -Force
New-Item -Path "d:\workspace2\MON WORK\mc5\MCS [SSH DEV]\src\service_innovant\service_innovant_fields.py" -ItemType File -Force
New-Item -Path "d:\workspace2\MON WORK\mc5\MCS [SSH DEV]\src\service_innovant\service_innovant.py" -ItemType File -Force
New-Item -Path "d:\workspace2\MON WORK\mc5\MCS [SSH DEV]\src\service_innovant\main.py" -ItemType File -Force

# Fichiers tracking_site
New-Item -Path "d:\workspace2\MON WORK\mc5\MCS [SSH DEV]\src\tracking_site\tracking_site_dataframe.py" -ItemType File -Force
New-Item -Path "d:\workspace2\MON WORK\mc5\MCS [SSH DEV]\src\tracking_site\tracking_site_fields.py" -ItemType File -Force
New-Item -Path "d:\workspace2\MON WORK\mc5\MCS [SSH DEV]\src\tracking_site\tracking_site_url.py" -ItemType File -Force
New-Item -Path "d:\workspace2\MON WORK\mc5\MCS [SSH DEV]\src\tracking_site\tracking_site.py" -ItemType File -Force
New-Item -Path "d:\workspace2\MON WORK\mc5\MCS [SSH DEV]\src\tracking_site\main.py" -ItemType File -Force

# Structure traitement_spark
New-Item -Path "d:\workspace2\MON WORK\mc5\MCS [SSH DEV]\src\traitement_spark\code" -ItemType Directory -Force
New-Item -Path "d:\workspace2\MON WORK\mc5\MCS [SSH DEV]\src\traitement_spark\code\__init__.py" -ItemType File -Force
New-Item -Path "d:\workspace2\MON WORK\mc5\MCS [SSH DEV]\src\traitement_spark\code\exemple.py" -ItemType File -Force
New-Item -Path "d:\workspace2\MON WORK\mc5\MCS [SSH DEV]\src\traitement_spark\code\helper.py" -ItemType File -Force
New-Item -Path "d:\workspace2\MON WORK\mc5\MCS [SSH DEV]\src\traitement_spark\code\settings.py" -ItemType File -Force
New-Item -Path "d:\workspace2\MON WORK\mc5\MCS [SSH DEV]\src\traitement_spark\code\utils.py" -ItemType File -Force

# Structure test/
New-Item -Path "d:\workspace2\MON WORK\mc5\MCS [SSH DEV]\test\resources" -ItemType Directory -Force
New-Item -Path "d:\workspace2\MON WORK\mc5\MCS [SSH DEV]\test\expected" -ItemType Directory -Force
New-Item -Path "d:\workspace2\MON WORK\mc5\MCS [SSH DEV]\test\input" -ItemType Directory -Force
New-Item -Path "d:\workspace2\MON WORK\mc5\MCS [SSH DEV]\test\resources\matomo_sample.csv" -ItemType File -Force
New-Item -Path "d:\workspace2\MON WORK\mc5\MCS [SSH DEV]\test\resources\perfo_test.csv" -ItemType File -Force
New-Item -Path "d:\workspace2\MON WORK\mc5\MCS [SSH DEV]\test\resources\spark_sample.csv" -ItemType File -Force
New-Item -Path "d:\workspace2\MON WORK\mc5\MCS [SSH DEV]\test\resources\territories_feribles.csv" -ItemType File -Force
New-Item -Path "d:\workspace2\MON WORK\mc5\MCS [SSH DEV]\test\expected\ville_agg.csv" -ItemType File -Force
New-Item -Path "d:\workspace2\MON WORK\mc5\MCS [SSH DEV]\test\input\villes.csv" -ItemType File -Force

# Fichiers divers
New-Item -Path "d:\workspace2\MON WORK\mc5\MCS [SSH DEV]\.gitignore" -ItemType File -Force
New-Item -Path "d:\workspace2\MON WORK\mc5\MCS [SSH DEV]\Jenkinsfile" -ItemType File -Force
New-Item -Path "d:\workspace2\MON WORK\mc5\MCS [SSH DEV]\README.md" -ItemType File -Force
New-Item -Path "d:\workspace2\MON WORK\mc5\MCS [SSH DEV]\setup.py" -ItemType File -Force
New-Item -Path "d:\workspace2\MON WORK\mc5\MCS [SSH DEV]\test\conftest.py" -ItemType File -Force
New-Item -Path "d:\workspace2\MON WORK\mc5\MCS [SSH DEV]\test\perfo_test.py" -ItemType File -Force
New-Item -Path "d:\workspace2\MON WORK\mc5\MCS [SSH DEV]\test\utils.py" -ItemType File -Force

# Créer les répertoires __pycache__ (vides)
New-Item -Path "d:\workspace2\MON WORK\mc5\MCS [SSH DEV]\src\campagne_marketing\__pycache__" -ItemType Directory -Force
New-Item -Path "d:\workspace2\MON WORK\mc5\MCS [SSH DEV]\src\service_innovant\__pycache__" -ItemType Directory -Force
New-Item -Path "d:\workspace2\MON WORK\mc5\MCS [SSH DEV]\src\tracking_site\__pycache__" -ItemType Directory -Force
New-Item -Path "d:\workspace2\MON WORK\mc5\MCS [SSH DEV]\src\traitement_spark\__pycache__" -ItemType Directory -Force

Write-Host "Structure MC5 créée avec succès!"

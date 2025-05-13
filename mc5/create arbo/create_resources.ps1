# Script PowerShell pour créer la structure du répertoire resources

# Définir l'emplacement racine
$baseDir = "d:\workspace2\MON WORK\mc5\MCS [SSH DEV]\src\traitement_spark"

# Créer le répertoire resources et son sous-dossier traitement_spark
New-Item -Path "$baseDir\resources" -ItemType Directory -Force
New-Item -Path "$baseDir\resources\traitement_spark" -ItemType Directory -Force

# Créer sous-dossiers expected et input
New-Item -Path "$baseDir\resources\traitement_spark\expected" -ItemType Directory -Force
New-Item -Path "$baseDir\resources\traitement_spark\input" -ItemType Directory -Force

# Fichiers dans expected
New-Item -Path "$baseDir\resources\traitement_spark\expected\__init__.py" -ItemType File -Force
New-Item -Path "$baseDir\resources\traitement_spark\expected\ville_agg.csv" -ItemType File -Force

# Fichiers dans input
New-Item -Path "$baseDir\resources\traitement_spark\input\__init__.py" -ItemType File -Force
New-Item -Path "$baseDir\resources\traitement_spark\input\villes.csv" -ItemType File -Force
New-Item -Path "$baseDir\resources\traitement_spark\input\Brut.csv" -ItemType File -Force
New-Item -Path "$baseDir\resources\traitement_spark\input\bv_diffusion_id_delivery_111494871.csv" -ItemType File -Force
New-Item -Path "$baseDir\resources\traitement_spark\input\bv_log_message_sample.csv" -ItemType File -Force
New-Item -Path "$baseDir\resources\traitement_spark\input\bv_operation_marketing.csv" -ItemType File -Force
New-Item -Path "$baseDir\resources\traitement_spark\input\bv_recipient_function_sample.csv" -ItemType File -Force
New-Item -Path "$baseDir\resources\traitement_spark\input\details.csv" -ItemType File -Force
New-Item -Path "$baseDir\resources\traitement_spark\input\details2.csv" -ItemType File -Force
New-Item -Path "$baseDir\resources\traitement_spark\input\details3.csv" -ItemType File -Force
New-Item -Path "$baseDir\resources\traitement_spark\input\francefoncier.csv" -ItemType File -Force
New-Item -Path "$baseDir\resources\traitement_spark\input\matomo_sample.csv" -ItemType File -Force
New-Item -Path "$baseDir\resources\traitement_spark\input\perfo_test.csv" -ItemType File -Force
New-Item -Path "$baseDir\resources\traitement_spark\input\pv_diffusion_log_id_delivery_111494871.csv" -ItemType File -Force
New-Item -Path "$baseDir\resources\traitement_spark\input\spark_add_inno.csv" -ItemType File -Force
New-Item -Path "$baseDir\resources\traitement_spark\input\spark_add_page.csv" -ItemType File -Force
New-Item -Path "$baseDir\resources\traitement_spark\input\spark_empty_header.csv" -ItemType File -Force
New-Item -Path "$baseDir\resources\traitement_spark\input\spark_empty.csv" -ItemType File -Force
New-Item -Path "$baseDir\resources\traitement_spark\input\spark_energy_comparator.csv" -ItemType File -Force
New-Item -Path "$baseDir\resources\traitement_spark\input\spark_filter.csv" -ItemType File -Force
New-Item -Path "$baseDir\resources\traitement_spark\input\spark_test.csv" -ItemType File -Force
New-Item -Path "$baseDir\resources\traitement_spark\input\territoires_fertiles.csv" -ItemType File -Force
New-Item -Path "$baseDir\resources\traitement_spark\input\test_ingest_csv_20220101.csv" -ItemType File -Force

# Fichiers de configuration
New-Item -Path "$baseDir\resources\test_mc5_logging.conf" -ItemType File -Force
New-Item -Path "$baseDir\resources\test_mc5.conf" -ItemType File -Force
New-Item -Path "$baseDir\resources\test_mc5.conf.ini" -ItemType File -Force

Write-Host "Structure resources créée avec succès selon les images !"

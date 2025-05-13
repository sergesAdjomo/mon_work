# Script PowerShell pour créer la structure du répertoire traitement_spark

# Définir l'emplacement racine
$baseDir = "d:\workspace2\MON WORK\mc5\MCS [SSH DEV]\src"

# Structure traitement_spark principale
New-Item -Path "$baseDir\traitement_spark" -ItemType Directory -Force
New-Item -Path "$baseDir\traitement_spark\__pycache__" -ItemType Directory -Force
New-Item -Path "$baseDir\traitement_spark\code" -ItemType Directory -Force
New-Item -Path "$baseDir\traitement_spark\test" -ItemType Directory -Force
New-Item -Path "$baseDir\traitement_spark\_init_.py" -ItemType File -Force

# Structure du sous-répertoire code/
New-Item -Path "$baseDir\traitement_spark\code\__pycache__" -ItemType Directory -Force
New-Item -Path "$baseDir\traitement_spark\code\__init__.py" -ItemType File -Force
New-Item -Path "$baseDir\traitement_spark\code\exemple.py" -ItemType File -Force
New-Item -Path "$baseDir\traitement_spark\code\helper.py" -ItemType File -Force
New-Item -Path "$baseDir\traitement_spark\code\settings.py" -ItemType File -Force
New-Item -Path "$baseDir\traitement_spark\code\utils.py" -ItemType File -Force

# Structure du sous-répertoire test/
New-Item -Path "$baseDir\traitement_spark\test\__pycache__" -ItemType Directory -Force
New-Item -Path "$baseDir\traitement_spark\test\resources" -ItemType Directory -Force

# Structure de test_campagne_marketing
New-Item -Path "$baseDir\traitement_spark\test\test_campagne_marketing" -ItemType Directory -Force
New-Item -Path "$baseDir\traitement_spark\test\test_campagne_marketing\__pycache__" -ItemType Directory -Force
New-Item -Path "$baseDir\traitement_spark\test\test_campagne_marketing\test_campagne_marketing_dataframe.py" -ItemType File -Force
New-Item -Path "$baseDir\traitement_spark\test\test_campagne_marketing\test_campagne_marketing.py" -ItemType File -Force

# Structure de test_service_innovant
New-Item -Path "$baseDir\traitement_spark\test\test_service_innovant" -ItemType Directory -Force
New-Item -Path "$baseDir\traitement_spark\test\test_service_innovant\__pycache__" -ItemType Directory -Force
New-Item -Path "$baseDir\traitement_spark\test\test_service_innovant\test_service_innovant_dataframe.py" -ItemType File -Force
New-Item -Path "$baseDir\traitement_spark\test\test_service_innovant\test_service_innovant.py" -ItemType File -Force

# Structure de test_tracking
New-Item -Path "$baseDir\traitement_spark\test\test_tracking" -ItemType Directory -Force
New-Item -Path "$baseDir\traitement_spark\test\test_tracking\test_tracking_site_dataframe.py" -ItemType File -Force
New-Item -Path "$baseDir\traitement_spark\test\test_tracking\test_tracking_site_url.py" -ItemType File -Force
New-Item -Path "$baseDir\traitement_spark\test\test_tracking\test_tracking_site.py" -ItemType File -Force
New-Item -Path "$baseDir\traitement_spark\test\test_tracking\__init__.py" -ItemType File -Force
New-Item -Path "$baseDir\traitement_spark\test\test_tracking\conftest.py" -ItemType File -Force
New-Item -Path "$baseDir\traitement_spark\test\test_tracking\perfo_test.py" -ItemType File -Force
New-Item -Path "$baseDir\traitement_spark\test\test_tracking\utils.py" -ItemType File -Force
New-Item -Path "$baseDir\traitement_spark\test\__init__.py" -ItemType File -Force

Write-Host "Structure traitement_spark créée avec succès selon les images (sans le contenu de resources) !"

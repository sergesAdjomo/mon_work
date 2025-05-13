# Script PowerShell pour créer la structure du répertoire src

# Définir l'emplacement racine
$baseDir = "d:\workspace2\MON WORK\mc5\MCS [SSH DEV]"

# Structure src/ principale - sans traitement_spark pour le moment
New-Item -Path "$baseDir\src\campagne_marketing" -ItemType Directory -Force
New-Item -Path "$baseDir\src\mc5.egg-info" -ItemType Directory -Force
New-Item -Path "$baseDir\src\service_innovant" -ItemType Directory -Force
New-Item -Path "$baseDir\src\spark-jdbc" -ItemType Directory -Force
New-Item -Path "$baseDir\src\tracking_site" -ItemType Directory -Force
New-Item -Path "$baseDir\src\tmp" -ItemType Directory -Force
New-Item -Path "$baseDir\src\.gitignore" -ItemType File -Force

# Structure de campagne_marketing avec ses fichiers Python
New-Item -Path "$baseDir\src\campagne_marketing\__pycache__" -ItemType Directory -Force
New-Item -Path "$baseDir\src\campagne_marketing\campagne_marketing_dataframe.py" -ItemType File -Force
New-Item -Path "$baseDir\src\campagne_marketing\campagne_marketing_fields.py" -ItemType File -Force
New-Item -Path "$baseDir\src\campagne_marketing\campagne_marketing.py" -ItemType File -Force
New-Item -Path "$baseDir\src\campagne_marketing\main.py" -ItemType File -Force

# Structure de service_innovant avec ses fichiers Python
New-Item -Path "$baseDir\src\service_innovant\__pycache__" -ItemType Directory -Force
New-Item -Path "$baseDir\src\service_innovant\main.py" -ItemType File -Force
New-Item -Path "$baseDir\src\service_innovant\service_innovant_dataframe.py" -ItemType File -Force
New-Item -Path "$baseDir\src\service_innovant\service_innovant_fields.py" -ItemType File -Force
New-Item -Path "$baseDir\src\service_innovant\service_innovant.py" -ItemType File -Force

# Structure de spark-jdbc 
New-Item -Path "$baseDir\src\spark-jdbc\main.py" -ItemType File -Force

# Structure de tracking_site avec ses fichiers Python
New-Item -Path "$baseDir\src\tracking_site\main.py" -ItemType File -Force
New-Item -Path "$baseDir\src\tracking_site\tracking_site_dataframe.py" -ItemType File -Force
New-Item -Path "$baseDir\src\tracking_site\tracking_site_fields.py" -ItemType File -Force
New-Item -Path "$baseDir\src\tracking_site\tracking_site_url.py" -ItemType File -Force
New-Item -Path "$baseDir\src\tracking_site\tracking_site.py" -ItemType File -Force

Write-Host "Structure src/ créée avec succès selon les images (sans traitement_spark) !"

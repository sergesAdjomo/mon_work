# Script PowerShell pour créer une structure MC5 propre

# Définir l'emplacement racine
$baseDir = "d:\workspace2\MON WORK\mc5"

# Créer le dossier principal
New-Item -Path "$baseDir\MCS [SSH DEV]" -ItemType Directory -Force

# Créer les dossiers de premier niveau
New-Item -Path "$baseDir\MCS [SSH DEV]\.pytest_cache" -ItemType Directory -Force
New-Item -Path "$baseDir\MCS [SSH DEV]\assets" -ItemType Directory -Force
New-Item -Path "$baseDir\MCS [SSH DEV]\bin" -ItemType Directory -Force
New-Item -Path "$baseDir\MCS [SSH DEV]\data" -ItemType Directory -Force
New-Item -Path "$baseDir\MCS [SSH DEV]\logapp" -ItemType Directory -Force
New-Item -Path "$baseDir\MCS [SSH DEV]\logctm" -ItemType Directory -Force
New-Item -Path "$baseDir\MCS [SSH DEV]\parm" -ItemType Directory -Force
New-Item -Path "$baseDir\MCS [SSH DEV]\shell" -ItemType Directory -Force
New-Item -Path "$baseDir\MCS [SSH DEV]\shellapp" -ItemType Directory -Force
New-Item -Path "$baseDir\MCS [SSH DEV]\sql" -ItemType Directory -Force
New-Item -Path "$baseDir\MCS [SSH DEV]\src" -ItemType Directory -Force
New-Item -Path "$baseDir\MCS [SSH DEV]\tmp" -ItemType Directory -Force

# Créer les fichiers racine
New-Item -Path "$baseDir\MCS [SSH DEV]\.gitignore" -ItemType File -Force
New-Item -Path "$baseDir\MCS [SSH DEV]\Jenkinsfile" -ItemType File -Force
New-Item -Path "$baseDir\MCS [SSH DEV]\README.md" -ItemType File -Force
New-Item -Path "$baseDir\MCS [SSH DEV]\setup.py" -ItemType File -Force

Write-Host "Structure propre MC5 créée avec succès !"

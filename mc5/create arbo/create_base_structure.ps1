# Script PowerShell pour créer UNIQUEMENT la structure de base MC5

# Définir l'emplacement racine
$baseDir = "d:\workspace2\MON WORK\mc5"

# Supprimer d'abord le dossier MCS [SSH DEV] s'il existe déjà
if (Test-Path "$baseDir\MCS [SSH DEV]") {
    Remove-Item -Path "$baseDir\MCS [SSH DEV]" -Recurse -Force
}

# Créer le dossier principal
New-Item -Path "$baseDir\MCS [SSH DEV]" -ItemType Directory -Force

# Créer UNIQUEMENT les dossiers de premier niveau (strictement comme dans l'image)
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

# Créer uniquement les 4 fichiers racine
New-Item -Path "$baseDir\MCS [SSH DEV]\.gitignore" -ItemType File -Force
New-Item -Path "$baseDir\MCS [SSH DEV]\Jenkinsfile" -ItemType File -Force
New-Item -Path "$baseDir\MCS [SSH DEV]\README.md" -ItemType File -Force
New-Item -Path "$baseDir\MCS [SSH DEV]\setup.py" -ItemType File -Force

Write-Host "Structure de base MC5 créée avec succès !"

# Script PowerShell pour créer la structure MC5 exacte comme dans l'image

# Répertoire racine
$rootDir = "d:\workspace2\MON WORK\mc5\MCS [SSH DEV]"

# Créer la structure de base - exactement comme dans l'image
New-Item -Path $rootDir -ItemType Directory -Force
New-Item -Path "$rootDir\.pytest_cache" -ItemType Directory -Force
New-Item -Path "$rootDir\assets" -ItemType Directory -Force
New-Item -Path "$rootDir\bin" -ItemType Directory -Force
New-Item -Path "$rootDir\data" -ItemType Directory -Force
New-Item -Path "$rootDir\logapp" -ItemType Directory -Force
New-Item -Path "$rootDir\logctm" -ItemType Directory -Force
New-Item -Path "$rootDir\parm" -ItemType Directory -Force
New-Item -Path "$rootDir\shell" -ItemType Directory -Force
New-Item -Path "$rootDir\shellapp" -ItemType Directory -Force
New-Item -Path "$rootDir\sql" -ItemType Directory -Force
New-Item -Path "$rootDir\src" -ItemType Directory -Force
New-Item -Path "$rootDir\tmp" -ItemType Directory -Force

# Fichiers à la racine
New-Item -Path "$rootDir\.gitignore" -ItemType File -Force
New-Item -Path "$rootDir\Jenkinsfile" -ItemType File -Force
New-Item -Path "$rootDir\README.md" -ItemType File -Force
New-Item -Path "$rootDir\setup.py" -ItemType File -Force

Write-Host "Structure MC5 créée avec succès selon l'image!"

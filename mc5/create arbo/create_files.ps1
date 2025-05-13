# Script PowerShell pour créer les fichiers dans parm, data, shell, shellapp et sql

# Définir l'emplacement racine
$baseDir = "d:\workspace2\MON WORK\mc5\MCS [SSH DEV]"

# Fichiers dans data/
New-Item -Path "$baseDir\data\Services_Inno_Historique_rowdetail.csv" -ItemType File -Force

# Fichiers dans parm/
New-Item -Path "$baseDir\parm\json_schema.json" -ItemType File -Force
New-Item -Path "$baseDir\parm\mc5_conf_Postgre.conf.ini" -ItemType File -Force
New-Item -Path "$baseDir\parm\mc5_logging.conf" -ItemType File -Force
New-Item -Path "$baseDir\parm\mc5_postgre.json" -ItemType File -Force
New-Item -Path "$baseDir\parm\mc5_sqlserver.conf" -ItemType File -Force
New-Item -Path "$baseDir\parm\mc5_sqlserver.conf.ini" -ItemType File -Force
New-Item -Path "$baseDir\parm\mc5_sqlserver.json" -ItemType File -Force
New-Item -Path "$baseDir\parm\mc5.conf" -ItemType File -Force
New-Item -Path "$baseDir\parm\mc5.conf.ini" -ItemType File -Force
New-Item -Path "$baseDir\parm\mc5.env" -ItemType File -Force
New-Item -Path "$baseDir\parm\mc5.fonctions.env" -ItemType File -Force

# Fichiers dans shell/
New-Item -Path "$baseDir\shell\DDMCSA1A1_SERVICES_INNO.KSH" -ItemType File -Force
New-Item -Path "$baseDir\shell\DDMCSADA0_SERV_INNO_HIST.KSH" -ItemType File -Force
New-Item -Path "$baseDir\shell\DDMCSA1A2_TRACKING_SITE.KSH" -ItemType File -Force
New-Item -Path "$baseDir\shell\DDMCSA1A3_TRACK_STE_DET.KSH" -ItemType File -Force
New-Item -Path "$baseDir\shell\DDMCSA1A5_PY_JDBC_SQLSERVER.KSH" -ItemType File -Force
New-Item -Path "$baseDir\shell\DDMCSA1B1_CAMPAGNE_MARK.KSH" -ItemType File -Force

# Fichiers dans shellapp/
New-Item -Path "$baseDir\shellapp\common.KSH" -ItemType File -Force
New-Item -Path "$baseDir\shellapp\packaging.KSH" -ItemType File -Force
New-Item -Path "$baseDir\shellapp\set_droits_fs.KSH" -ItemType File -Force

# Fichiers dans sql/
New-Item -Path "$baseDir\sql\create_service_innovant_historique.hql" -ItemType File -Force
New-Item -Path "$baseDir\sql\exemple_hive_2.sql" -ItemType File -Force
New-Item -Path "$baseDir\sql\exemple_hive.sql" -ItemType File -Force

Write-Host "Fichiers créés avec succès selon l'image !"

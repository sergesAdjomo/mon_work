@echo off
REM Création de l'arborescence producer-matomo-wa7

REM Répertoire racine
mkdir producer-matomo-wa7

REM Dossier parm
mkdir producer-matomo-wa7\parm
echo. > producer-matomo-wa7\parm\certificats.crt
echo. > producer-matomo-wa7\parm\logging.conf
echo. > producer-matomo-wa7\parm\wa7_confini
echo. > producer-matomo-wa7\parm\wa7.conf
echo. > producer-matomo-wa7\parm\wa7.env
echo. > producer-matomo-wa7\parm\wa7.fonctions.env

REM Dossier shell
mkdir producer-matomo-wa7\shell
echo. > producer-matomo-wa7\shell\common.ksh
echo. > producer-matomo-wa7\shell\UDWA7AJA0_KFK.KSH
echo. > producer-matomo-wa7\shell\UDWA7AJA1_KFK.KSH

REM Dossier src\wa7_producerMatomo
mkdir producer-matomo-wa7\src\wa7_producerMatomo

REM Sous-dossier producer
mkdir producer-matomo-wa7\src\wa7_producerMatomo\producer
echo. > producer-matomo-wa7\src\wa7_producerMatomo\producer\aspirateur.py
echo. > producer-matomo-wa7\src\wa7_producerMatomo\producer\common.py
echo. > producer-matomo-wa7\src\wa7_producerMatomo\producer\kafkasender.py
echo. > producer-matomo-wa7\src\wa7_producerMatomo\producer\localdb.py
echo. > producer-matomo-wa7\src\wa7_producerMatomo\producer\parameter.py

REM Sous-dossier tools
mkdir producer-matomo-wa7\src\wa7_producerMatomo\producer\tools
echo. > producer-matomo-wa7\src\wa7_producerMatomo\producer\tools\__init__.py
echo. > producer-matomo-wa7\src\wa7_producerMatomo\producer\tools\configuration.py

REM Fichier main.py dans src\wa7_producerMatomo
echo. > producer-matomo-wa7\src\wa7_producerMatomo\main.py

REM Dossier tests
mkdir producer-matomo-wa7\tests
echo. > producer-matomo-wa7\tests\test_base.py
echo. > producer-matomo-wa7\tests\test_connection_matomo.py
echo. > producer-matomo-wa7\tests\test_env.py
echo. > producer-matomo-wa7\tests\test_kafka.py

REM Fichiers racine
echo. > producer-matomo-wa7\.gitignore
echo. > producer-matomo-wa7\MANIFEST.in
echo. > producer-matomo-wa7\master
echo. > producer-matomo-wa7\README.md
echo. > producer-matomo-wa7\setup.py

echo Arborescence producer-matomo-wa7 créée avec succès !
#!/bin/bash

# Créer la structure de base
mkdir -p "mc5/MCS [SSH: DEV]"/{data,src,test,resources,shell,sql,tmp}

# Structure data/
touch "mc5/MCS [SSH: DEV]/data/Services_Inno_Historique_rowdetail.csv"
mkdir -p "mc5/MCS [SSH: DEV]/data"/{logapp,logctm,parm}
touch "mc5/MCS [SSH: DEV]/data/json_schema.json"
touch "mc5/MCS [SSH: DEV]/data"/mc5_conf_Postgre.conf.ini mc5_logging.conf mc5_postgre.json mc5_sqlserver.{conf,conf.ini,json} mc5.{conf,conf.ini,env,fonctions.env}

# Structure shell/
mkdir -p "mc5/MCS [SSH: DEV]/shell"/{shellapp,sql}
touch "mc5/MCS [SSH: DEV]/shell"/DDMCSA{1A1_SERVICES_INNOKSH,DA0_SERV_INNO_HIST.KSH,1A2_TRACKING_STE.KSH,1A3_TRACK_STE_DET.KSH,1A5_PY_JDBC_SOLSERVER.KSH,1B1_CAMPAGNE_MARK.KSH}
touch "mc5/MCS [SSH: DEV]/shell/shellapp"/{common.KSH,packaging.KSH,set_droits_fs.KSH}

# Structure sql/
touch "mc5/MCS [SSH: DEV]/sql"/create_service_innovant_historique.hql {exemple_hive_2.sql,example_hive.sql}

# Structure src/
mkdir -p "mc5/MCS [SSH: DEV]/src"/{campagne_marketing,service_innovant,tracking_site,traitement_spark}
touch "mc5/MCS [SSH: DEV]/src/campagne_marketing"/{campagne_marketing_dataframe.py,campagne_marketing_fields.py,campagne_marketing.py,main.py}
touch "mc5/MCS [SSH: DEV]/src/service_innovant"/{service_innovant_dataframe.py,service_innovant_fields.py,service_innovant.py,main.py}
touch "mc5/MCS [SSH: DEV]/src/tracking_site"/{tracking_site_dataframe.py,tracking_site_fields.py,tracking_site_url.py,tracking_site.py,main.py}

# Structure traitement_spark
mkdir -p "mc5/MCS [SSH: DEV]/src/traitement_spark/code"
touch "mc5/MCS [SSH: DEV]/src/traitement_spark/code"/{__init__.py,exemple.py,helper.py,settings.py,utils.py}

# Structure test/
mkdir -p "mc5/MCS [SSH: DEV]/test"/{resources,expected,input}
touch "mc5/MCS [SSH: DEV]/test/resources"/{matomo_sample.csv,perfo_test.csv,spark_*.csv,territories_feribles.csv}
touch "mc5/MCS [SSH: DEV]/test/expected/ville_agg.csv"
touch "mc5/MCS [SSH: DEV]/test/input/villes.csv"

# Fichiers divers
touch "mc5/MCS [SSH: DEV]"/{.gitignore,Jenkinsfile,README.md,setup.py}
touch "mc5/MCS [SSH: DEV]/test"/{conftest.py,perfo_test.py,utils.py}

# Créer les répertoires __pycache__ (vides)
mkdir -p "mc5/MCS [SSH: DEV]/src"/*/__pycache__

echo "Structure MC5 créée avec succès!"
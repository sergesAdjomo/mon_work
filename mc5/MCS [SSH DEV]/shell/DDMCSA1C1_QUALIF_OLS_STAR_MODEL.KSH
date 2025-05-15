#!/bin/ksh
# @(#) INGESTION PYTHON BD6 - QUALIFICATION OLS STAR MODEL
# -------------------------------------------------------------------------------------------------------------------------------------------------- #
# (E): Aucun parametres
# -------------------------------------------------------------------------------------------------------------------------------------------------- #
# NTO 2019-05-20 Creation
# SAG 2025-05-14 Adaptation pour le modèle en étoile
# -------------------------------------------------------------------------------------------------------------------------------------------------- #
. /dexploit/tools/tools.profile
jobInitialize ${0} "$@"
jobExport
set -- $_JOB_CMDLINE

# ---------------------------------------------------------------------------- #
# --------------------  PARAM SPECIFIQUE ------------------------------------- #
# ---------------------------------------------------------------------------- #
set -x
# Définition du nom de l'application - crucial pour les chemins Python
export _YCDAPPNAME=qualification_ols_star_model
export _YCICODE="${_YCICODE}"
export _YCIJOBNAME=$(basename $0|sed 's/\.KSH//')
SPARK_MAJOR_VERSION=3
export APP_CONF_FILE=${_YCICODE}.conf
# ---------------------------------------------------------------------------- #
# Functions                                                                    #
# ---------------------------------------------------------------------------- #
. ${_YCDPARM}/${_YCICODE}.env
. ${_YCDPARM}/${_YCICODE}.fonctions.env

###################################################################################################
# MAIN
###################################################################################################


# !! lancer packaging en DEV 
# EN REC -> PROD livrer les bin et supprimer cette ligne  
if [  "$(echo ${_YCDAPP} | cut -d'/' -f2 | rev | cut -d '-' -f1 | rev)" = "home" ]
then
    . ${_YCDSHELLAPP}/packaging.KSH
fi

# ---------------------------------------------------------------------------- #
# --------------------    Début du JOB   ------------------------------------- #
# ---------------------------------------------------------------------------- #
jobDebut
    DTE=$(date +%Y%m%d.%H%M%S)
    TRACE=${_YCDLOGAPP}/${DTE}_${_YCFJOB}_${_YCICODE}.log
    YARN_TRACE_NAME=${DTE}_${_YCFJOB}_${_YCICODE}_yarn.log
    LOCAL_YARN_TRACE=${_YCDLOGAPP}/${YARN_TRACE_NAME}
    HDFS_YARN_TRACE=/${ENV_DSN}/ep/traitement/${_YCICODE}/app_log/spark_yarn/$(date +%Y%m)/
    echo -e "Traces sous : ${TRACE}"
    exec 1>${TRACE}
    exec 2>&1

    set -x
    # genaration du fichier de conf a partir du fihcier conf.ini
    fg_gen_properties ${_YCDPARM}/${_YCICODE}.conf.ini ${_YCDPARM}/${APP_CONF_FILE}

    # creéation du ticket kerberos avec le compte appli
    export TMP_TGT_NAME=${KEYTAB_NAME}.$$ 
    export KRB5CCNAME=/tmp/${TMP_TGT_NAME}
    fg_kinit "$KEYTAB_PATH" "$KEYTAB_USER"

    echo -e "users ayant acces a Spark UI : $SPARKUI_USERS"
	# Lancement du Spark submit avec le module qualification_ols_star_model 
    # Spark utilise le nom du module Python relatif au PYTHONPATH
    # Nous utilisons le module qualification_ols_star_model/main.py
    export MAIN="main.py"
    
    echo "Module principal à exécuter : ${MAIN}"
    
    # Personnalisation des paramètres Spark pour le modèle en étoile
    export DRIVER_MEMORY="2g"        # Plus de mémoire pour le driver
    export EXECUTOR_MEMORY="4g"      # Plus de mémoire pour les exécuteurs
    export EXECUTOR_CORES="2"        # Plus de coeurs pour le traitement
    export NUM_EXECUTORS="4"         # Plus d'exécuteurs pour le parallélisme

    if [ "${SPARK_MAJOR_VERSION}" == "3" ] ;then
        echo "Lancement du Spark 3 pour le modèle en étoile"
        
        # Lancement avec spécification explicite du module qualification_ols_star_model
        spark3submit
    else
        echo "Lancement du Spark 2 pour le modèle en étoile"
        
        # Lancement avec spécification explicite du module qualification_ols_star_model
        sparksubmit
    fi
    # Récupération du code retour
    SPARK_RETURN_CODE=$?
    
    # Récupération de la log YARN
    get_yarn_spark_log

    # Vérification de la bonne exécution
    jobTest $SPARK_RETURN_CODE

    # Destruction du ticket kerberos
    kdestroy

    echo -e "FIN DU BATCH - MODÈLE EN ÉTOILE QUALIFICATION OLS"

jobFin
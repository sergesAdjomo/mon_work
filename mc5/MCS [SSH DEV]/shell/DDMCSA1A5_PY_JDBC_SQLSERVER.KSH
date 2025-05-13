 
#!/bin/ksh
# @(#) INGESTION PYTHON BD6
# -------------------------------------------------------------------------------------------------------------------------------------------------- #
# (E): Aucun parametres
# -------------------------------------------------------------------------------------------------------------------------------------------------- #
# NTO 2019-05-20 Creation
# -------------------------------------------------------------------------------------------------------------------------------------------------- #
. /dexploit/tools/tools.profile
jobInitialize ${0} "$@"
jobExport
set -- $_JOB_CMDLINE

# ---------------------------------------------------------------------------- #
# --------------------  PARAM SPECIFIQUE ------------------------------------- #
# ---------------------------------------------------------------------------- #
set -x
export SPARK_MAJOR_VERSION=3
export _YCDAPPNAME=spark_jdbc
export _YCIJOBNAME=$(basename $0|sed 's/\.KSH//')
export APP_CONF_FILE=${_YCICODE}_sqlserver.conf
export JSON_FILE=${_YCDPARM}/${_YCICODE}_sqlserver.json
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
    fg_gen_properties ${_YCDPARM}/${_YCICODE}_sqlserver.conf.ini ${_YCDPARM}/${APP_CONF_FILE}

    # creéation du ticket kerberos avec le compte appli
    export TMP_TGT_NAME=${KEYTAB_NAME}.$$ 
    export KRB5CCNAME=/tmp/${TMP_TGT_NAME}
    fg_kinit "$KEYTAB_PATH" "$KEYTAB_USER"
 
    
    echo -e "users ayant acces a Spark UI : $SPARKUI_USERS"

    # Lancement du Spark submit 
    spark3submit
    # Récupération du code retour
    SPARK_RETURN_CODE=$?
    
    # Récupération de la log YARN
    get_yarn_spark_log

    # Vérification de la bonne exécution
    jobTest $SPARK_RETURN_CODE

    # Destruction du ticket kerberos
    kdestroy

    echo -e "FIN DU BATCH"

jobFin
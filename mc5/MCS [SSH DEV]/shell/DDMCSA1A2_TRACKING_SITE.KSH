 
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
export _YCDAPPNAME=service_innovant_historique
export _YCICODE="${_YCICODE}"
export _YCIJOBNAME=$(basename $0|sed 's/\.KSH//')
export APP_CONF_FILE=${_YCICODE}.conf
# ---------------------------------------------------------------------------- #
# Functions                                                                    #
# ---------------------------------------------------------------------------- #
. ${_YCDPARM}/${_YCICODE}.env
. ${_YCDPARM}/${_YCICODE}.fonctions.env

###################################################################################################
# MAIN
###################################################################################################


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
    
	#Chargement des donnees historiques dans HDFS
	export hdfs_path="/${ENV_DSN}/ep/lac/${_YCICODE}/app_db_external/db_${ENV_DSN}_${_YCICODE}.db/service_innovant_histo/"

	hdfs dfs -mkdir -p $hdfs_path                                   
	hdfs dfs -rm -r $hdfs_path/service_innovant_histo.csv                      
	hdfs dfs -put ${_YCDDATA}/Services_Inno_Historique_rowdetail.csv $hdfs_path/service_innovant_histo.csv

	#Creation de la structure Hive
	fl_exec_hql create_service_innovant_historique.hql

    # Destruction du ticket kerberos
    kdestroy

    echo -e "FIN DU BATCH"

jobFin

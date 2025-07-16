#!/bin/ksh
# ------------------------------------------------------------------- #
# producerMatomo : aspiration matomo
# ------------------------------------------------------------------- #

. /dexploit/tools/tools.profile
jobInitialize ${0} "$*"
jobExport
set ${_JOB_CMDLINE}

# ------------------------------------------------------------------- #
# ENVIRONNEMENT
# ------------------------------------------------------------------- #
. ${_YCDPARM}/wa7.env
. ${_YCDPARM}/wa7.fonctions.env
. ${_YCDSHELL}/common.ksh

# Appel de la fonction de génération du fichier de configuration
fg_gen_properties ${_YCDPARM}/${_YCICODE}_conf.ini ${_YCDPARM}/${_YCICODE}.conf

export SOURCE_NAME=aspiration_matomobdt
# ------------------------------------------------------------------- #
# MAIN
# ------------------------------------------------------------------- #
set -o errexit # set -e : sortie en erreur en cas d'echec d'une commande
jobDebut
    jobEtape "Test de la date"
    if [ "$1" -eq NULL ]
    then
        mydate=$(date +%Y%m%d)
    else
        mydate=$1
    fi

    # Recuperation ticket kerberos pour authentification
    jobEtape "Initialisation de l environement Kerberos"
    export KRB5CCNAME=/tmp/${KEYTAB_NAME}.$$
    fg_kinit "$KEYTAB_PATH" "$KEYTAB_USER"

    jobEtape "batch python : Debut de aspiration ${SOURCE_NAME}"
    ${_YCDBIN}/producerMatomo-1.1.1_venv/bin/python3 -m wa7.producerMatomo.main \
	--logconfiguration=${_YCDPARM}/logging.conf \
    --jobconfiguration=${_YCDPARM}/${_YCICODE}.conf \
	--datectrlm=${mydate} \
    --matomo_id_site=${MATOMO_ID_SITE} \
    --matomo_api_url=${MATOMO_API_URL} \
    --matomo_api_token=${MATOMO_API_TOKEN} \
    --matomo_sps_user=${MATOMO_SPS_USER} \
    --matomo_sps_mdp=${MATOMO_SPS_MDP}

    kdestroy
jobFin

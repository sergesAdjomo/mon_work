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

. ${_YCDPARM}/${_YCICODE}.env
. ${_YCDPARM}/${_YCICODE}.fonctions.env

export REC_GROUPE_OWNER=gf_hdp_${ENV_DSN}_${_YCICODE}_moe


chown -R ${USER}:${REC_GROUPE_OWNER} ${_YCDAPP}*
chmod 775 -R ${_YCDAPP}*
#!/bin/ksh
# @(#) PACKAGING ICDC DATA
# -------------------------------------------------------------------------------------------------------------------------------------------------- #
# (E): Aucun parametres
# -------------------------------------------------------------------------------------------------------------------------------------------------- #
# LBH 2024-02-01 Creation
# -------------------------------------------------------------------------------------------------------------------------------------------------- #
. /dexploit/tools/tools.profile
jobInitialize ${0} "$@"
jobExport
set -- $_JOB_CMDLINE
. ${_YCDPARM}/${_YCICODE}.env
. ${_YCDPARM}/${_YCICODE}.fonctions.env

export VIRTUAL_ENV_PATH='/applications/dev/virtualenv'
export VIRTUAL_ENV="BD3_venv_${PYTHON_VERSION}"
export VENV=${_YCICODE}-${PACKAGE_VERSION}_venv

jobDebut
echo "$(date -u)"
cd ~
if [ -d ${_YCDBIN}/${VENV} ]; then
    echo "supression du dossier basket"
    rm -R ${_YCDBIN}/*
fi
if [ -d dist/ ]; then
    echo "supression du dossier dist"
    rm -R dist
fi
if [ -d basket/ ]; then
    echo "supression du dossier basket"
    rm -R basket
fi
echo "Activation du Venv"
source ${VIRTUAL_ENV_PATH}/${VIRTUAL_ENV}/bin/activate
echo "Lancement du packaging"
build_bd_pyspark -a ${_YCICODE}
echo "dézip et déplacement sur le bin"
tar -xzvf dist/${_YCICODE}-${PACKAGE_VERSION}.tar.gz -C ${_YCDTMP}
cp -R ${_YCDTMP}/bin/* ${_YCDBIN}
rm -R ${_YCDTMP}/*
echo -e "FIN DU BATCH"
echo "$(date -u)"

jobFin

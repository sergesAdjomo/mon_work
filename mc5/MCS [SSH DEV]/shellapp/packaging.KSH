 
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

export VIRTUAL_ENV_PATH='/applications/dev/virtualenv'

if [[ -z "$1" || "$1" != "2" ]]
    then export SPARK_MAJOR_VERSION=3
    export VIRTUAL_ENV="BD3_venv_3.8.2"
else
    export SPARK_MAJOR_VERSION=2
    export VIRTUAL_ENV="BD3_venv_3.6.9"
fi

export PACKAGE_VERSION=`grep -oP "(?<=version=(\'|\"))[^(\'|\")]+" ${_YCDAPP}/setup.py`
export VENV=${_YCICODE}-${PACKAGE_VERSION}_venv

. ${_YCDPARM}/${_YCICODE}.env
. ${_YCDPARM}/${_YCICODE}.fonctions.env
jobDebut
    if [ -d "${_YCDBIN}/${_YCICODE}-${PACKAGE_VERSION}_venv" ]; then
        echo "Le venv existe"
        echo "copy du ${_YCDAPP}/src dans ${_YCDBIN}/${_YCICODE}-${PACKAGE_VERSION}_venv/lib/python3.8/site-packages "
        cp -R ${_YCDAPP}/src/* ${_YCDBIN}/${_YCICODE}-${PACKAGE_VERSION}_venv/lib/python3.8/site-packages
    else
        echo "Le venv n'existe pas "
        echo "`date -u`"
        echo "creation du venv sur le home"
        cd ~;
        if [ -d ${_YCDBIN}/${VENV} ]; then echo "supression du dossier basket" 
        rm -R ${_YCDBIN}/*
        fi
        if [ -d dist/ ]; then echo "supression du dossier dist" 
        rm -R dist
        fi
        if [ -d basket/ ]; then echo "supression du dossier basket" 
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
        echo "lancement du common"
        . ${_YCDSHELLAPP}/common.KSH

        echo "zip du venv ${VENV}"
        cd ${_YCDBIN}/${VENV};zip -r ${_YCDBIN}/${_YCICODE}-${PACKAGE_VERSION}_pyspark_payload.zip *  
        echo -e "FIN DU BATCH"
        echo "`date -u`"

    fi
jobFin
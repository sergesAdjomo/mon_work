import setuptools

setuptools.setup(
    ## Métadonnées du paquet
    # à amender en fonction du contexte du projet
    name="mc5",
    # Indiquez ici le numéro de version courant
    version="5.4.2",
    packages=setuptools.find_namespace_packages(where="src", include=["*"]),
    package_dir={"": "src"},
    url="http://bitbucket.serv.cdc.fr/scm/mc5/module-mc5.git",
    license="proprietary",
    author="Equipe du projet mc5 datamartclient",
    author_email="Equipe du projet mc5",
    description="Readme du projet mc5 datamartclient module",
    long_description=open("README.md").read(),
    ## Informations sur les points d'entrée de ce paquet
    ## Informations de dépendances de ce paquet
    # Indiquez ici les dépendances du paquet
    # La précision de la version est facultative, mais recommandée
    # Sauf exception, cette liste ne doit pas rester vide
    install_requires=["venv-pack==0.2.0", "hadoop-utils-icdc==0.0.39", "ingesteur_spark_jdbc==1.2.0","xlrd"],
    entry_points={
        "console_scripts": [
            "job_traitement_mc5 = job_traitement_mc5.main:main",
            "job_traitement_streaming_mc5 = job_traitement_streaming_mc5.main:main",
        ]
    },
)

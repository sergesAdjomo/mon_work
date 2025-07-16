import setuptools

setuptools.setup(
    
    ## Métadonnées du paquet
    
    # à amender en fonction du contexte du projet
    
    name='ci6',
    
    # Indiquez ici le numéro de version courant
    version='0.0.1',
    packages=setuptools.find_namespace_packages(where="src", include=["*"]),
    package_dir={'': 'src'},
    url='http://bitbucket.serv.cdc.fr/scm/ci6/module-ci6.git',
    license='proprietary',
    author='Equipe du projet ci6 ci6_consumer',
    author_email='Equipe du projet ci6',
    description='Readme du projet ci6 ci6_consumer ci6_consumer',
    long_description=open('README.md').read(),

    ## Informations sur les points d'entrée de ce paquet
 
    ## Informations de dépendances de ce paquet
    
    # Indiquez ici les dépendances du paquet
    # La précision de la version est facultative, mais recommandée
    # Sauf exception, cette liste ne doit pas rester vide
    install_requires=[
        "hadoop_utils_icdc~=0.0.41",
        "kafka-logging-handler-icdc~=0.0.7",
        ],
    entry_points={
        "console_scripts": [
            "job_traitement_ci6 = job_traitement_ci6.main:main",
            "job_traitement_streaming_ci6 = job_traitement_streaming_ci6.main:main"
        
        ]
    }
)

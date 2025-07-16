import setuptools

setuptools.setup(
    
    ## Métadonnées du paquet
    
    # à amender en fonction du contexte du projet
    
    name='producerMatomo',
    
    # Indiquez ici le numéro de version courant
    version='1.1.1',
    packages=setuptools.find_namespace_packages(where='src', include=['wa7.*']),
    namespace_packages=['wa7'],
    package_dir={'': 'src'},
    url='http://bitbucket.serv.cdc.fr/scm/wa7/producer-matomo-wa7.git',
    license='proprietary',
    author='bcc',
    author_email='LD-DEI-BCCT-MOE',
    description='aspiration matomo',
    long_description=open('README.rst').read(),

    ## Informations sur les points d'entrée de ce paquet

    # Permet de définir des noms de scripts qui seront disponible dans l'environnement ou le paquet sera installé

    entry_points={
        "console_scripts": [
            'producer = wa7.producerMatomo.main:main',
        ]
    },
 
    ## Informations de dépendances de ce paquet
    
    # Indiquez ici les dépendances du paquet
    # La précision de la version est facultative, mais recommandée
    # Sauf exception, cette liste ne doit pas rester vide
    install_requires=[
        'requests >= 2.23',
        'urllib3',
        'SQLAlchemy<2',
        "confluent-kafka==1.9.2"
        # ...
    ],
    
    # Option avancéecat 
    # Indiquez ici des dépendances optionnelles permettant d'activer des fonctionnalités supplémentaires de votre paquet
    extras_require={
        # 'pdf_gen': ['reportlab>=3.5'],
        # ...
    },

    ## Informations nécessaires au système de paquet
    
    # Sauf cas particulier, il n'est pas necessaire de retoucher ces lignes
    include_package_data=True,
    zip_safe=False,
)
 

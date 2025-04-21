import setuptools

setuptools.setup(



## Métadonnées du paquet



# à amender en fonction du contexte du projet



name='ingesteur_spark_jdbc',

#name='xx1',

# Indiquez ici le numéro de version courant

version="1.0.0",

url='',

license='proprietary',

author='cumanisaa-e',

author_email='cds.umanisaa-e@caissedesdepots.fr',

description='Ceci est une pattern ingestion pyspark JDBC',

long_description=open('README.rst').read(),

## Informations sur les points d'entrée de ce paquet

# Permet de définir des noms de scripts qui seront disponible dans l'environnement ou le paquet sera installé

entry_points={

"console_scripts": [

# 'nom_du_binaire = icdc.convert_format_table.convert_format_table.nom_module:nom_fonction',

]

},



## Informations de dépendances de ce paquet



# Indiquez ici les dépendances du paquet

# La précision de la version est facultative, mais recommandée

# Sauf exception, cette liste ne doit pas rester vide

install_requires=[

"hadoop-utils-icdc~=0.0.36",

"kafka-python-icdc[krb5_auto]",

"kafka-logging-handler-icdc",

"fastavro",

"pytz"

],



# Option avancée

# Indiquez ici des dépendances optionnelles permettant d'activer des fonctionnalités supplémentaires de votre paquet

extras_require={

},

## Informations nécessaires au système de paquet



# Sauf cas particulier, il n'est pas necessaire de retoucher ces lignes

packages=setuptools.find_namespace_packages(where='src', include=['*']),

package_dir={'': 'src'},

include_package_data=True,

zip_safe=False,

)
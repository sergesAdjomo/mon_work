Ingesteur Spark JDBC

=====================================================

1. Fonctionnalités :

Le module ingesteur_spark_JDBC permet d’ingérerdes données sur HIve depuis une base de données SGBD en utilisant une connection JDBC.

2. Paramétrage :

Le parametrage des ingestion est défini dans un fichier json. Ci-dessous un exemple de parametrage:

"jdbc_tables_infos": [

{

"jdbc_table": "data_gen",

"jdbc_schema": "omair",

"jdbc_ingest_type": "delta_query",

"jdbc_reference_column": "date_maj",

"jdbc_reference_column_type": "timestamp(6)",

"jdbc_partition_column": "",

"jdbc_partition_column_type" : "",

"jdbc_num_partitions": 8,

"jdbc_fetchsize": 10000,

"jdbc_lower_bound": "",

"jdbc_upper_bound": "",

"jdbc_query":"select id,nom,date_maj from omair.data_gen",

"hive_table": "data_gen_posgres_deltaquery",

"hive_database": "{{db_hive_brute}}",

"hive_table_full": "data_gen_full",

"hive_ingest_full": false,

"hive_schema_partition":"",

"hive_table_format": "parquet"

}

]

}

Détails des paramètres

"jdbc_table":

Le nom de la table à ingérer

"jdbc_schema":

Le schema de la base où se trouve la table.

"jdbc_ingest_type":

Renseigne le mode d'ingestion de la table . Les valeurs possibles sont: "full", "delta", "query" et "delta_query"



• Le mode "full" ingère tout le contenu de la table sans aucun filtre appliqué.



• Le mode "delta" permet de filtrer les données de la table en se basant une colonne de delta obligatoire. C'est le paramètre "jdbc_reference_column"

• Le mode "query" permet de filtrer les données de la table en appliquant la requête indiquée dans l'options "jdbc_query"



• Le mode "delta_query" permet de filtrer les données de la table en appliquant la requête indiquée dans l'options "jdbc_query" ensuite applique le filtre en se basant sur une colonne de delta.

Le mode "delta_query" combine le mode "query" et le mode "delta". Dans ce mode, il est obligatoire de spécifier les paramètres "query" et "jdbc_reference_column"

"jdbc_reference_column":

Paramètre servant à indiquer la colonne de delta dans la table .



Ce paramètre est obligatoire lorsque "jdbc_ingest_type" est "delta" ou "delta_query"

"jdbc_reference_column_type":

Permet d'indiquer le type de la colonne de delta. En théorie, les types possibles sont: numériques, date et timestamp

Attention: Jsuqu'à la version 0.0.16, seul le type Data et timesatmp sont supportés.

TODO: Ajouter la prise en charge du type numérique

"jdbc_partition_column" :

Paramètre spark JDBC permet de spécifier une colonne qui sert à définir le degré de parallélisme.

Ce paramètre est optionnel. Mais lorsqu'il est indiqué, il faut obligatoirement indiquer avec les paramètres "jdbc_lower_bound" et jdbc_upper_bound

Voir la doc: https://downloads.apache.org/spark/docs/3.3.2/sql-data-sources-jdbc.html

"jdbc_lower_bound" et "jdbc_upper_bound" :

Paramètre spark JDBC permet de définir la plage de valeur de la colonne de partitionnement.

Ces deux paramètres sont obligatoires lorsque "jdbc_partition_column" est renseigné.

Les positionner à "auto" si vous voulez utiliser le mode automatique (calcul du min et max de la colonne de partitionnement)

"jdbc_partition_column_type":

"date", "timestamp" ou "int"

Indique le type de la colonne de partitionnement. Ce paramètre est obligatoire lorsque "jdbc_partition_column" est renseigné

"jdbc_num_partitions":

Paramètre spark JDBC permet de setter le nombre maximum de partition lors de la lecture de la table .

Voir la doc: https://downloads.apache.org/spark/docs/3.3.2/sql-data-sources-jdbc.html

"jdbc_fetchsize":

Paramètre spark JDBC le nombre de ligne à charger par lot en interrogeant la table .

Voir la doc: https://downloads.apache.org/spark/docs/3.3.2/sql-data-sources-jdbc.html

"jdbc_query":

Permet de spécifier la requête à exécute sur la table . Ce paramètre est renseigné lorsque "jdbc_ingest_type" est "delta" ou "delta_query"

"hive_table":

Indique le nom de la table hive à charger

"hive_database":

Indique le nom de la database hive à charger

"hive_database_path":

Indique le chemin hdfs de la base Hive

"hive_table_full":

Indique le nom de la table hive full à charger

""hive_ingest_full":

Boolean, Indique s'il faut alimenter le table hive full ou pas : true ou false

"hive_schema_partition":

Indique les colonnes de partitionnement de la table hive cible

""hive_write_mode":

Indique le mode d'écriture de la table hive.

Trois modes sont possibles: "overwrite","append" et "overwrite_par_partition"

• Le mode "overwrite" écrase toute la table

• Le mode "overwrite_par_partition" écrase uniquement la partition si elle existe, sinon la partition est simplement ajoutéelle

• Le mode "append" ajoute simplement les lignes dans la table.

"hive_table_format":

Indique le format de stockage de la table hive: Parquet, etc..



3. Paramétrage 2 :

Fichier ${CODEAPP}.conf.ini:

L'ingestion spark jdbc est spécifiée dans le fichier conf.ini dans une section dédiée nommée [SRC_DATABASE]

Voici un exemple de configuration du fichier conf.ini défini pour l'application QX0

[SRC_DATABASE]

JOB_NAME : ${_YCDAPPNAME}

JOB_CTRLM : ${_YCIJOBNAME}

SRC_DATABASE_TYPE :

SRC_CONNEXION_TYPE : JDBC

SRC_URL_JDBC : ${CONNEXION_JDBC}

SRC_USER: ${CONNEXION_USER}

SRC_PASSWORD:

SRC_JKS_PATH: ${CONNEXION_JKS_PATH}

SRC_JKS_ALIAS: ${CONNEXION_JKS_ALIAS}

SRC_WALLET_PATH:

PARAM_METADATA_JSON :${INGEST_METADATA_JSON}

LAST_VALUE_FILE_SNAPSHOT : /${ENV_DSN}/ep/flux_entrant/${_YCICODE}/depot/${_YCDAPPNAME}/last_value/snapshot${JSON_SPEC}

LAST_VALUE_FILE_FULL : /${ENV_DSN}/ep/flux_entrant/${_YCICODE}/depot/${_YCDAPPNAME}/last_value/full${JSON_SPEC}

TX_EXPLOIT : /${ENV_DSN}/ep/flux_entrant/${_YCICODE}/app_db_external/db_${ENV_DSN}_${_YCICODE}_brute.db/tx_exploit

NB_JDBC_PARALLEL:3
# ingesteur_spark_JDBC/src/spark_jdbc/code/ingestion.py



from spark_jdbc.code.table import Table

from pyspark.sql.functions import col, max

from spark_jdbc.code.constants import SCHEMA_LAST_VALUE

from pyspark.sql.functions import lit

from pyspark.sql.types import TimestampType

from datetime import datetime

import time

class Ingestion:

    def __init__(self, table:Table):
        """
        Initialise une instance de la classe avec une table donnée.
        
        Args:
            table (Table): La table à utiliser pour l'initialisation.
        
        Attributs:
            table (Table): La table fournie pour l'initialisation.
            settings (dict): Les paramètres associés à la table.
        """
        self.table = table
        self.settings = table.settings
        self.__set_driver_class()

    def __set_driver_class(self):
        """
        Définit la classe du driver JDBC en fonction du type de base de données source spécifié dans les paramètres.
        
        Cette méthode vérifie le type de base de données source (src_database_type) et assigne la classe de driver JDBC appropriée à l'attribut driver_class.
        
        Si le type de base de données n'est pas pris en charge, une exception ValueError est levée.
        
        Types de bases de données pris en charge :
        - postgre : "org.postgresql.Driver"
        - oracle : "oracle.jdbc.driver.OracleDriver"
        - mysql : "com.mysql.cj.jdbc.Driver"
        - sqlserver : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
        
        Raises:
            ValueError: Si le type de base de données source n'est pas pris en charge.
        """
        if self.settings.src_database_type == "postgre":
            self.driver_class = "org.postgresql.Driver"
        elif self.settings.src_database_type == "oracle":
            self.driver_class = "oracle.jdbc.driver.OracleDriver"
        elif self.settings.src_database_type == "mysql":
            self.driver_class = "com.mysql.cj.jdbc.Driver"
        elif self.settings.src_database_type == "sqlserver":
            self.driver_class ="com.microsoft.sqlserver.jdbc.SQLServerDriver"
        else:
            raise ValueError(f"Unsupported jdbc database type: {self.settings.src_database_type}")

    def get_jdbc_table_name(self):
        """
        Récupère le nom de la table JDBC en fonction du type d'ingestion.
        
        Cette méthode utilise un dictionnaire de gestionnaires pour déterminer
        la fonction de gestion appropriée en fonction du type d'ingestion JDBC
        spécifié dans l'attribut jdbc_ingest_type de la table.
        
        Types d'ingestion supportés et leurs gestionnaires associés :
        - 'delta': géré par _handle_delta_ingest
        - 'full': géré par _handle_full_ingest
        - 'query': géré par _handle_query_ingest
        - 'delta_query': géré par _handle_delta_query_ingest
        
        Si le type d'ingestion spécifié n'est pas supporté, une exception
        ValueError est levée.
        
        Returns:
            str: Le nom de la table JDBC après traitement par le gestionnaire approprié.
        
        Raises:
            ValueError: Si le type d'ingestion spécifié n'est pas supporté.
        """
        jdbc_ingest_type_handlers = {
            'delta': self._handle_delta_ingest,
            'full': self._handle_full_ingest,
            'query': self._handle_query_ingest,
            'delta_query': self._handle_delta_query_ingest
        }
        
        handler = jdbc_ingest_type_handlers.get(self.table.jdbc_ingest_type)
        
        if not handler:
            raise ValueError(f"Unsupported ingest type: {self.table.jdbc_ingest_type}")
        
        return handler()



    def _handle_delta_ingest(self):
        """
        Gère l'ingestion delta pour différentes bases de données source.
        
        Cette méthode génère une requête SQL pour sélectionner les nouvelles lignes à partir d'une table source,
        en fonction du type de base de données source spécifié dans les paramètres.
        
        Returns:
            str: La requête SQL pour l'ingestion delta.
        
        Raises:
            ValueError: Si le type de base de données source n'est pas pris en charge.
        
        Notes:
            - Pour les bases de données PostgreSQL, la requête sélectionne les lignes où la colonne de référence est supérieure à une valeur donnée.
            - Pour les bases de données Oracle, la requête sélectionne les lignes où la colonne de référence est supérieure à une date donnée.
            - Pour les bases de données SQL Server, la requête sélectionne les lignes où la colonne de référence est supérieure à une valeur donnée.
        """
        if self.settings.src_database_type == "postgre":
            return f"(select * from {self.table.jdbc_schema}.{self.table.jdbc_table} where {self.table.jdbc_reference_column} > '{self.table.reference_column_max_value}') as tbl"
        
        if self.settings.src_database_type == "oracle":
            return f"(SELECT * FROM {self.table.jdbc_schema}.{self.table.jdbc_table} WHERE {self.table.jdbc_reference_column} > TO_DATE('{self.table.reference_column_max_value}', 'YYYY-MM-DD HH24:MI:SS'))"
        
        if (self.settings.src_database_type == "sqlserver") or (self.settings.src_database_type == "mysql"):
            return f"select * from {self.table.jdbc_schema}.{self.table.jdbc_table} where {self.table.jdbc_reference_column} > '{self.table.reference_column_max_value}'"
        
        raise ValueError(f"Unsupported source database type for delta ingest: {self.settings.src_database_type}")

    def _handle_full_ingest(self):
        """
        Gère l'ingestion complète des données depuis une table JDBC.
        
        Si la colonne de partition JDBC de la table n'est pas nulle et contient des éléments,
        retourne le nom complet de la table avec son schéma. Sinon, retourne une requête SQL
        pour sélectionner toutes les données de la table.
        
        Returns:
            str: Le nom complet de la table avec son schéma ou une requête SQL pour sélectionner
            toutes les données de la table.
        """
        if self.table.jdbc_partition_column is not None and len(self.table.jdbc_partition_column)>0:
            return f"{self.table.jdbc_schema}.{self.table.jdbc_table}"
        else:
            return f" SELECT * FROM {self.table.jdbc_schema}.{self.table.jdbc_table}"

    def _handle_query_ingest(self):
        """
        Gère l'ingestion de la requête JDBC.
        
        Cette méthode renvoie la requête JDBC associée à la table.
        
        Returns:
            str: La requête JDBC de la table.
        """
        return self.table.jdbc_query



def _handle_delta_query_ingest(self):
    """
    Gère l'ingestion des requêtes delta en fonction du type de base de données source.

    Cette méthode construit et retourne une requête SQL pour extraire les données delta
    à partir de la base de données source spécifiée. La requête est construite en fonction
    du type de base de données source (PostgreSQL, Oracle, SQL Server) et utilise une
    colonne de référence pour filtrer les nouvelles données.

    Returns:
        str: La requête SQL pour l'ingestion des données delta.

    Raises:
        ValueError: Si le type de base de données source n'est pas supporté.
    """
    if self.settings.src_database_type == "postgre":
        if self.table.jdbc_partition_column is not None and len(self.table.jdbc_partition_column) > 0:
            return f"(WITH query AS ({self.table.jdbc_query}) SELECT * FROM query WHERE {self.table.jdbc_reference_column} > '{self.table.reference_column_max_value}') as v"
        else:
            return f"(WITH query AS ({self.table.jdbc_query}) SELECT * FROM query WHERE {self.table.jdbc_reference_column} > '{self.table.reference_column_max_value}')"

    if self.settings.src_database_type == "oracle":
        if self.table.jdbc_partition_column is not None and len(self.table.jdbc_partition_column) > 0:
            return f"(WITH query as ({self.table.jdbc_query}) SELECT * FROM query WHERE {self.table.jdbc_reference_column} > TO_DATE('{self.table.reference_column_max_value}', 'YYYY-MM-DD HH24:MI:SS')) v"
        else:
            return f"WITH query as ({self.table.jdbc_query}) SELECT * FROM query WHERE {self.table.jdbc_reference_column} > TO_DATE('{self.table.reference_column_max_value}', 'YYYY-MM-DD HH24:MI:SS')"

    if (self.settings.src_database_type == "sqlserver") or (self.settings.src_database_type == "mysql"):
        if self.table.jdbc_partition_column is not None and len(self.table.jdbc_partition_column) > 0:
            return f"(SELECT * FROM ({self.table.jdbc_query}) AS subquery WHERE {self.table.jdbc_reference_column} > '{self.table.reference_column_max_value}') as v"
        else:
            return f"SELECT * FROM ({self.table.jdbc_query}) AS subquery WHERE {self.table.jdbc_reference_column} > '{self.table.reference_column_max_value}'"
            
    raise ValueError(f"Unsupported source database type for delta_query ingest: {self.settings.src_database_type}")



def get_bounds(self):
    """
    Calcule les bornes inférieure et supérieure pour le partitionnement des données.

    Cette méthode calcule la valeur min et max de la colonne de partitionnement pour déterminer les paramètres lowerBound et upperBound.

    Returns:
        tuple: Un tuple (lowerBound, upperBound) contenant les bornes pour le partitionnement.
    """
    self.settings.logger.info(f"On détermine lowerBound et upperBound de manière dynamique à partir de la colonne {self.table.jdbc_partition_column} de la table {self.get_jdbc_table_name()}")

    if self.settings.src_database_type == "oracle" and self.table.jdbc_partition_column_type == "date":
        # on crée un number a partir de la date pour parraléliser l'import
        query = f"""SELECT min(to_number(to_char({self.table.jdbc_partition_column}, 'yyyymmddhh24miss'))) AS "min_partition", max(to_number(to_char({self.table.jdbc_partition_column}, 'yyyymmddhh24miss'))) AS "max_partition" FROM {self.get_jdbc_table_name()}"""
    else:
        query = f"SELECT min({self.table.jdbc_partition_column}), max({self.table.jdbc_partition_column}) FROM {self.get_jdbc_table_name()}"

    df_min_max = (self.settings.spark.read
        .format("jdbc")
        .option("driver", self.driver_class)
        .option("url", self.settings.src_url_jdbc)
        .option("query", query)
        .option("user", self.settings.src_user)
        .option("password", self.settings.src_password)
        .load())

    # On vérifie que la requête a bien renvoyé des valeurs, il peut ne pas en avoir (delta à vide par exemple)
    if (df_min_max.collect()[0][0] is None) or (df_min_max.collect()[0][1] is None):
        return None, None

    if (self.settings.src_database_type == "oracle" and self.table.jdbc_partition_column_type == "date") or (self.table.jdbc_partition_column_type == "int"):
        lowerBound, upperBound = str(int(df_min_max.collect()[0][0])), str(int(df_min_max.collect()[0][1]))
    else:
        lowerBound, upperBound = str(df_min_max.collect()[0][0]), str(df_min_max.collect()[0][1])

def read_table(self):
    """
    Lit une table JDBC et charge les données dans un DataFrame Spark.

    Cette méthode lit une table JDBC en utilisant les paramètres de connexion et les options de partitionnement
    spécifiés dans les attributs de l'objet. Les données sont ensuite chargées dans un DataFrame Spark.

    Returns:
        None

    Attributs:
        jdbc_table_name (str): Le nom de la table JDBC à lire.
        jdbc_table_df (DataFrame): Le DataFrame Spark contenant les données lues de la table JDBC.
        jdbc_table_df_empty (bool): Indique si le DataFrame est vide après la lecture des données.

    Log:
        Info: Indique le début de la lecture de la table.
        Debug: Affiche la requête envoyée pour charger la table.
        Info: Indique si la récupération des données est vide.
    """
    jdbc_table_name = self.get_jdbc_table_name()
    self.settings.logger.info(f"Lecture de la table {self.table.jdbc_schema}.{self.table.jdbc_table} en {self.table.jdbc_ingest_type}")
    
    # initialisation de la fonction de read()
    self.jdbc_table_df = (self.settings.spark.read.format("jdbc")
        .option("url", self.settings.src_url_jdbc)
        .option("driver", self.driver_class)
        .option("user", self.settings.src_user).option("password", self.settings.src_password)
        .option("numPartitions", self.table.jdbc_num_partitions)
        .option("fetchsize", self.table.jdbc_fetchsize))

    if self.table.jdbc_partition_column is not None and len(self.table.jdbc_partition_column) > 0:
        self.settings.logger.debug(f"Le requete envoye est dtable : {jdbc_table_name} pour charger la table {self.table.hive_table}")

        if self.table.jdbc_lower_bound == "auto" or self.table.jdbc_upper_bound == "auto":
            lowerBound, upperBound = self.get_bounds()
            if self.table.jdbc_lower_bound != "auto":
                lowerBound = self.table.jdbc_lower_bound
            if self.table.jdbc_upper_bound != "auto":
                upperBound = self.table.jdbc_upper_bound
            
            self.settings.logger.info(f"lowerBound : {lowerBound} upperBound : {upperBound}")
            
            # si on a pas de bound on retire la partition partition
            if not((lowerBound is None) or (upperBound is None) or (lowerBound == "") or (upperBound == "")):
                if self.settings.src_database_type == "oracle" and self.table.jdbc_partition_column_type == "date":
                    # sur oracle si on est sur une date on va créer une colonne "partition" de type number pour paralléliser l'import
                    dbtable = f"""(SELECT to_number(to_char({self.table.jdbc_partition_column}, 'yyyymmddhh24miss')) AS "partition", tb.* FROM {jdbc_table_name} tb) alias"""
                    self.settings.logger.debug(f"Le requete envoye est dbtable : {dbtable} pour charger la table {self.table.hive_table}")
                    self.jdbc_table_df = (self.jdbc_table_df.option("dbtable", dbtable)
                        .option("partitionColumn", "partition")
                        .option("lowerBound", lowerBound)
                        .option("upperBound", upperBound))
                else:
                    self.settings.logger.debug(f"Le requete envoye est dbtable : {jdbc_table_name} pour charger la table {self.table.hive_table}")
                    self.jdbc_table_df = (self.jdbc_table_df.option("dbtable", jdbc_table_name)
                        .option("partitionColumn", self.table.jdbc_partition_column)
                        .option("lowerBound", lowerBound)
                        .option("upperBound", upperBound))
    else:
        # Quand l'option partitionColumn est None, on convertit la requête en objet query. L'objet dtable ne supporte pas partitionColumn=None.
        self.settings.logger.debug(f"Le requete envoye est query : {jdbc_table_name} pour charger la table {self.table.hive_table}")
        self.jdbc_table_df = self.jdbc_table_df.option("query", jdbc_table_name)

    # Chargement des données
    self.jdbc_table_df = self.jdbc_table_df.load()

    # si partition créé spécialement pour parraléliser l'import, on l'enlève
    if "partition" in self.jdbc_table_df.columns:
        self.jdbc_table_df = self.jdbc_table_df.drop("partition")

    df_take = self.jdbc_table_df.isEmpty()

    if df_take:
        self.jdbc_table_df_empty = True
        self.settings.logger.info(f"La récupération des données de la table {self.table.jdbc_schema}.{self.table.jdbc_table} en {self.table.jdbc_ingest_type} ({self.table.reference_column_max_value}) est vide")
    else:
        self.jdbc_table_df_empty = False

def generate_reference_column_max_value_dataframe(self):
    """
    Génère un DataFrame contenant la valeur maximale de la colonne de référence.

    Cette méthode vérifie le type d'ingestion JDBC de la table. Si le type est 'delta' ou 'delta_query',
    elle sélectionne la valeur maximale de la colonne de référence spécifiée dans le DataFrame JDBC et
    crée un DataFrame avec cette valeur. Sinon, elle crée un DataFrame vide avec le schéma approprié.

    Attributs:
        self : Référence à l'instance de la classe contenant cette méthode.

    Modifie:
        self.reference_column_max_value_df : pyspark.sql.DataFrame
        DataFrame contenant la valeur maximale de la colonne de référence ou un DataFrame vide.
    """
    if self.table.jdbc_ingest_type in ['delta', 'delta_query']:
        self.table.jdbc_reference_column_max_value = self.jdbc_table_df.select(max(col(self.table.jdbc_reference_column))).first()[0]
        data_ref_col = [(self.table.jdbc_schema, self.table.jdbc_table, self.table.jdbc_reference_column, self.table.jdbc_reference_column_max_value)]
        self.reference_column_max_value_df = self.settings.spark.createDataFrame(data=data_ref_col, schema=SCHEMA_LAST_VALUE)
    else:
        self.reference_column_max_value_df = self.settings.spark.createDataFrame(data=[], schema=SCHEMA_LAST_VALUE)

def write_mode(self, df, hive_write_mode, table, partition=None):
    """
    Écrit un DataFrame Spark dans une table Hive avec le mode spécifié.

    Args:
        df (DataFrame): Le DataFrame Spark à écrire.
        hive_write_mode (str): Le mode d'écriture Hive. Peut être "overwrite", "append" ou "overwrite_par_partition".
        table (str): Le nom de la table Hive.
        partition (str, optional): La partition de la table Hive. Par défaut, None.

    Returns:
        DataFrame: Le DataFrame JDBC après l'écriture.

    Raises:
        Exception: Si une erreur survient lors de l'écriture dans la table Hive.
    """
    exceptions = []
    start_time = time.time()
    df = df.withColumn('xx_date_insertion', lit(datetime.now().strftime('%Y-%m-%d %H:%M:%S')).cast(TimestampType()))
    
    try:
        table_name = f"{self.table.hive_database}.{table}"
        table_path = f"{self.settings.db_hive_brute_path}/{table}"
        self.settings.logger.info(f"Ecriture de {self.table.jdbc_schema}.{self.table.jdbc_table} sur la table {table_name} en {hive_write_mode} || path >{table_path} || partition > {partition} || format {self.table.hive_table_format}")
        
        if hive_write_mode in ["overwrite", "append"]:
            df.write.mode(hive_write_mode) \
                .option("TRANSLATED_TO_EXTERNAL", "TRUE") \
                .saveAsTable(table_name, path=table_path,
                            partitionBy=partition, format=self.table.hive_table_format, compression='snappy')
        elif hive_write_mode == "overwrite_par_partition":
            # Sauvegarde de la configuration actuelle puis passage en mode partitions dynamiques
            old_conf = self.settings.spark.conf.get("spark.sql.sources.partitionOverwriteMode")
            self.settings.spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
            self.settings.spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
            df.write.insertInto(f"{self.table.hive_database}.{self.table.hive_table}", overwrite=True)
            # Remise de la conf à l'état précédent
            self.settings.spark.conf.set("spark.sql.sources.partitionOverwriteMode", old_conf)
        
        self.settings.logger.info(f"Fin de l'écriture dans la table {self.table.hive_database}.{self.table.hive_table}. Temps total : {str(time.time() - start_time)}s")
        self.settings.spark.sql(f"ANALYZE TABLE {table_name} COMPUTE STATISTICS")
        self.settings.logger.info(f"fin du compute statistics sur {table_name} ")
        
        return self.jdbc_table_df
    
    except Exception as ex:
        self.settings.logger.error(f"erreur dans lecriture de la table '{table_name}' : {str(ex)}")
        exceptions.append(ex)
        raise


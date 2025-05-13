"""
Module pour la dimension DIM_LOCALISATION du modèle en étoile qualification_ols.

Ce module contient les fonctions nécessaires pour créer et gérer
la dimension géographique du modèle en étoile.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, concat_ws
from .qualification_ols_config import FIELDS, TABLES

def create_dim_localisation(df: DataFrame) -> DataFrame:
    """
    Crée la dimension DIM_LOCALISATION (Dimension géographique).
    
    Args:
        df: DataFrame source contenant les données qualification_ols
        
    Returns:
        DataFrame contenant la dimension DIM_LOCALISATION
    """
    return df.select(
        concat_ws("_", col("annee"), col("mois"), col("siren")).alias("annee_mois_SIREN"),
        concat_ws("_", col("annee"), col("mois")).alias("annee_mois"),
        col("code_postal").alias("adresse_postal"),
        col("code_departement").alias("code_Departement"),
        col("ville").alias("Ville"),
        col("nbre_habitant").cast("integer").alias("nbre_habitant"),
        col("region").alias("Region")
    ).distinct()

def save_dim_localisation(df: DataFrame, spark: SparkSession, db_lac: str) -> None:
    """
    Sauvegarde la dimension DIM_LOCALISATION dans le lac de données.
    
    Args:
        df: DataFrame contenant la dimension DIM_LOCALISATION
        spark: Session Spark active
        db_lac: Nom de la base de données du lac
    """
    df.write \
        .mode("overwrite") \
        .format("parquet") \
        .saveAsTable(f"{db_lac}.{TABLES['DIM_LOCALISATION']}")

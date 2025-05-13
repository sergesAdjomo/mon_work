"""
Module pour la dimension DIM_TEMPS du modèle en étoile qualification_ols.

Ce module contient les fonctions nécessaires pour créer et gérer
la dimension temporelle du modèle en étoile.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, concat_ws
from .qualification_ols_config import FIELDS, TABLES

def create_dim_temps(df: DataFrame) -> DataFrame:
    """
    Crée la dimension DIM_TEMPS (Dimension temporelle).
    
    Args:
        df: DataFrame source contenant les données qualification_ols
        
    Returns:
        DataFrame contenant la dimension DIM_TEMPS
    """
    return df.select(
        concat_ws("_", col("annee"), col("mois")).alias("annee_mois"),
        col("annee"),
        col("mois")
    ).distinct()

def save_dim_temps(df: DataFrame, spark: SparkSession, db_lac: str) -> None:
    """
    Sauvegarde la dimension DIM_TEMPS dans le lac de données.
    
    Args:
        df: DataFrame contenant la dimension DIM_TEMPS
        spark: Session Spark active
        db_lac: Nom de la base de données du lac
    """
    df.write \
        .mode("overwrite") \
        .format("parquet") \
        .saveAsTable(f"{db_lac}.{TABLES['DIM_TEMPS']}")

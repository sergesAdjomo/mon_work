"""
Module pour la table de faits FT_qualif_donnees_usage du modèle en étoile qualification_ols.

Ce module contient les fonctions nécessaires pour créer et gérer
la table de faits du modèle en étoile.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, concat_ws
from .qualification_ols_config import FIELDS, TABLES

def create_ft_qualif_donnees_usage(df: DataFrame) -> DataFrame:
    """
    Crée la table de faits FT_qualif_donnees_usage.
    
    Args:
        df: DataFrame source contenant les données qualification_ols
        
    Returns:
        DataFrame contenant la table de faits
    """
    return df.select(
        concat_ws("_", col("annee"), col("mois"), col("siren")).alias("annee_mois_SIREN"),
        concat_ws("_", col("annee"), col("mois")).alias("annee_mois"),
        col("siren").alias("SIREN"),
        col("nb_de").cast("integer").alias("nb_de"),
        col("chiffre_d_affaire_moyen").cast("integer").alias("chiffre_d_affaire_moyen"),
        col("montant_signe").cast("integer").alias("montant_signe")
    )

def save_ft_qualif_donnees_usage(df: DataFrame, spark: SparkSession, db_lac: str) -> None:
    """
    Sauvegarde la table de faits FT_qualif_donnees_usage dans le lac de données.
    
    Args:
        df: DataFrame contenant la table de faits
        spark: Session Spark active
        db_lac: Nom de la base de données du lac
    """
    df.write \
        .mode("overwrite") \
        .format("parquet") \
        .saveAsTable(f"{db_lac}.{TABLES['FT_QUALIF_DONNEES_USAGE']}")

"""
Module pour la dimension DIM_PM_BDT du modèle en étoile qualification_ols.

Ce module contient les fonctions nécessaires pour créer et gérer
la dimension principale business du modèle en étoile.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, concat_ws, lit, when
from .qualification_ols_config import FIELDS, TABLES

def create_dim_pm_bdt(df: DataFrame) -> DataFrame:
    """
    Crée la dimension DIM_PM_BDT (Dimension principale business).
    
    Args:
        df: DataFrame source contenant les données qualification_ols
        
    Returns:
        DataFrame contenant la dimension DIM_PM_BDT
    """
    return df.select(
        concat_ws("_", col("annee"), col("mois"), col("siren")).alias("annee_mois_SIREN"),
        col("code_tiers").alias("code_tiers"),
        col("code_dr").alias("code_dr"),
        col("libelle_dr").alias("libelle_dr"),
        col("denomination_unite_legale").alias("denomination_unite_legale"),
        col("secteur_activite").alias("secteur_activite"),
        col("activite_principale").alias("activite_principale"),
        col("segment_nrc").alias("segmentation_NRC"),
        col("domaine_de_competences").alias("domaine_de_competences"),
        col("competences_exercee").alias("competences_exercee"),
        col("zone_montagne").alias("zone_montagne"),
        col("type_montagne").alias("type_montagne"),
        col("zone_littoral").alias("zone_littoral"),
        col("type_littoral").alias("type_littoral"),
        col("niveau_d_attractivite").alias("niveau_d_attractivite"),
        col("date_de_creation").alias("date_de_creation"),
        col("date_de_fermeture").alias("date_de_fermeture"),
        col("categorie_n1").alias("categorie_N1"),
        col("categorie_n2").alias("categorie_N2"),
        col("categorie_n3").alias("categorie_N3"),
        col("categorie_n4").alias("categorie_N4"),
        col("tete_de_groupe").cast("boolean").alias("tete_de_groupe"),
        col("part_collective").alias("part_collective"),
        col("acv").cast("boolean").alias("ACV"),
        col("pvd").cast("boolean").alias("PVD"),
        col("opv").cast("boolean").alias("OPV"),
        col("effectif").cast("integer").alias("effectif"),
        col("annee").alias("annee"),
        col("annee_mois").alias("annee-mois")
    ).distinct()

def save_dim_pm_bdt(df: DataFrame, spark: SparkSession, db_lac: str) -> None:
    """
    Sauvegarde la dimension DIM_PM_BDT dans le lac de données.
    
    Args:
        df: DataFrame contenant la dimension DIM_PM_BDT
        spark: Session Spark active
        db_lac: Nom de la base de données du lac
    """
    df.write \
        .mode("overwrite") \
        .format("parquet") \
        .saveAsTable(f"{db_lac}.{TABLES['DIM_PM_BDT']}")

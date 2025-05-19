# qualification_ols_star_model/transformations/dim_localisation.py
from pyspark.sql.functions import col, substring, coalesce, lit, when, desc
from pyspark.sql import Window
import pyspark.sql.functions as F

from qualification_ols_star_model.constants.fields import FIELDS


def prepare_dim_localisation(dim_pm_bdt, bv_coord_postales, bv_departement, bv_region):
    """
    Prépare la dimension LOCALISATION selon les spécifications du modèle en étoile.
    
    Args:
        dim_pm_bdt: DataFrame de la dimension PM_BDT
        bv_coord_postales: DataFrame des coordonnées postales
        bv_departement: DataFrame des départements
        bv_region: DataFrame des régions
        
    Returns:
        DataFrame: Dimension DIM_LOCALISATION conforme au modèle
    """
    # 1. Préparation des coordonnées postales - prendre la plus récente par code_tiers
    window_spec = Window.partitionBy(FIELDS.get("code_tiers")).orderBy(desc(FIELDS.get("dat_horodat")))
    
    coord_postales_latest = bv_coord_postales.withColumn(
        "row_num", 
        F.row_number().over(window_spec)
    ).filter(col("row_num") == 1).drop("row_num")
    
    # 2. Extraction du code département à partir du code postal
    coord_postales_with_dept = coord_postales_latest.withColumn(
        "code_dept", 
        substring(col(FIELDS.get("adr_code_postal")), 1, 2)
    )
    
    # 3. Jointure avec le département
    loc_with_dept = coord_postales_with_dept.join(
        bv_departement,
        col("code_dept") == col(FIELDS.get("code_departement")),
        "left"
    )
    
    # 4. Jointure avec la région
    loc_with_region = loc_with_dept.join(
        bv_region,
        on=FIELDS.get("code_region"),
        how="left"
    )
    
    # 5. Sélection des colonnes pour la dimension finale selon l'image
    localisation_df = loc_with_region.select(
        FIELDS.get("code_tiers"),
        FIELDS.get("adr_code_postal"),
        col(FIELDS.get("code_departement")).alias("code_Departement"),
        col(FIELDS.get("lib_bureau_distrib")).alias("Ville"),
        col(FIELDS.get("lib_clair_region")).alias("Region")
    )
    
    # 6. Jointure avec dim_pm_bdt pour obtenir les clés du modèle en étoile
    dim_localisation = dim_pm_bdt.select(
        "annee_mois_SIREN",
        "annee_mois",
        "SIREN",
        FIELDS.get("code_tiers")
    ).join(
        localisation_df,
        on=FIELDS.get("code_tiers"),
        how="left"
    )
    
    # 7. Sélection finale des colonnes pour la dimension selon l'image
    dim_localisation = dim_localisation.select(
        col("annee_mois_SIREN"),
        col("annee_mois"),
        col(FIELDS.get("adr_code_postal")),
        col("code_Departement"),
        col("Ville"),
        col("Region")
    )
    
    return dim_localisation
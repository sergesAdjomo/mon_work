# qualification_ols_star_model/transformations/dim_localisation.py
from pyspark.sql.functions import col, regexp_replace, when, lit, coalesce

from qualification_ols_star_model.constants.fields import FIELDS


def prepare_dim_localisation(dim_pm_bdt, bv_coord_postales, bv_departement, bv_region):
    """
    Prépare la dimension LOCALISATION avec des jointures améliorées
    
    Args:
        dim_pm_bdt: DataFrame de la dimension PM_BDT
        bv_coord_postales: DataFrame des coordonnées postales
        bv_departement: DataFrame des départements
        bv_region: DataFrame des régions
        
    Returns:
        DataFrame: Dimension DIM_LOCALISATION enrichie
    """
    # Amélioration 1: Extraire le code département du code postal de manière plus robuste
    # Prendre en compte les cas spéciaux comme la Corse (2A, 2B) et les DOM-TOM
    coordonnees_postales_enrichies = bv_coord_postales.withColumn(
        "code_dept_extrait",
        when(
            regexp_replace(col(FIELDS.get("adr_code_postal")), "^(\\d{2}).*", "$1") == "20",
            when(
                col(FIELDS.get("adr_code_postal")) < "20200",
                lit("2A")
            ).otherwise(lit("2B"))
        ).otherwise(regexp_replace(col(FIELDS.get("adr_code_postal")), "^(\\d{2}).*", "$1"))
    )
    
    # Tri des coordonnées postales par date pour prendre la plus récente en cas de doublons
    coordonnees_postales_les_plus_recentes = coordonnees_postales_enrichies\
        .orderBy(col("dat_horodat").desc())\
        .dropDuplicates([FIELDS.get("code_tiers")])
    
    # Joindre coordonnées postales avec département et région
    localisation_base = (
        coordonnees_postales_les_plus_recentes
        .join(
            bv_departement,
            on=col("code_dept_extrait") == col(FIELDS.get("code_departement")),
            how="left"
        )
        .join(
            bv_region,
            on=["code_region"],
            how="left"
        )
    )

    # Sélectionner les colonnes pertinentes avec des transformations pour standardiser les données
    localisation_base = localisation_base.select(
        FIELDS.get("code_tiers"),
        FIELDS.get("adr_code_postal"),
        FIELDS.get("code_departement"),
        # Standardisation et nettoyage des valeurs
        coalesce(col("libelle_bureau_distribution"), lit("Non défini")).alias(FIELDS.get("ville")),
        # S'assurer que le nombre d'habitants est un entier
        coalesce(col("nbre_habitant").cast("integer"), lit(0)).alias(FIELDS.get("nbre_habitant")),
        coalesce(col("libelleclair_region"), lit("Non défini")).alias(FIELDS.get("region"))
    )
    
    # Ajouter d'autres attributs géographiques potentiellement utiles
    localisation_base = localisation_base.withColumn(
        "zone_geo", 
        when(col(FIELDS.get("code_departement")).isin("75", "92", "93", "94"), lit("Ile de France"))
        .when(col(FIELDS.get("code_departement")).isin("13", "06", "83"), lit("Sud-Est"))
        .when(col(FIELDS.get("code_departement")).isin("33", "31", "64"), lit("Sud-Ouest"))
        .when(col(FIELDS.get("code_departement")).isin("59", "62"), lit("Nord"))
        .otherwise(lit("Autres"))
    )

    # Joindre avec PM_BDT pour obtenir le SIREN et créer la clé primaire annee_mois_SIREN
    dim_localisation = (
        dim_pm_bdt
        .select(
            FIELDS.get("annee_mois_siren"),  # Clé primaire pour la jointure avec la table de faits
            FIELDS.get("annee_mois"),        # Clé étrangère pour DIM_TEMPS
            FIELDS.get("siren"),             # SIREN pour identification de l'entreprise
            FIELDS.get("code_tiers")         # Code Tiers pour jointure avec localisation
        )
        .join(
            localisation_base,
            on=[FIELDS.get("code_tiers")],
            how="left"
        )
    )
    
    # Enrichissement avec des métriques géographiques supplémentaires
    dim_localisation = dim_localisation.withColumn(
        "densite_population", 
        when(col(FIELDS.get("nbre_habitant")) > 0, lit(1)).otherwise(lit(0))
    )

    return dim_localisation
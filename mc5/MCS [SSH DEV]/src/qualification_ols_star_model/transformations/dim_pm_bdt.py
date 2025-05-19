# qualification_ols_star_model/transformations/dim_pm_bdt.py
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, year, month, to_date, lit, concat

from qualification_ols_star_model.constants.fields import FIELDS


def prepare_dim_pm_bdt(spark, bv_pm, bv_tiers, date_ctrlm):
    """
    Prépare la dimension PM_BDT avec les champs exacts selon le modèle en étoile.
    
    Args:
        spark: Session Spark
        bv_pm: DataFrame des personnes morales
        bv_tiers: DataFrame des tiers
        date_ctrlm: Date de contrôle
        
    Returns:
        DataFrame: Dimension DIM_PM_BDT conforme aux spécifications
    """
    # Filtrer les personnes morales OLS actives et siège
    pm_base = bv_pm.filter(
        (col(FIELDS.get("is_ols")) == "1") & 
        (col(FIELDS.get("etat_admin")) == "A") & 
        (col(FIELDS.get("etab_siege")) == "1")
    ).select(
        # Sélectionner exactement les colonnes requises selon l'image 1
        col(FIELDS.get("siren")).alias("SIREN"),
        col(FIELDS.get("denom_unite_legale")).alias("Raison_sociale"),
        col(FIELDS.get("sous_cat")).alias("Sous_categorie"),
        col(FIELDS.get("is_tete_groupe")).alias("Tete_de_groupe")
    )
    
    # Extraire l'année du CTRLM
    date_obj = to_date(lit(date_ctrlm), "yyyyMMdd")
    year_val = year(date_obj)
    
    # Ajouter l'année comme attribut
    pm_base = pm_base.withColumn("annee", year_val)
    
    # Ajouter annee_mois pour la jointure
    pm_base = pm_base.withColumn(
        "annee_mois",
        concat(
            year(date_obj).cast("string"),
            lit("_"),
            month(date_obj).cast("string")
        )
    )
    
    # Ajouter la clé pour les jointures
    pm_base = pm_base.withColumn(
        "annee_mois_SIREN",
        concat(col("annee_mois"), lit("_"), col("SIREN"))
    )
    
    # Récupérer les code_tiers depuis bv_tiers pour les jointures avec localisation
    tiers_filtres = bv_tiers.select(
        FIELDS.get("siren"),
        FIELDS.get("code_tiers")
    )
    
    # Joindre pour avoir le code_tiers disponible
    dim_pm_bdt = pm_base.join(
        tiers_filtres,
        pm_base.SIREN == tiers_filtres[FIELDS.get("siren")],
        "left"
    ).drop(FIELDS.get("siren"))
    
    return dim_pm_bdt
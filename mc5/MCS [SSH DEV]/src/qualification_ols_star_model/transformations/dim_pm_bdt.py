# qualification_ols_star_model/transformations/dim_pm_bdt.py
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, concat, lit, year, month, to_date, current_timestamp

from qualification_ols_star_model.constants.fields import FIELDS


def prepare_dim_pm_bdt(spark, bv_pm, bv_tiers, date_ctrlm):
    """
    Prépare la dimension PM_BDT à partir des tables source
    
    Args:
        spark: Session Spark
        bv_pm: DataFrame des personnes morales
        bv_tiers: DataFrame des tiers
        date_ctrlm: Date de contrôle
        
    Returns:
        DataFrame: Dimension DIM_PM_BDT
    """
    # Filtrer les personnes morales OLS
    pm_base = bv_pm.select(
        col(FIELDS.get("siren")),
        col(FIELDS.get("denom_unite_legale")),
        col("is_tete_de_groupe"),  # Accès direct à la colonne is_tete_de_groupe dans la source
        # Ajouter les champs du domaine métier avec des valeurs NULL pour les colonnes manquantes
        lit(None).alias(FIELDS.get("code_dr") or "code_dr"),
        lit(None).alias(FIELDS.get("libelle_dr") or "libelle_dr"),
        col(FIELDS.get("denom_unite_legale")).alias(FIELDS.get("denomination_unite_legale") or "denomination_unite_legale"),
        col("secteur_activite"),  # Cette colonne existe
        col("activite_principale"),  # Cette colonne existe
        lit(None).alias(FIELDS.get("sous_activite_ht") or "sous_activite_ht"),
        # Domaine de compétences
        lit(None).alias(FIELDS.get("domaine_de_competences") or "domaine_de_competences"),
        lit(None).alias(FIELDS.get("competences_exercee") or "competences_exercee"),
        # Zones géographiques spécifiques
        lit(None).alias(FIELDS.get("zone_montagne") or "zone_montagne"),
        lit(None).alias(FIELDS.get("type_montagne") or "type_montagne"),
        lit(None).alias(FIELDS.get("zone_littoral") or "zone_littoral"),
        lit(None).alias(FIELDS.get("type_littoral") or "type_littoral"),
        # Niveau d'activité et dates
        lit(None).alias(FIELDS.get("niveau_d_attractivite") or "niveau_d_attractivite"),
        current_timestamp().alias("date_de_creation"),
        current_timestamp().alias("date_de_fermeture"),
        # Catégories
        lit(None).alias(FIELDS.get("categorie_N1") or "categorie_N1"),
        lit(None).alias(FIELDS.get("categorie_N2") or "categorie_N2"),
        lit(None).alias(FIELDS.get("categorie_N3") or "categorie_N3"),
        lit(None).alias(FIELDS.get("categorie_N4") or "categorie_N4"),
        # Booléens
        col("is_tete_de_groupe").cast("boolean").alias(FIELDS.get("is_tete_groupe")),
        lit(None).cast("double").alias(FIELDS.get("part_collective") or "part_collective"),
        lit(False).alias(FIELDS.get("ACU") or "ACU"),
        lit(False).alias(FIELDS.get("PVD") or "PVD"),
        lit(False).alias(FIELDS.get("OPV") or "OPV"),
        lit(None).cast("integer").alias(FIELDS.get("effectif") or "effectif")
    ).filter(col("is_ols") == "1")

    # Récupérer les informations des tiers
    tiers_info = bv_tiers.select(
        FIELDS.get("siren"),
        FIELDS.get("code_tiers")
    )

    # Joindre les tables
    dim_pm_bdt = pm_base.join(
        tiers_info, 
        on=[pm_base.siren == tiers_info.siren], 
        how="left"
    ).drop(tiers_info.siren)

    # Ajouter le champ annee_mois pour la clé
    dim_pm_bdt = dim_pm_bdt.withColumn(
        FIELDS.get("annee_mois"), 
        concat(
            year(to_date(lit(date_ctrlm), "yyyyMMdd")), 
            lit("-"), 
            month(to_date(lit(date_ctrlm), "yyyyMMdd"))
        )
    )

    # Créer la clé primaire
    dim_pm_bdt = dim_pm_bdt.withColumn(
        FIELDS.get("annee_mois_siren"), 
        concat(
            col(FIELDS.get("annee_mois")), 
            lit("_"), 
            col(FIELDS.get("siren"))
        )
    )

    return dim_pm_bdt
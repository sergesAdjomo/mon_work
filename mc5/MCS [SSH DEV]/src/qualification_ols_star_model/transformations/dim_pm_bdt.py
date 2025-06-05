# qualification_ols_star_model/transformations/dim_pm_bdt.py
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, year, month, to_date, lit, concat, coalesce, format_string

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
    # Examinez les colonnes disponibles dans le DataFrame
    pm_base = bv_pm.filter(
        (col("is_ols") == 1) & 
        (col("etat_administratif") == "A") & 
        (col("etablissement_siege") == 1)
    ).select(
        # Sélectionner et mapper les colonnes depuis bv_personne_morale_bdt
        col("siren").alias("SIREN"),
        col("denomination_unite_legale"),
        col("sous_categorie"),
        # Ne pas sélectionner directement secteur_activite, utiliser lit() à la place
        lit("").alias("secteur_activité"),  # Accent ajouté mais valeur vide
        lit("").alias("activite_principale"),  # Valeur vide si non disponible
        coalesce(col("is_tete_de_groupe"), lit(0)).cast("boolean").alias("tete_de_groupe"),
        
        # Colonnes additionnelles nécessaires pour le schéma
        lit("").alias("code_dr"),
        lit("").alias("libelle_dr"),
        lit("").alias("segmentation MRC"),  # Nom exact du schéma
        lit("").alias("domaine de compétences"),  # Espaces et accent
        lit("").alias("compétences exercée"),  # Espaces et accent
        lit("").alias("zone montagne"),  # Espace
        lit("").alias("type montagne"),  # Espace
        lit("").alias("zone littoral"),  # Espace
        lit("").alias("type littoral"),  # Espace
        lit("").alias("niveau d'attractivité"),  # Format exact avec apostrophe et accent
        lit("").alias("date de création"),  # Espace et accent
        lit("").alias("date de fermeture"),  # Espace
        lit("").alias("categorie_N1"),
        lit("").alias("catégorie_N2"),  # Accent ajouté
        lit("").alias("catégorie_N3"),  # Accent ajouté
        lit("").alias("catégorie_N4"),  # Accent ajouté
        format_string("%.0f%%", lit(0)).alias("part_collectivite"),  # Format avec %
        lit(False).cast("boolean").alias("ACV"),
        lit(False).cast("boolean").alias("PVD"),
        lit(False).cast("boolean").alias("QPV"),  # Changé de CPV à QPV
        lit(0).cast("int").alias("effectif")
    )
    
    # Extraire l'année du CTRLM
    date_obj = to_date(lit(date_ctrlm), "yyyyMMdd")
    year_val = year(date_obj).cast("string")  # Cast to string as per schema
    
    # Ajouter l'année comme attribut
    pm_base = pm_base.withColumn("annee", year_val)
    
    # Ajouter annee_mois pour la jointure
    pm_base = pm_base.withColumn(
        "annee_mois",
        concat(
            year(date_obj).cast("string"),
            lit("-"),  # Utiliser un tiret comme dans le schéma anne-mois
            month(date_obj).cast("string")
        )
    )
    
    # Ajouter la clé primaire pour les jointures
    pm_base = pm_base.withColumn(
        "annee_mois_SIREN",
        concat(col("annee_mois"), lit("_"), col("SIREN"))
    )
    
    # Récupérer les code_tiers depuis bv_tiers pour les jointures et compléter les données de création/fermeture
    # Sélectionner seulement les colonnes pertinentes de bv_tiers_bdt
    
    # Pour débogage, loguer les colonnes disponibles dans bv_tiers
    bv_tiers_columns = bv_tiers.columns
    print("Colonnes disponibles dans bv_tiers:", bv_tiers_columns)
    
    # Maintenant que nous avons les colonnes de date disponibles, utilisons-les
    tiers_data = bv_tiers.select(
        col("siren"),
        col("code_tiers"),
        # Utiliser les colonnes de date de bv_tiers_bdt et les convertir en string
        coalesce(col("date_creation_tiers").cast("string"), lit(None)).alias("date_creation"),
        coalesce(col("date_fermeture_tiers").cast("string"), lit(None)).alias("date_fermeture")
    )
    
    # Joindre pour avoir les informations du tiers disponibles
    dim_pm_bdt = pm_base.join(
        tiers_data,
        pm_base.SIREN == tiers_data.siren,
        "left"
    ).drop("siren")
    
    # Mettre à jour les dates de création et fermeture depuis bv_tiers_bdt
    dim_pm_bdt = dim_pm_bdt.withColumn(
        "date de création", 
        coalesce(col("date_creation"), lit(""))
    ).withColumn(
        "date de fermeture", 
        coalesce(col("date_fermeture"), lit(""))
    ).drop("date_creation", "date_fermeture")
    
    # Définir l'ordre exact des colonnes selon le schéma spécifié
    final_columns = [
        "annee_mois_SIREN",        # PK
        "code_tiers",              # FK1
        "code_dr",
        "libelle_dr",
        "denomination_unite_legale",
        "secteur_activité",
        "activite_principale",
        "segmentation MRC",
        "domaine de compétences",
        "compétences exercée",
        "zone montagne",
        "type montagne",
        "zone littoral",
        "type littoral",
        "niveau d'attractivité",
        "date de création",
        "date de fermeture",
        "categorie_N1",
        "catégorie_N2",
        "catégorie_N3",
        "catégorie_N4",
        "tete_de_groupe",
        "part_collectivite",
        "ACV",
        "PVD",
        "QPV",
        "effectif",
        "annee",
        "annee_mois"              # "anne-mois" dans le schéma mais gardé comme "annee_mois" par cohérence
    ]
    
    # Assurons-nous que tous les champs ont des types appropriés pour éviter les erreurs de conversion
    # Créons un dataframe temporaire où on force les conversions de type
    for col_name in dim_pm_bdt.columns:
        # Les champs booléens doivent être correctement castés
        if col_name in ["tete_de_groupe", "ACV", "PVD", "QPV"]:
            dim_pm_bdt = dim_pm_bdt.withColumn(col_name, col(col_name).cast("boolean"))
        
        # L'effectif doit être un entier
        elif col_name == "effectif":
            dim_pm_bdt = dim_pm_bdt.withColumn(col_name, col(col_name).cast("integer"))
        
        # Les pourcentages et autres champs numériques doivent être des doubles ou des string
        elif col_name == "part_collectivite":
            dim_pm_bdt = dim_pm_bdt.withColumn(col_name, col(col_name).cast("string"))
            
        # Assurons-nous que toutes les chaînes de caractères sont bien des chaînes
        # pour éviter des erreurs de conversion
        else:
            dim_pm_bdt = dim_pm_bdt.withColumn(col_name, col(col_name).cast("string"))
    
    # Sélectionner les colonnes dans l'ordre exact selon le schéma
    dim_pm_bdt = dim_pm_bdt.select(*final_columns)
    
    return dim_pm_bdt
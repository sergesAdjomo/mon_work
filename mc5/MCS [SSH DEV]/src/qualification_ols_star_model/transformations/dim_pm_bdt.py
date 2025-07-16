# qualification_ols_star_model/transformations/dim_pm_bdt.py
from pyspark.sql.functions import col, year, month, to_date, lit, concat, coalesce, when
from pyspark.sql.types import IntegerType, BooleanType, StringType
from qualification_ols_star_model.constants.fields import FIELDS


def prepare_dim_pm_bdt(spark, bv_pm, bv_tiers, date_ctrlm):
    pm_base = bv_pm.filter(
        (col(FIELDS.get("is_ols")) == "1") & 
        (col(FIELDS.get("etat_admin")) == "A") & 
        (col(FIELDS.get("etab_siege")) == "1")
    ).select(
        col(FIELDS.get("siren")).alias("SIREN"),
        col(FIELDS.get("denom_unite_legale")).alias("Raison_sociale"),
        col(FIELDS.get("sous_cat")).alias("Sous_categorie"),
        when(coalesce(col(FIELDS.get("is_tete_groupe")), lit("0")) == "1", True).otherwise(False).cast(BooleanType()).alias("Tete_de_groupe"),
        lit("").alias("secteur_activité"),
        lit("").alias("activite_principale"),
        lit("").alias("code_dr"),
        lit("").alias("libelle_dr"),
        col(FIELDS.get("denom_unite_legale")).alias("denomination_unite_legale"),
        lit("").alias("segmentation MRC"),
        lit("").alias("domaine de compétences"),
        lit("").alias("compétences exercée"),
        lit("").alias("zone montagne"),
        lit("").alias("type montagne"),
        lit("").alias("zone littoral"),
        lit("").alias("type littoral"),
        lit("").alias("niveau d'attractivité"),
        lit("").alias("date de création"),
        lit("").alias("date de fermeture"),
        lit("").alias("categorie_N1"),
        lit("").alias("catégorie_N2"),
        lit("").alias("catégorie_N3"),
        lit("").alias("catégorie_N4"),
        lit("0").alias("part_collectivite"),
        lit(False).cast(BooleanType()).alias("ACV"),
        lit(False).cast(BooleanType()).alias("PVD"),
        lit(False).cast(BooleanType()).alias("QPV"),
        lit(0).cast(IntegerType()).alias("effectif")
    )
    
    date_obj = to_date(lit(date_ctrlm), "yyyyMMdd")
    year_val = year(date_obj)
    
    pm_base = pm_base.withColumn("annee", year_val.cast(StringType()))
    pm_base = pm_base.withColumn(
        "annee_mois",
        concat(year(date_obj).cast("string"), lit("_"), month(date_obj).cast("string"))
    )
    pm_base = pm_base.withColumn(
        "annee_mois_SIREN",
        concat(col("annee_mois"), lit("_"), col("SIREN"))
    )
    
    tiers_filtres = bv_tiers.select(FIELDS.get("siren"), FIELDS.get("code_tiers"))
    
    dim_pm_bdt = pm_base.join(
        tiers_filtres,
        pm_base.SIREN == tiers_filtres[FIELDS.get("siren")],
        "left"
    ).drop(FIELDS.get("siren"))
    
    dim_pm_bdt = dim_pm_bdt.withColumn("Tete_de_groupe", col("Tete_de_groupe").cast(BooleanType()))
    dim_pm_bdt = dim_pm_bdt.withColumn("ACV", col("ACV").cast(BooleanType()))
    dim_pm_bdt = dim_pm_bdt.withColumn("PVD", col("PVD").cast(BooleanType()))
    dim_pm_bdt = dim_pm_bdt.withColumn("QPV", col("QPV").cast(BooleanType()))
    dim_pm_bdt = dim_pm_bdt.withColumn("effectif", 
                                     when(col("effectif").isNull(), 0)
                                     .otherwise(col("effectif").cast("double").cast(IntegerType())))
    
    for string_column in [c for c in dim_pm_bdt.columns if c not in ["Tete_de_groupe", "ACV", "PVD", "CPV", "effectif"]]:
        dim_pm_bdt = dim_pm_bdt.withColumn(string_column, col(string_column).cast(StringType()))
    
    return dim_pm_bdt
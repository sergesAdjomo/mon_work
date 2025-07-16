# qualification_ols_star_model/transformations/dim_localisation.py
from pyspark.sql.functions import col, substring, lit, desc
from pyspark.sql import Window
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, IntegerType
from qualification_ols_star_model.constants.fields import FIELDS


def prepare_dim_localisation(dim_pm_bdt, bv_coord_postales, bv_departement, bv_region):
    window_spec = Window.partitionBy(FIELDS.get("code_tiers")).orderBy(desc(FIELDS.get("dat_horodat")))
    
    coord_postales_latest = bv_coord_postales.withColumn(
        "row_num", 
        F.row_number().over(window_spec)
    ).filter(col("row_num") == 1).drop("row_num")
    
    coord_postales_with_dept = coord_postales_latest.withColumn(
        "code_dept", 
        substring(col(FIELDS.get("adr_code_postal")), 1, 2)
    )
    
    loc_with_dept = coord_postales_with_dept.join(
        bv_departement,
        col("code_dept") == col(FIELDS.get("code_departement")),
        "left"
    )
    
    loc_with_region = loc_with_dept.join(
        bv_region,
        on=FIELDS.get("code_region"),
        how="left"
    )
    
    localisation_df = loc_with_region.select(
        FIELDS.get("code_tiers"),
        FIELDS.get("adr_code_postal"),
        col(FIELDS.get("code_departement")).alias("code_Departement"),
        col(FIELDS.get("lib_bureau_distrib")).alias("Ville"),
        col(FIELDS.get("lib_clair_region")).alias("Region")
    )
    
    dim_localisation = dim_pm_bdt.select(
        "annee_mois_SIREN",
        "annee_mois",
        FIELDS.get("code_tiers")
    ).join(
        localisation_df,
        on=FIELDS.get("code_tiers"),
        how="left"
    )
    
    dim_localisation = dim_localisation.select(
        col("annee_mois_SIREN").cast(StringType()),
        col("annee_mois").cast(StringType()),
        col(FIELDS.get("adr_code_postal")).cast(StringType()).alias("adr_code_postal"),
        col("code_Departement").cast(IntegerType()),
        col("Ville").cast(StringType()),
        lit(0).cast(IntegerType()).alias("nbre_habitant"),
        col("Region").cast(StringType())
    )
    
    return dim_localisation
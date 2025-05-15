# qualification_ols_star_model/transformations/dim_temps.py
from pyspark.sql.functions import concat, lit, year, month, quarter, dayofmonth, to_date, expr

from qualification_ols_star_model.constants.fields import FIELDS


def prepare_dim_temps(spark, date_ctrlm):
    """
    Prépare la dimension TEMPS avec une granularité améliorée
    
    Args:
        spark: Session Spark
        date_ctrlm: Date de contrôle
        
    Returns:
        DataFrame: Dimension DIM_TEMPS enrichie
    """
    # Extraire année et mois à partir de la date de contrôle
    date_controle = to_date(lit(date_ctrlm), "yyyyMMdd")
    
    # Créer la dimension TEMPS avec une granularité enrichie
    dim_temps = spark.createDataFrame(
        [(
            # Clé principale pour le modèle en étoile
            concat(
                year(date_controle).cast("string"), 
                lit("-"), 
                month(date_controle).cast("string")
            ),
            # Attributs temporels enrichis
            year(date_controle).cast("string"),
            month(date_controle).cast("string"),
            quarter(date_controle).cast("string"),
            dayofmonth(date_controle).cast("string"),
            expr(f"case when month(to_date('{date_ctrlm}', 'yyyyMMdd')) in (1,2,3) then '1' "
                 f"when month(to_date('{date_ctrlm}', 'yyyyMMdd')) in (4,5,6) then '2' "
                 f"when month(to_date('{date_ctrlm}', 'yyyyMMdd')) in (7,8,9) then '3' "
                 f"else '4' end").alias("trimestre"),
            expr(f"case when month(to_date('{date_ctrlm}', 'yyyyMMdd')) in (1,2,3,4,5,6) then '1' "
                 f"else '2' end").alias("semestre")
        )], 
        [
            FIELDS.get("annee_mois"), 
            FIELDS.get("annee_dim"), 
            FIELDS.get("mois"),
            "trimestre_fiscal",
            "jour",
            "trimestre",
            "semestre"
        ]
    )
    
    # Ajouter des attributs dérivés pour des analyses temporelles plus fines
    dim_temps = dim_temps.withColumn(
        "annee_precedente", 
        (dim_temps[FIELDS.get("annee_dim")].cast("int") - 1).cast("string")
    )
    
    dim_temps = dim_temps.withColumn(
        "mois_texte",
        expr(f"case when month(to_date('{date_ctrlm}', 'yyyyMMdd')) = 1 then 'Janvier' "
             f"when month(to_date('{date_ctrlm}', 'yyyyMMdd')) = 2 then 'Février' "
             f"when month(to_date('{date_ctrlm}', 'yyyyMMdd')) = 3 then 'Mars' "
             f"when month(to_date('{date_ctrlm}', 'yyyyMMdd')) = 4 then 'Avril' "
             f"when month(to_date('{date_ctrlm}', 'yyyyMMdd')) = 5 then 'Mai' "
             f"when month(to_date('{date_ctrlm}', 'yyyyMMdd')) = 6 then 'Juin' "
             f"when month(to_date('{date_ctrlm}', 'yyyyMMdd')) = 7 then 'Juillet' "
             f"when month(to_date('{date_ctrlm}', 'yyyyMMdd')) = 8 then 'Août' "
             f"when month(to_date('{date_ctrlm}', 'yyyyMMdd')) = 9 then 'Septembre' "
             f"when month(to_date('{date_ctrlm}', 'yyyyMMdd')) = 10 then 'Octobre' "
             f"when month(to_date('{date_ctrlm}', 'yyyyMMdd')) = 11 then 'Novembre' "
             f"else 'Décembre' end")
    )
    
    return dim_temps
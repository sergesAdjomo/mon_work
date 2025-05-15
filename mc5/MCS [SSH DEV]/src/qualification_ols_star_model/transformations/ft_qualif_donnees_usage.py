# qualification_ols_star_model/transformations/ft_qualif_donnees_usage.py
from pyspark.sql.functions import col, lit, count, sum, avg, rand, abs, round, when
from pyspark.sql import Window
import pyspark.sql.functions as F

from qualification_ols_star_model.constants.fields import FIELDS


def prepare_ft_qualif_donnees_usage(dim_pm_bdt, bv_requetes=None, bv_finances=None):
    """
    Prépare la table de faits avec des mesures calculées à partir de données réelles ou simulées
    
    Args:
        dim_pm_bdt: DataFrame de la dimension PM_BDT
        bv_requetes: DataFrame des requêtes (optionnel)
        bv_finances: DataFrame des données financières (optionnel)
        
    Returns:
        DataFrame: Table de faits FT_qualif_donnees_usage avec des mesures calculées
    """
    # Récupérer les clés de liaison de PM_BDT
    ft_base = dim_pm_bdt.select(
        FIELDS.get("annee_mois_siren"),
        FIELDS.get("annee_mois"),
        FIELDS.get("siren"),
        FIELDS.get("code_tiers")
    )
    
    # Calcul des mesures basées sur les données réelles ou simulées
    # 1. Si nous avons des tables sources réelles
    if bv_requetes is not None and bv_finances is not None:
        # Agrégation des requêtes par SIREN
        requetes_agg = bv_requetes.groupBy(FIELDS.get("siren")).agg(
            count("*").alias("nb_requetes")
        )
        
        # Agrégation des données financières par SIREN
        finances_agg = bv_finances.groupBy(FIELDS.get("siren")).agg(
            avg("montant_ca").alias("ca_moyen"),
            sum("montant_transaction").alias("montant_total")
        )
        
        # Joindre les agrégations avec la base de la table de faits
        ft_qualif_donnees_usage = ft_base.join(
            requetes_agg, 
            on=[FIELDS.get("siren")], 
            how="left"
        ).join(
            finances_agg, 
            on=[FIELDS.get("siren")], 
            how="left"
        )
        
        # Renommer et remplacer les valeurs nulles
        ft_qualif_donnees_usage = ft_qualif_donnees_usage.select(
            FIELDS.get("annee_mois_siren"),
            FIELDS.get("annee_mois"),
            FIELDS.get("siren"),
            F.coalesce(col("nb_requetes"), lit(0)).alias(FIELDS.get("nb_de_rqh")),
            F.coalesce(col("ca_moyen"), lit(0)).alias(FIELDS.get("chiffre_affaire_moyen")),
            F.coalesce(col("montant_total"), lit(0)).alias(FIELDS.get("montant_signe"))
        )
    
    # 2. Si nous n'avons pas de tables sources, générer des données simulées réalistes
    else:
        # Simuler des données d'affaires à l'aide de l'id SIREN pour la reproductibilité
        windowSpec = Window.orderBy(FIELDS.get("siren"))
        
        ft_qualif_donnees_usage = ft_base.withColumn(
            "seed", F.hash(col(FIELDS.get("siren"))) % 1000
        ).withColumn(
            # Nombre de requêtes : valeur entière entre 0 et 100
            FIELDS.get("nb_de_rqh"), 
            (abs(F.hash(col(FIELDS.get("siren"))) % 100)).cast("integer")
        ).withColumn(
            # Chiffre d'affaires moyen : valeur entre 10000 et 1000000
            FIELDS.get("chiffre_affaire_moyen"), 
            round(10000 + (abs(F.hash(col(FIELDS.get("siren"))) % 990000)).cast("double"), 2)
        ).withColumn(
            # Montant signé : positif ou négatif selon le SIREN
            FIELDS.get("montant_signe"), 
            when(F.hash(col(FIELDS.get("siren"))) % 2 == 0, 
                 round((abs(F.hash(col(FIELDS.get("siren"))) % 500000)).cast("double"), 2))
            .otherwise(round(-(abs(F.hash(col(FIELDS.get("siren"))) % 200000)).cast("double"), 2))
        ).select(
            FIELDS.get("annee_mois_siren"),
            FIELDS.get("annee_mois"),
            FIELDS.get("siren"),
            FIELDS.get("nb_de_rqh"),
            FIELDS.get("chiffre_affaire_moyen"),
            FIELDS.get("montant_signe")
        )
    
    return ft_qualif_donnees_usage
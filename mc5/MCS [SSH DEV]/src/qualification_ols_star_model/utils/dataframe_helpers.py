# qualification_ols_star_model/utils/dataframe_helpers.py
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from qualification_ols_star_model.constants.fields import FIELDS


def join_star_model(ft_qualif_donnees_usage, dim_pm_bdt, dim_temps, dim_localisation):
    """
    Assemble le modèle en étoile en joignant toutes les dimensions
    
    Args:
        ft_qualif_donnees_usage: Table de faits
        dim_pm_bdt: Dimension PM_BDT
        dim_temps: Dimension TEMPS
        dim_localisation: Dimension LOCALISATION
        
    Returns:
        DataFrame: Datamart final
    """
    if not isinstance(ft_qualif_donnees_usage, DataFrame):
        raise TypeError("Table de faits non initialisée")

    # Joindre la table de faits avec toutes les dimensions
    result_df = (
        ft_qualif_donnees_usage
        .join(
            dim_pm_bdt,
            on=[FIELDS.get("annee_mois_siren")],
            how="inner"
        )
        .join(
            dim_temps,
            on=[FIELDS.get("annee_mois")],
            how="inner"
        )
        .join(
            dim_localisation,
            on=[FIELDS.get("annee_mois_siren")],
            how="left"
        )
    )

    # Sélectionner les champs finaux pour le datamart
    result_df = result_df.select(
        # Clés
        col(FIELDS.get("annee_mois_siren")),
        col(FIELDS.get("siren")),
        
        # Attributs de DIM_PM_BDT
        col(FIELDS.get("denom_unite_legale")).alias("Raison_sociale"),
        col(FIELDS.get("is_tete_groupe")).alias("Tête_de_groupe"),
        
        # Attributs de DIM_LOCALISATION
        col(FIELDS.get("ville")).alias("Ville"),
        col(FIELDS.get("region")).alias("Région"),
        
        # Mesures de FT_QUALIF_DONNEES_USAGE
        col(FIELDS.get("nb_de_rqh")),
        col(FIELDS.get("chiffre_affaire_moyen")),
        col(FIELDS.get("montant_signe"))
    )

    return result_df


def validate_dataframe(df, name="DataFrame"):
    """
    Valide qu'un DataFrame est bien initialisé
    
    Args:
        df: DataFrame à valider
        name: Nom du DataFrame pour les messages d'erreur
        
    Returns:
        bool: True si le DataFrame est valide
    """
    if not isinstance(df, DataFrame):
        raise TypeError(f"{name} n'est pas un DataFrame valide")
    return True
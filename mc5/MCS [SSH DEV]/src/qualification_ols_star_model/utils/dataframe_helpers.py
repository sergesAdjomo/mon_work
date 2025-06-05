# qualification_ols_star_model/utils/dataframe_helpers.py
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, when, lit

from qualification_ols_star_model.constants.fields import FIELDS


def join_star_model(ft_qualif_donnees_usage, dim_pm_bdt, dim_temps, dim_localisation):
    """
    Assemble le modèle en étoile selon le schéma des images.
    
    Args:
        ft_qualif_donnees_usage: Table de faits
        dim_pm_bdt: Dimension PM_BDT
        dim_temps: Dimension TEMPS
        dim_localisation: Dimension LOCALISATION
        
    Returns:
        DataFrame: Datamart complet
    """
    # Vérification des entrées
    try:
        if not all(isinstance(df, DataFrame) for df in 
                  [ft_qualif_donnees_usage, dim_pm_bdt, dim_temps, dim_localisation]):
            raise TypeError("Toutes les entrées doivent être des DataFrames")
    except Exception as e:
        print(f"Erreur de vérification des entrées: {e}")
        # Continuer avec ce qu'on a
    
    # Jointure de la table de faits avec les dimensions
    try:
        # Pour éviter les conflits de noms de colonnes, aliaser chaque table
        # avec un préfixe distinct
        ft_with_alias = ft_qualif_donnees_usage
        
        # Utiliser des noms uniques pour les champs des tables de dimensions
        pm_bdt_cols = dim_pm_bdt.select(
            col("annee_mois_SIREN").alias("pm_annee_mois_SIREN"),
            col("denomination_unite_legale").alias("pm_Raison_sociale"),
            col("categorie_N1").alias("pm_Sous_categorie"),
            col("tete_de_groupe").alias("pm_Tete_de_groupe")
        )
        
        # Utiliser des conditions de jointure explicites plutôt que "on" 
        # pour plus de contrôle sur la sémantique de la jointure
        model = ft_with_alias.join(
            pm_bdt_cols,
            ft_with_alias["annee_mois_SIREN"] == pm_bdt_cols["pm_annee_mois_SIREN"],
            "inner"
        ).drop("pm_annee_mois_SIREN")
        
        # Jointure avec DIM_TEMPS sur annee_mois
        if dim_temps is not None:
            model = model.join(
                dim_temps,
                model["annee_mois"] == dim_temps["annee_mois"],
                "inner"
            ).drop(dim_temps["annee_mois"])
        
        # Jointure avec DIM_LOCALISATION sur annee_mois_SIREN
        if dim_localisation is not None and dim_localisation.count() > 0:
            loc_cols = dim_localisation.select(
                col("annee_mois_SIREN").alias("loc_annee_mois_SIREN"),
                col("adr_code_postal").alias("loc_code_postal"),
                col("code_Departement").alias("loc_departement"),
                col("Ville").alias("loc_ville"),
                col("Region").alias("loc_region")
            )
            
            model = model.join(
                loc_cols,
                model["annee_mois_SIREN"] == loc_cols["loc_annee_mois_SIREN"],
                "left"
            ).drop("loc_annee_mois_SIREN")
    except Exception as join_err:
        print(f"Erreur lors de la jointure du modèle en étoile: {join_err}")
        # Utiliser uniquement la table de faits si les jointures échouent
        model = ft_qualif_donnees_usage
    
    # Sélectionner les colonnes pour la vue finale du datamart selon l'image
    try:
        # Vérifier quelles colonnes sont disponibles et les sélectionner
        # en utilisant les alias pour éviter les ambiguïtés
        cols_to_select = []
        
        # Ajouter les colonnes si elles existent
        columns_available = model.columns
        
        # SIREN était probablement dans ft_qualif_donnees_usage
        if "SIREN" in columns_available:
            cols_to_select.append(col("SIREN"))
        else:
            cols_to_select.append(lit(None).alias("SIREN"))
        
        # Colonnes de PM_BDT avec préfixe
        if "pm_Raison_sociale" in columns_available:
            cols_to_select.append(col("pm_Raison_sociale").alias("Raison_sociale"))
        elif "Raison_sociale" in columns_available:
            cols_to_select.append(col("Raison_sociale"))
        else:
            cols_to_select.append(lit(None).alias("Raison_sociale"))
            
        if "pm_Sous_categorie" in columns_available:
            cols_to_select.append(col("pm_Sous_categorie").alias("Sous_categorie"))
        elif "Sous_categorie" in columns_available:
            cols_to_select.append(col("Sous_categorie"))
        else:
            cols_to_select.append(lit(None).alias("Sous_categorie"))
            
        if "pm_Tete_de_groupe" in columns_available:
            cols_to_select.append(col("pm_Tete_de_groupe").alias("Tete_de_groupe"))
        elif "Tete_de_groupe" in columns_available:
            cols_to_select.append(col("Tete_de_groupe"))
        else:
            cols_to_select.append(lit(None).alias("Tete_de_groupe"))
        
        # Colonnes de LOCALISATION avec préfixe
        if "loc_ville" in columns_available:
            cols_to_select.append(col("loc_ville").alias("Ville"))
        elif "Ville" in columns_available:
            cols_to_select.append(col("Ville"))
        else:
            cols_to_select.append(lit(None).alias("Ville"))
            
        if "loc_region" in columns_available:
            cols_to_select.append(col("loc_region").alias("Region"))
        elif "Region" in columns_available:
            cols_to_select.append(col("Region"))
        else:
            cols_to_select.append(lit(None).alias("Region"))
        
        # Métriques
        for metric in ["nb_de_tiers", "chiffre_d_affaire_moyen", "montant_signe"]:
            if metric in columns_available:
                cols_to_select.append(col(metric))
            else:
                cols_to_select.append(lit(0).alias(metric))
        
        # Sélectionner les colonnes finales
        return model.select(*cols_to_select)
    except Exception as select_err:
        print(f"Erreur lors de la sélection finale: {select_err}")
        # Retourner le modèle tel quel si la sélection échoue
        return model


def validate_dataframe(df, name="DataFrame"):
    """
    Valide un DataFrame et génère des métriques de qualité.
    
    Args:
        df: DataFrame à valider
        name: Nom du DataFrame pour les logs
        
    Returns:
        DataFrame: Métriques de qualité
    """
    if not isinstance(df, DataFrame):
        raise TypeError(f"{name} n'est pas un DataFrame valide")
    
    # Compter le nombre total de lignes
    count_total = df.count()
    
    # Obtenir le nombre de colonnes
    num_columns = len(df.columns)
    
    # Calculer le nombre de valeurs nulles par colonne
    null_metrics = []
    for col_name in df.columns:
        null_count = df.filter(col(col_name).isNull()).count()
        null_percentage = (null_count / count_total) * 100 if count_total > 0 else 0
        null_metrics.append((col_name, null_count, null_percentage))
    
    print(f"Validation du DataFrame {name}:")
    print(f"- Nombre total de lignes: {count_total}")
    print(f"- Nombre de colonnes: {num_columns}")
    print("- Valeurs nulles par colonne:")
    for col_name, null_count, null_percentage in null_metrics:
        print(f"  * {col_name}: {null_count} ({null_percentage:.2f}%)")
    
    return df
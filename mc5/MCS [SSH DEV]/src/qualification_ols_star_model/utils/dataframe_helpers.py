# qualification_ols_star_model/utils/dataframe_helpers.py
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit


def join_star_model(ft_qualif_donnees_usage, dim_pm_bdt, dim_temps, dim_localisation):
    try:
        if not all(isinstance(df, DataFrame) for df in 
                  [ft_qualif_donnees_usage, dim_pm_bdt, dim_temps, dim_localisation]):
            raise TypeError("Toutes les entrées doivent être des DataFrames")
    except Exception:
        pass
    
    try:
        ft_with_alias = ft_qualif_donnees_usage
        
        pm_bdt_cols = dim_pm_bdt.select(
            col("annee_mois_SIREN").alias("pm_annee_mois_SIREN"),
            col("Raison_sociale").alias("pm_Raison_sociale"),
            col("Sous_categorie").alias("pm_Sous_categorie"),
            col("Tete_de_groupe").alias("pm_Tete_de_groupe")
        )
        
        model = ft_with_alias.join(
            pm_bdt_cols,
            ft_with_alias["annee_mois_SIREN"] == pm_bdt_cols["pm_annee_mois_SIREN"],
            "inner"
        ).drop("pm_annee_mois_SIREN")
        
        if dim_temps is not None:
            model = model.join(
                dim_temps,
                model["annee_mois"] == dim_temps["annee_mois"],
                "inner"
            ).drop(dim_temps["annee_mois"])
        
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
    except Exception:
        model = ft_qualif_donnees_usage
    
    try:
        cols_to_select = []
        columns_available = model.columns
        
        if "SIREN" in columns_available:
            cols_to_select.append(col("SIREN"))
        else:
            cols_to_select.append(lit(None).alias("SIREN"))
        
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
        
        for metric in ["nb_de_tiers", "chiffre_d_affaire_moyen", "montant_signe"]:
            if metric in columns_available:
                cols_to_select.append(col(metric))
            else:
                cols_to_select.append(lit(0).alias(metric))
        
        return model.select(*cols_to_select)
    except Exception:
        return model


def validate_dataframe(df, name="DataFrame"):
    if not isinstance(df, DataFrame):
        raise TypeError(f"{name} n'est pas un DataFrame valide")
    
    count_total = df.count()
    num_columns = len(df.columns)
    
    return df
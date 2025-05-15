# qualification_ols_star_model/constants/fields.py
# Définition des champs pour le modèle en étoile

FIELDS = {
    # Champs communs
    "siren": "siren",
    "annee_mois": "annee_mois",
    "annee_mois_siren": "annee_mois_siren",
    
    # DIM_PM_BDT
    "code_tiers": "code_tiers",
    "code_dr": "code_dr",
    "libelle_dr": "libelle_dr",
    "denom_unite_legale": "denomination_unite_legale",
    "is_tete_groupe": "is_tete_de_groupe",  # Même clé que qualification_ols avec la bonne valeur
    # Autres champs de PM_BDT...
    
    # DIM_TEMPS
    "annee_dim": "annee",
    "mois": "mois",
    
    # DIM_LOCALISATION
    "adr_code_postal": "adr_code_postal",
    "code_departement": "code_Departement",
    "ville": "Ville",
    "region": "Region",
    
    # FT_qualif_donnees_usage
    "nb_de_rqh": "nb_de_rqh",
    "chiffre_affaire_moyen": "chiffre_d_affaire_moyen",
    "montant_signe": "montant_signe",
}
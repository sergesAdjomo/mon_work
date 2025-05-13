"""
Configuration globale pour le modèle en étoile qualification_ols.

Ce fichier centralise les définitions des champs et des tables utilisés
dans le modèle en étoile.
"""

# Configuration des champs utilisés dans le modèle
FIELDS = {
    # Champs source
    "siren": "siren",
    "siret": "siret",
    "denom_unite_legale": "denomination_unite_legale",
    "sous_cat": "sous_categorie",
    "code_tiers": "code_tiers",
    "code_dr": "code_dr",
    "libelle_dr": "libelle_dr",
    "code_postal": "code_postal",
    "code_departement": "code_departement",
    "ville": "ville",
    "region": "region",
    
    # Champs spécifiques au modèle en étoile
    "annee": "annee",
    "mois": "mois",
    "annee_mois": "annee_mois",
    "annee_mois_SIREN": "annee_mois_SIREN",
    "secteur_activite": "secteur_activite",
    "activite_principale": "activite_principale",
    "segment_nrc": "segment_nrc",
    "domaine_de_competences": "domaine_de_competences",
    "competences_exercee": "competences_exercee",
    "zone_montagne": "zone_montagne",
    "type_montagne": "type_montagne",
    "zone_littoral": "zone_littoral",
    "type_littoral": "type_littoral",
    "niveau_d_attractivite": "niveau_d_attractivite",
    "date_de_creation": "date_de_creation",
    "date_de_fermeture": "date_de_fermeture",
    "categorie_n1": "categorie_N1",
    "categorie_n2": "categorie_N2",
    "categorie_n3": "categorie_N3",
    "categorie_n4": "categorie_N4",
    "tete_de_groupe": "tete_de_groupe",
    "part_collective": "part_collective",
    "acv": "ACV",
    "pvd": "PVD",
    "opv": "OPV",
    "effectif": "effectif", 
    "nbre_habitant": "nbre_habitant",
    "nb_de": "nb_de",
    "chiffre_d_affaire_moyen": "chiffre_d_affaire_moyen",
    "montant_signe": "montant_signe",
}

# Configuration des tables du modèle en étoile
TABLES = {
    "DIM_PM_BDT": "DIM_PM_BDT",
    "DIM_TEMPS": "DIM_TEMPS", 
    "DIM_LOCALISATION": "DIM_LOCALISATION",
    "FT_QUALIF_DONNEES_USAGE": "FT_qualif_donnees_usage"
}

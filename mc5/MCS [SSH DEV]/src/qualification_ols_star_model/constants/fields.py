# qualification_ols_star_model/constants/fields.py
"""
Définition des champs pour le modèle en étoile qualification_ols.
"""

FIELDS = {
    # Champs originaux de qualification_ols
    "siren": "siren",
    "siret": "siret",
    "denom_unite_legale": "denomination_unite_legale",
    "sous_cat": "sous_categorie",
    "adr_code_postal": "adr_code_postal",
    "lib_bureau_distrib": "libelle_bureau_distribution",
    "code_tiers": "code_tiers",
    "is_tete_groupe": "is_tete_de_groupe",
    "code_departement": "code_departement",
    "code_region": "code_region",
    "lib_clair_region": "libelleclair_region",
    "is_ols": "is_ols",
    "etat_admin": "etat_administratif", 
    "etab_siege": "etablissement_siege",
    "code_etatiers": "code_etatiers",
    "dat_horodat": "dat_horodat",
    "siret_par_date": "siret_par_date",
    
    # Champs spécifiques pour le modèle en étoile (selon les images)
    "annee_mois": "annee_mois",
    "annee_mois_siren": "annee_mois_SIREN",
    "annee_dim": "annee",
    "mois": "mois",
    
    # Champs pour les mesures de la table de faits
    "nb_de_tiers": "nb_de_tiers",
    "nb_de_rqh": "nb_de_rqh",  # nombre de requêtes
    "chiffre_affaire_moyen": "chiffre_d_affaire_moyen",
    "montant_signe": "montant_signe",
    
    # Champs pour les dimensions (d'après les images)
    "ville": "Ville",
    "region": "Region",
    "nbre_habitant": "nbre_habitant",
    
    # Colonnes supplémentaires pour DIM_PM_BDT selon le schéma complet
    "code_dr": "code_dr", 
    "libelle_dr": "libelle_dr",
    "denomination_unite_legale": "denomination_unite_legale",
    "secteur_activite": "secteur_activite", 
    "activite_principale": "activite_principale",
    "segment_taille_PM": "segment_taille_PM",
    "domaine_de_competences": "domaine_de_competences",
    "competences_avancee": "competences_avancee",
    "zone_montagne": "zone_montagne",
    "type_montagne": "type_montagne",
    "zone_littoral": "zone_littoral",
    "type_littoral": "type_littoral",
    "niveau_attractivite": "niveau_d_attractivite",
    "date_creation": "date_de_creation",
    "date_fermeture": "date_de_fermeture",
    "categorie_N1": "categorie_N1",
    "categorie_N2": "categorie_N2",
    "categorie_N3": "categorie_N3", 
    "categorie_N4": "categorie_N4",
    "part_collectivite": "part_collectivite",
    "ACV": "ACV",
    "PVD": "PVD",
    "CPV": "CPV",
    "QPV": "QPV",
    "effectif": "effectif",
}
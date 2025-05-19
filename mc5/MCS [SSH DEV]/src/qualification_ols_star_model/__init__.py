"""
Package qualification_ols_star_model pour la création d'un modèle en étoile.

Ce package implémente un modèle en étoile pour les données qualification_ols avec:
- 3 dimensions (DIM_PM_BDT, DIM_TEMPS, DIM_LOCALISATION)
- 1 table de faits (FT_qualif_donnees_usage)
"""

__all__ = ['TraitementQualificationOLSStarModel']

# Pour faciliter l'import direct
from qualification_ols_star_model.traitement import TraitementQualificationOLSStarModel
# ingesteur_spark_JDBC/src/spark_jdbc/code/params.py



from spark_jdbc.code.settings import Settings

import json

import re

from operator import attrgetter

class Params:

    def __init__(self, settings:Settings):
        """

Initialise une nouvelle instance de la classe avec les paramètres donnés.

Args:

        settings (Settings): Les paramètres de configuration à utiliser.
        """
        self.settings = settings
        self.load_params()

    def load_params(self):
        """

Charge les paramètres à partir d'un fichier JSON, remplace les variables dans le fichier avec les valeurs des paramètres de configuration et charge les informations des tables JDBC.

Étapes:

1. Charge le fichier brut en tant que chaîne de caractères.

2. Remplace les occurrences de la regex {{XX}} par le contenu de XX dans les paramètres de configuration.

3. Débogue le fichier de configuration après remplacement des variables.

4. Charge les informations des tables JDBC à partir de la chaîne JSON modifiée et les stocke dans l'attribut params.

Raises:

        FileNotFoundError: Si le fichier spécifié dans self.settings.param_metadata_json n'existe pas.
        json.JSONDecodeError: Si le contenu du fichier JSON est invalide.
        """
        #On charge le fichier brut en string
        with open(self.settings.param_metadata_json) as paramFile:
            param_str = paramFile.read()

        #Permet de remplacer les matchs de la regex {{XX}} avec le contenu XX au niveau des settings
        #https://stackoverflow.com/questions/32670413/replace-all-matches-using-re-findall
        params_str = re.sub(
            r"{{(.*?)}}",
            lambda match: "{0}".format(attrgetter(match.group(1))(self.settings)),
            param_str,
        )



        self.settings.logger.debug('Fichier de configuration dévariabilisé : ' + params_str)
        
        self.params = json.loads(params_str)['jdbc_tables_infos']



    def get_filtred_params(self, filterItem, filterValue=None):
        """

Filtre les paramètres en fonction d'un élément de filtre et d'une valeur de filtre optionnelle.

Args:

filterItem (str): L'élément de filtre à utiliser pour filtrer les paramètres.

filterValue (str, optionnel): La valeur de filtre à utiliser pour filtrer les paramètres.

Si None, tous les éléments avec filterItem seront inclus.

Returns:

        list: Une liste de paramètres filtrés qui correspondent aux critères de filtre.
        """
        filtred_params = []
        for item in self.params:
            if item.get(filterItem) and (filterValue is None or item[filterItem]==filterValue):
                filtred_params.append(item)
        return filtred_params




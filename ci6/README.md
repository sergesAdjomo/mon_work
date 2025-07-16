    # Ingestion, traitement et exposition des données CI6

## 1. Lancement des scripts KSH

Trois scripts principaux orchestrent les traitements :

- **Ingestion Kafka (zone brute)**  
  Transfère les données du topic Kafka source vers la zone brute Hive.  
  Dans le dossier `/shell`, lancez :
  ```sh
  ./DDCI6AJA2_CONSUMER.KSH
  ```
  > **Attention** : Ne pas lancer le consumer sans lancer également l’expose, sinon les données risquent d’être perdues.

- **Traitement Spark (zone fine)**  
  Transfère les données de la zone brute vers la zone fine.  
  Dans le dossier `/shell`, lancez :
  ```sh
  ./DDCI6AJA0_BTCH.KSH
  ```

- **Exposition des données**  
  Exporte ou publie les données traitées.  
  Dans le dossier `/shell`, lancez :
  ```sh
  ./DDCI6AJA2_EXPO.KSH
  ```

### Notes sur les traitements

- Sur la **couche brute**, l’écriture se fait en mode **overwrite** (remplacement des données).
- Sur la **couche fine**, l’écriture se fait en mode **append** (ajout des données).
- En environnement pré-production et production, il est impératif d’**ordonner les traitements** : ingestion → traitement → exposition.

## 2. Gestion du schéma des données

> **Note :** Pour les flux Kafka, la structure des données est définie côté producteur (schéma Avro ou JSON Schema) et gérée automatiquement par le consumer via le Schema Registry. Aucun mapping manuel n'est nécessaire dans le projet CI6 pour ce flux.

## 3. Parcours et format des données

- **Kafka** : Les données sont lues au format Avro ou JSON selon le topic.
- **Zone brute** : Les données sont stockées en **JSON** dans Hive/HDFS.
- **Zone fine** : Les données sont transformées et stockées dans un format optimisé (ex : Parquet ou autre selon le paramétrage Spark).

**Résumé du parcours** :  
Kafka (Avro/JSON) → Brute (JSON) → Fine (Parquet/optimisé)

## 4. Informations fonctionnelles

Pour les informations fonctionnelles, consultez la page Confluence CI6 :  
https://confluence.serv.cdc.fr/pages/viewpage.action?pageId=1259077934

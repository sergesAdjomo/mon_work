 
    # Documentation technique du dossier `src`

Ce dossier regroupe l’ensemble du code source Python du projet CI6, organisé en modules spécialisés :

## 1. `consumer/`

- **Rôle** : Consommation des données depuis Kafka et alimentation de la zone brute Hive/HDFS.
- **Fichiers principaux** :
  - `consumer.py` : Classe principale pour la connexion à Kafka, la désérialisation des messages (en utilisant le schéma du Schema Registry), la gestion des offsets et l’écriture dans Hive/HDFS.
  - `main.py` : Point d’entrée pour lancer le consommateur.

## 2. `traitement_spark/`

- **Rôle** : Orchestration des traitements de données avec Apache Spark.
- **Sous-dossiers** :
  - `code/` : Logique métier et utilitaires Spark (gestion de schéma, sécurité, helpers…)
    - `appsec.py` : Gestion de la sécurité et des credentials.
    - `schema_registry.py` : Accès au schema registry Kafka (récupération dynamique du schéma, pas de mapping manuel nécessaire).
    - `settings.py` : Paramétrage des traitements Spark.
    - `utils.py` : Fonctions utilitaires pour Spark.
  - `test/` : Tests unitaires et de validation des traitements Spark.
- **Fichier principal** :
  - `main.py` : Point d’entrée pour exécuter les traitements Spark.

## 3. `expose/`

- **Rôle** : Préparation et exposition des données traitées vers l’extérieur.
- **Fichiers principaux** :
  - `expose.py` : Logique d’exposition des données (export, publication…)
  - `setschema.py` : Conversion dynamique du schéma Avro récupéré (via Schema Registry) en schéma Spark (`StructType`).
  - `main.py` : Point d’entrée pour l’exposition.

---

## Parcours technique des données

- **Kafka** : Les données sont lues au format **Avro** (pour des topics publics) ou **JSON** (pour des topics privés internes à l'équipe), selon le topic.
- **Zone brute** : Les données sont stockées en **DataFrame** au format Parquet.
- **Zone fine** : Les données sont transformées et stockées dans un format Parquet.

**Résumé du parcours** :  
Kafka (Avro/JSON) → Brute (DataFrame Parquet) → Fine (Parquet)

## Lien utile

Pour les informations fonctionnelles, consultez la page Confluence CI6 :  
https://confluence.serv.cdc.fr/pages/viewpage.action?pageId=1259077934

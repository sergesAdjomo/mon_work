 
####################################################
##  Script chiffrage durée des traitements spark  ##
##              Théo JAOUDET - 2024               ##
####################################################

import sys
from icdc.hdputils.spark import sparkUtils
import time
import pandas as pd
from pyspark.sql.functions import col, when, lit
from pyspark.sql.dataframe import *
from pyspark.sql import DataFrame

path: str = r"src/traitement_spark/test/resources/"
spark_csv: str = f"{path}spark_test.csv"
spark_filter: str = f"{path}spark_filter.csv"
spark_result: str = f"{path}perfo_test.csv"


def difference(df):
    df["Difference"] = float(0)
    for i in range(1, len(df) - 2, 2):
        curr = df.iloc[i]["Temps"]
        self_val = df.iloc[i + 1]["Temps"]
        df.at[i, "Difference"] = curr - self_val
        df.at[i + 1, "Difference"] = self_val - curr
    return df


class PerfoTest:
    def __init__(self) -> None:
        self.df: DataFrame = None
        self.df2: DataFrame = None

    def transformations_df(self):
        # Given
        df_pd = pd.DataFrame(columns=["Type_action", "Temps", "id_df"])
        startAll = time.time()

        # When transformations
        start = time.time()
        spark = sparkUtils()
        print("Temps création spark_session :", time.time() - start)
        df_pd.loc[len(df_pd)] = ["creation_spark_session", time.time() - start, 0]

        # Creation
        start = time.time()
        df = spark.spark.read.csv(spark_csv, sep=";", header=True)
        print("Temps création df :", time.time() - start)
        df_pd.loc[len(df_pd)] = ["creation_df", time.time() - start, id(df)]
        start = time.time()
        self.df = spark.spark.read.csv(spark_csv, sep=";", header=True)
        print("Temps création self.df :", time.time() - start)
        df_pd.loc[len(df_pd)] = ["creation_self_df", time.time() - start, id(self.df)]

        # Filter
        start = time.time()
        df = df.filter((col("type") == "action"))
        print("Temps filter df :", time.time() - start)
        df_pd.loc[len(df_pd)] = ["filter_df", time.time() - start, id(df)]
        start = time.time()
        self.df = self.df.filter((col("type") == "action"))
        print("Temps filter self.df :", time.time() - start)
        df_pd.loc[len(df_pd)] = ["filter_self_df", time.time() - start, id(self.df)]

        # Join
        start = time.time()
        df2 = spark.spark.read.csv(spark_filter, sep=";", header=True)
        df2 = df2.join(df, "url", "left")
        print("Temps join df :", time.time() - start)
        df_pd.loc[len(df_pd)] = ["join_df", time.time() - start, id(df2)]
        start = time.time()
        self.df2 = spark.spark.read.csv(spark_filter, sep=";", header=True)
        self.df2 = self.df2.join(self.df, "url", "left")
        print("Temps join self.df :", time.time() - start)
        df_pd.loc[len(df_pd)] = ["join_self_df", time.time() - start, id(self.df2)]

        # Union
        start = time.time()
        df2 = spark.spark.read.csv(spark_filter, sep=";", header=True)
        df2 = df2.union(df)
        print("Temps union df :", time.time() - start)
        df_pd.loc[len(df_pd)] = ["union_df", time.time() - start, id(df2)]
        start = time.time()
        self.df2 = spark.spark.read.csv(spark_filter, sep=";", header=True)
        self.df2 = self.df2.union(self.df)
        print("Temps union self.df :", time.time() - start)
        df_pd.loc[len(df_pd)] = ["union_self_df", time.time() - start, id(self.df2)]

        # withColumn
        start = time.time()
        df = df.withColumn(
            "Nom_de_Services",
            when(
                col("url").like(
                    "https://www.banquedesterritoires.fr/produits-services/services-digitaux/mon-comparateur-energetiqu%"
                ),
                "Mon Comparateur Energétique",
            )
            .when(
                (col("pagetitle") == "PRIORENO - Accueil")
                & (
                    col("url").like(
                        "https://mon-compte.banquedesterritoires.fr/#/ext/rn1-bdt-wc-prioreno/%"
                    )
                ),
                "PrioRéno BP",
            )
            .when(
                (col("pagetitle") == "PrioRéno LS - Accueil")
                & (
                    col("url")
                    == "https://mon-compte.banquedesterritoires.fr/#/ext/rn2-bdt-wc-prioreno/accueil"
                ),
                "PrioRéno LS",
            )
            .when(
                col("pagetitle").like("France Foncier + : %"),
                "France Foncier +",
            )
            .when(
                col("url")
                == "https://www.banquedesterritoires.fr/produits-services/services-digitaux/cartographie-des-structures-innovation-francaises",
                "Cartographie des structures d'innovation territoriales",
            )
            .otherwise("Autres pages"),
        ).withColumn("Nom_Categorie", lit("Services Inno"))
        print("Temps withColumn df :", time.time() - start)
        df_pd.loc[len(df_pd)] = ["withColumn_df", time.time() - start, id(df)]
        start = time.time()
        self.df = self.df.withColumn(
            "Nom_de_Services",
            when(
                col("url").like(
                    "https://www.banquedesterritoires.fr/produits-services/services-digitaux/mon-comparateur-energetiqu%"
                ),
                "Mon Comparateur Energétique",
            )
            .when(
                (col("pagetitle") == "PRIORENO - Accueil")
                & (
                    col("url").like(
                        "https://mon-compte.banquedesterritoires.fr/#/ext/rn1-bdt-wc-prioreno/%"
                    )
                ),
                "PrioRéno BP",
            )
            .when(
                (col("pagetitle") == "PrioRéno LS - Accueil")
                & (
                    col("url")
                    == "https://mon-compte.banquedesterritoires.fr/#/ext/rn2-bdt-wc-prioreno/accueil"
                ),
                "PrioRéno LS",
            )
            .when(
                col("pagetitle").like("France Foncier + : %"),
                "France Foncier +",
            )
            .when(
                col("url")
                == "https://www.banquedesterritoires.fr/produits-services/services-digitaux/cartographie-des-structures-innovation-francaises",
                "Cartographie des structures d'innovation territoriales",
            )
            .otherwise("Autres pages"),
        ).withColumn("Nom_Categorie", lit("Services Inno"))
        print("Temps withColumn self.df :", time.time() - start)
        df_pd.loc[len(df_pd)] = ["withColumn_self_df", time.time() - start, id(self.df)]

        # When actions
        # Select
        start = time.time()
        df = df.select("url", "pagetitle", "type")
        print("Temps select df :", time.time() - start)
        df_pd.loc[len(df_pd)] = ["select_df", time.time() - start, id(df)]
        start = time.time()
        self.df = self.df.select("url", "pagetitle", "type")
        print("Temps select self.df :", time.time() - start)
        df_pd.loc[len(df_pd)] = ["select_self_df", time.time() - start, id(self.df)]

        # Sort
        start = time.time()
        df = df.sort("url", ascending=False)
        print("Temps sort df :", time.time() - start)
        df_pd.loc[len(df_pd)] = ["sort_df", time.time() - start, id(df)]
        start = time.time()
        self.df = self.df.sort("url", ascending=False)
        print("Temps sort self.df :", time.time() - start)
        df_pd.loc[len(df_pd)] = ["sort_self_df", time.time() - start, id(self.df)]

        # Count
        start = time.time()
        res = df.count()
        print("Temps count df :", time.time() - start)
        df_pd.loc[len(df_pd)] = ["count_df", time.time() - start, 0]
        start = time.time()
        res = self.df.count()
        print("Temps count self.df :", time.time() - start)
        df_pd.loc[len(df_pd)] = ["count_self_df", time.time() - start, 0]

        # Collect
        start = time.time()
        res = len(df.collect())
        print("Temps len collect df :", time.time() - start)
        df_pd.loc[len(df_pd)] = ["collect_df", time.time() - start, 0]
        start = time.time()
        res = len(self.df.collect())
        print("Temps len collect self.df :", time.time() - start)
        df_pd.loc[len(df_pd)] = ["collect_self_df", time.time() - start, 0]

        # Then
        spark.spark.stop()
        print("Le programme a duré :", time.time() - startAll)
        df_pd.loc[len(df_pd)] = ["programme_all", time.time() - startAll, 0]

        df_pd = difference(df_pd)
        df_pd.to_csv(spark_result, sep=";", header=True, index=False)
        print("Fichier csv résultat crée")


cut = PerfoTest()
cut.transformations_df()

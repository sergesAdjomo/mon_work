 
from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import col, row_number, substring, when
from traitement_spark.code.settings import Settings
from traitement_spark.code.utils import CommonUtils

import qualification_ols.qualification_ols_fields as Fields


class TraitementQualificationOLSDataFrame(CommonUtils):
    def __init__(self, spark, config):

        self.spark = spark
        self.config = config

        # BVs lues sur le HUB
        self.bv_pm: DataFrame = None
        self.bv_tiers: DataFrame = None
        self.bv_coord_postales: DataFrame = None
        self.bv_departement: DataFrame = None
        self.bv_region: DataFrame = None

        # Dataframes pour le calcul des codes_tiers
        self.base_pm: DataFrame = None
        self.pm: DataFrame = None
        self.pm_full: DataFrame = None
        self.t0: DataFrame = None
        self.t1: DataFrame = None
        self.t2: DataFrame = None
        self.t3: DataFrame = None
        self.t4: DataFrame = None

        # Dataframe du mart exposé
        self.df: DataFrame = None

        super().__init__(self.spark, self.config)

        self.settings = Settings(self.config, self.logger, self.date_ctrlm)

    def compute_code_tiers_cases(self):
        # Etape 1 : calculer le dataframe "base_pm", avec les bons filtres, et créer un produit cartésien avec la bv_personne_morale pour récupérer le flag "is_ols" à 1 sur tous les SIRET
        self.base_pm = self.bv_pm.select(
            col(Fields.FIELDS.get("siren")).alias("base_siren"),
            col(Fields.FIELDS.get("siret")).alias("base_siret"),
            Fields.FIELDS.get("denom_unite_legale"),
            Fields.FIELDS.get("sous_cat"),
            Fields.FIELDS.get("is_tete_groupe"),
            Fields.FIELDS.get("is_ols"),
        ).filter((col(Fields.FIELDS.get("is_ols"))) == "1")
        self.pm = self.bv_pm.select(
            col(Fields.FIELDS.get("siren")).alias("pm_siren"),
            col(Fields.FIELDS.get("siret")).alias("pm_siret"),
        )
        self.pm_full = self.base_pm.join(
            self.pm, on=[self.base_pm.base_siren == self.pm.pm_siren], how="inner"
        )

        # Etape 2 : définir les t0, t1, t2, t3, t4 à partir de la bv_tiers
        self.t0 = self.bv_tiers.select(
            Fields.FIELDS.get("siren"),
            Fields.FIELDS.get("siret"),
            col(Fields.FIELDS.get("code_tiers")).alias("t0_code_tiers"),
        ).filter(col(Fields.FIELDS.get("code_etatiers")) == "A")
        self.t1 = self.bv_tiers.select(
            Fields.FIELDS.get("siren"),
            Fields.FIELDS.get("siret"),
            col(Fields.FIELDS.get("code_tiers")).alias("t1_code_tiers"),
        ).filter(col(Fields.FIELDS.get("code_etatiers")) != "A")
        self.t2 = self.bv_tiers.select(
            Fields.FIELDS.get("siren"),
            Fields.FIELDS.get("siret"),
            col(Fields.FIELDS.get("code_tiers")).alias("t2_code_tiers"),
        ).filter(col(Fields.FIELDS.get("code_etatiers")) == "A")
        self.t3 = self.bv_tiers.select(
            Fields.FIELDS.get("siren"),
            Fields.FIELDS.get("siret"),
            col(Fields.FIELDS.get("code_tiers")).alias("t3_code_tiers"),
        ).filter(col(Fields.FIELDS.get("code_etatiers")) == "A")
        self.t4 = self.bv_tiers.select(
            Fields.FIELDS.get("siren"),
            Fields.FIELDS.get("siret"),
            col(Fields.FIELDS.get("code_tiers")).alias("t4_code_tiers"),
        )

        # Etape 3 : créer toutes les jointures avec base_pm ou pm, et les t0, t1, t2, t3, t4, pour obtenir tous les cas de code_tiers associés possibles
        self.df = (
            self.pm_full.join(
                self.t0,
                on=[
                    self.base_pm.base_siren == self.t0.siren,
                    self.base_pm.base_siret == self.t0.siret,
                ],
                how="left",
            )
            .drop("siren", "siret")
            .join(
                self.t1,
                on=[
                    self.base_pm.base_siren == self.t1.siren,
                    self.base_pm.base_siret == self.t1.siret,
                ],
                how="left",
            )
            .drop("siren", "siret")
            .join(
                self.t2,
                on=[
                    self.pm.pm_siren == self.t2.siren,
                    self.pm.pm_siret == self.t2.siret,
                ],
                how="left",
            )
            .drop("siren", "siret")
            .join(self.t3, on=[self.pm.pm_siren == self.t3.siren], how="left")
            .drop("siren", "siret")
            .join(self.t4, on=[self.pm.pm_siren == self.t4.siren], how="left")
            .drop("siren", "siret")
        )

        return self

    def compute_df_with_priority_code_tiers(self):
        if not isinstance(self.df, DataFrame):
            raise TypeError("Expected a DataFrame")

        # Etape 4 : choisir le code_tiers en fonction de l'ordre de priorité : t0 > t1 > t2 > t3 > t4
        self.df = self.df.withColumn(
            "code_tiers",
            when(col("t0_code_tiers").isNotNull(), col("t0_code_tiers"))
            .when(col("t1_code_tiers").isNotNull(), col("t1_code_tiers"))
            .when(col("t2_code_tiers").isNotNull(), col("t2_code_tiers"))
            .when(col("t3_code_tiers").isNotNull(), col("t3_code_tiers"))
            .when(col("t4_code_tiers").isNotNull(), col("t4_code_tiers")),
        )

        # Etape 5 : renommer les colonnes et sélectionner uniquement celles utiles
        self.df = self.df.select(
            col("base_siren").alias(Fields.FIELDS.get("siren")),
            col("base_siret").alias(Fields.FIELDS.get("siret")),
            Fields.FIELDS.get("code_tiers"),
            Fields.FIELDS.get("denom_unite_legale"),
            Fields.FIELDS.get("sous_cat"),
            Fields.FIELDS.get("is_tete_groupe"),
        )

        return self

    def join_bvs(self):
        if not isinstance(self.df, DataFrame):
            raise TypeError("Expected a DataFrame")

        self.df = (
            self.df.join(self.bv_coord_postales, on=["code_tiers"], how="left")
            .join(
                self.bv_departement,
                on=substring(self.bv_coord_postales["adr_code_postal"], 1, 2)
                == self.bv_departement["code_departement"],
                how="left",
            )
            .join(self.bv_region, on=["code_region"], how="left")
        )

        return self

    def compute_siret_par_date(self):
        if not isinstance(self.df, DataFrame):
            raise TypeError("Expected a DataFrame")

        window_spec = Window.partitionBy(
            Fields.FIELDS.get("siren"), Fields.FIELDS.get("siret")
        ).orderBy(self.df[Fields.FIELDS.get("dat_horodat")].desc())

        self.df = self.df.withColumn(
            Fields.FIELDS.get("siret_par_date"), row_number().over(window_spec)
        )

        return self

    def compute_qualif_ols_dataframe(self):
        if not isinstance(self.df, DataFrame):
            raise TypeError("Expected a DataFrame")

        self.df = self.df.select(
            col(Fields.FIELDS.get("siren")),
            col(Fields.FIELDS.get("denom_unite_legale")).alias("Raison_sociale"),
            col(Fields.FIELDS.get("sous_cat")),
            col(Fields.FIELDS.get("lib_bureau_distrib")).alias("Ville"),
            col(Fields.FIELDS.get("lib_clair_region")).alias("Région"),
            col(Fields.FIELDS.get("code_tiers")),
            col(Fields.FIELDS.get("is_tete_groupe")).alias("Tête_de_groupe"),
        ).filter(col(Fields.FIELDS.get("siret_par_date")) == 1)

        return self

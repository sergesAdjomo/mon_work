from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when
from traitement_spark.code.settings import Settings
from traitement_spark.code.utils import CommonUtils

import service_innovant.service_innovant_fields as Fields


class TraitementServiceInnovantDataFrame(CommonUtils):
    def __init__(self, spark, config):

        self.spark = spark
        self.config = config

        self.df: DataFrame = None
        self.service_innovant_df: DataFrame = None
        self.page_offres_df: DataFrame = None

        super().__init__(self.spark, self.config)

        self.settings = Settings(self.config, self.logger, self.date_ctrlm)

    def set_urls(self):
        urls = [
            "https://www.banquedesterritoires.fr/produits-services/services-digitaux/dataviz-pvd",
            "https://www.banquedesterritoires.fr/revitalisation-petites-villes-demain/solutions",
            "https://www.banquedesterritoires.fr/produits-services/services-digitaux/dataviz-territoires-dindustrie",
            "https://www.banquedesterritoires.fr/produits-services/services-digitaux/prioreno-ponts",
            "https://www.banquedesterritoires.fr/renovation-ouvrages-d-art",
            "https://www.banquedesterritoires.fr/produits-services/services-digitaux/offre-prioreno",
            "https://www.banquedesterritoires.fr/produits-services/services-digitaux/mon-diag-ecoles",
            "https://www.banquedesterritoires.fr/produits-services/services-digitaux",
            "https://www.banquedesterritoires.fr/produits-services/services-digitaux/service-prioreno-renovation-logements-sociaux",
        ]
        return urls

    def filter_service_innovant(self) -> DataFrame:
        try:
            self.service_innovant_df = self.df.filter(
                (col(Fields.FIELDS.get("type")) == "action")
                & (
                    (
                        (col(Fields.FIELDS.get("pagetitle")) == "PRIORENO - Accueil")
                        & (
                            col(Fields.FIELDS.get("url")).like(
                                "https://mon-compte.banquedesterritoires.fr/#/ext/rn1-bdt-wc-prioreno/%"
                            )
                        )
                    )
                    | (
                        (col(Fields.FIELDS.get("pagetitle")) == "PrioRéno LS - Accueil")
                        & (
                            col(Fields.FIELDS.get("url"))
                            == "https://mon-compte.banquedesterritoires.fr/#/ext/rn2-bdt-wc-prioreno/accueil"
                        )
                    )
                    | (
                        col(Fields.FIELDS.get("pagetitle")).like(
                            "France Foncier + : le portail national de recherche de terrain%"
                        )
                    )
                    | (
                         col(Fields.FIELDS.get("pagetitle")).like(
                             "France Foncier + : industrial%"
                         )
                     )
                    | (
                        col(Fields.FIELDS.get("url")).like(
                            "https://www.banquedesterritoires.fr/produits-services/services-digitaux/mon-comparateur-energetiqu%"
                        )
                    )
                    | (
                        col(Fields.FIELDS.get("url"))
                        == "https://www.banquedesterritoires.fr/produits-services/services-digitaux/cartographie-des-structures-innovation-francaises"
                    )
                    | (
                        col(Fields.FIELDS.get("url"))
                        == "https://www.banquedesterritoires.fr/produits-services/services-digitaux/cartographie-lieu-innovation-francais"
                    )  
                    | (
                        col(Fields.FIELDS.get("pagetitle")) == "Contrib France Foncier - Home"
                    )
                    | (
                        col(Fields.FIELDS.get("url"))
                        == "https://www.banquedesterritoires.fr/notice-de-lecture-des-donnees-du-portail-france-foncier"
                    )
                )
            )
            return self
        except Exception as e:
            self.logger.error(f"unable to continue due to issues related to : {e}")
            raise

    def add_inno_columns(self) -> DataFrame:
        try:
            self.service_innovant_df = self.service_innovant_df.withColumn(
                "Nom_Services",
                when(
                    col(Fields.FIELDS.get("url")).like(
                        "https://www.banquedesterritoires.fr/produits-services/services-digitaux/mon-comparateur-energetiqu%"
                    ),
                    "Mon Comparateur Energétique",
                )
                .when(
                    (col(Fields.FIELDS.get("pagetitle")) == "PRIORENO - Accueil")
                    & (
                        col(Fields.FIELDS.get("url")).like(
                            "https://mon-compte.banquedesterritoires.fr/#/ext/rn1-bdt-wc-prioreno/%"
                        )
                    ),
                    "PrioRéno BP",
                )
                .when(
                    (col(Fields.FIELDS.get("pagetitle")) == "PrioRéno LS - Accueil")
                    & (
                        col(Fields.FIELDS.get("url"))
                        == "https://mon-compte.banquedesterritoires.fr/#/ext/rn2-bdt-wc-prioreno/accueil"
                    ),
                    "PrioRéno LS",
                )
                .when(
                    col(Fields.FIELDS.get("pagetitle")).like(
                        "France Foncier + : le portail national de recherche de terrain%"
                    ),
                    "France Foncier +",
                )
                .when(
                    col(Fields.FIELDS.get("pagetitle")).like(
                        "France Foncier + : industrial%"
                    ),
                    "France Foncier +",
                )
                .when(
                    col(Fields.FIELDS.get("url")) == "https://www.banquedesterritoires.fr/notice-de-lecture-des-donnees-du-portail-france-foncier",
                    "France Foncier +",
                )
                .when(
                    col(Fields.FIELDS.get("pagetitle")) == "Contrib France Foncier - Home",
                    "France Foncier + Connecté",
                )
                .when(
                    (
                        col(Fields.FIELDS.get("url"))
                        == "https://www.banquedesterritoires.fr/produits-services/services-digitaux/cartographie-des-structures-innovation-francaises"
                    )
                    | (
                        col(Fields.FIELDS.get("url"))
                        == "https://www.banquedesterritoires.fr/produits-services/services-digitaux/cartographie-lieu-innovation-francais"
                    ),
                    "Cartographie des structures d'innovation territoriales",
                )
                .otherwise("Autres pages"),
            ).withColumn("Nom_Categorie", 
                when(
                    col(Fields.FIELDS.get("url")) == "https://www.banquedesterritoires.fr/notice-de-lecture-des-donnees-du-portail-france-foncier",
                    "Notice de Lecture"
                )
                .otherwise("Services Innovants")
            )
            return self
        except Exception as e:
            self.logger.error(f"unable to continue due to issues related to : {e}")
            raise

    def filter_page_offres(self) -> DataFrame:
        try:
            self.page_offres_df = self.df.filter(
                (col(Fields.FIELDS.get("type")) == "action")
                & (col(Fields.FIELDS.get("url")).isin(self.set_urls()))
            )
            return self
        except Exception as e:
            self.logger.error(f"unable to continue due to issues related to : {e}")
            raise

    def add_page_offres_columns(self) -> DataFrame:
        try:
            self.page_offres_df = self.page_offres_df.withColumn(
                "Nom_Services",
                when(
                    col(Fields.FIELDS.get("url"))
                    == "https://www.banquedesterritoires.fr/produits-services/services-digitaux/dataviz-pvd",
                    "Dataviz PVD",
                )
                .when(
                    col(Fields.FIELDS.get("url"))
                    == "https://www.banquedesterritoires.fr/revitalisation-petites-villes-demain/solutions",
                    "Dataviz PVD",
                )
                .when(
                    col(Fields.FIELDS.get("url"))
                    == "https://www.banquedesterritoires.fr/produits-services/services-digitaux/dataviz-territoires-dindustrie",
                    "Dataviz TI Privé",
                )
                .when(
                    col(Fields.FIELDS.get("url"))
                    == "https://www.banquedesterritoires.fr/produits-services/services-digitaux/prioreno-ponts",
                    "PrioReno Ponts",
                )
                .when(
                    col(Fields.FIELDS.get("url"))
                    == "https://www.banquedesterritoires.fr/renovation-ouvrages-d-art",
                    "PrioReno Ponts",
                )
                .when(
                    col(Fields.FIELDS.get("url"))
                    == "https://www.banquedesterritoires.fr/produits-services/services-digitaux/offre-prioreno",
                    "PrioRéno BP",
                )
                .when(
                    col(Fields.FIELDS.get("url"))
                    == "https://www.banquedesterritoires.fr/produits-services/services-digitaux/mon-diag-ecoles",
                    "Mon Diag Ecoles",
                )
                .when(
                    col(Fields.FIELDS.get("url"))
                    == "https://www.banquedesterritoires.fr/produits-services/services-digitaux",
                    "Page Produits - services digitaux",
                )
                .when(
                    col(Fields.FIELDS.get("url"))
                    == "https://www.banquedesterritoires.fr/produits-services/services-digitaux/service-prioreno-renovation-logements-sociaux",
                    "PrioRéno LS",
                )
                .otherwise("Other"),
            ).withColumn("Nom_Categorie", lit("Page Offres"))
            return self
        except Exception as e:
            self.logger.error(f"unable to continue due to issues related to : {e}")
            raise
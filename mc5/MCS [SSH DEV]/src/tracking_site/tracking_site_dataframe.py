 
from pyspark.sql import DataFrame
from pyspark.sql.functions import avg, col, count, countDistinct
from pyspark.sql.functions import sum as S
from pyspark.sql.functions import when
from pyspark.sql.window import Window
from traitement_spark.code.settings import Settings
from traitement_spark.code.utils import CommonUtils

import tracking_site.tracking_site_fields as Fields


class TraitementTrackingSiteDataFrame(CommonUtils):
    def __init__(self, spark, config):

        self.spark = spark
        self.config = config
        self.df: DataFrame = None

        super().__init__(self.spark, self.config)

        self.settings = Settings(self.config, self.logger, self.date_ctrlm)
        self.URL_BDT = Fields.FIELDS.get("url_bdt")

    def set_urls(self):
        urls = [
            "https://www.banquedesterritoires.fr/collectivites-locales",
            "https://www.banquedesterritoires.fr/entreprises-publiques-locales",
            "https://www.banquedesterritoires.fr/habitat-social",
            "https://www.banquedesterritoires.fr/professions-juridiques",
            "https://www.banquedesterritoires.fr/entreprises",
            "https://www.banquedesterritoires.fr/acteurs-financiers",
            "https://www.banquedesterritoires.fr/espace-partenaires-acteurs-territoire",
        ]
        return urls

    def compute_marketing_offre(self):
        try:
            self.df = self.df.withColumn(
                "Marketing_de_loffre",
                when(
                    (col(Fields.FIELDS.get("type")) == "action")
                    & (
                        (
                            (
                                col(Fields.FIELDS.get("type_page_type_de_contenu"))
                                == "offer"
                            )
                            & (col(Fields.FIELDS.get("tag_neva")) == "offre transverse")
                        )
                        | (
                            (
                                col(Fields.FIELDS.get("type_page_type_de_contenu"))
                                == "Offres"
                            )
                            & (col(Fields.FIELDS.get("tag_neva")) == "Produit")
                        )
                        | (
                            (
                                col(Fields.FIELDS.get("type_page_type_de_contenu"))
                                == "Page intermediaire"
                            )
                            & (
                                col(Fields.FIELDS.get("type_page_categorie_de_contenu"))
                                == "Produits et services"
                            )
                        )
                        | (
                            (
                                col(Fields.FIELDS.get("type_page_type_de_contenu"))
                                == "Page intermediaire"
                            )
                            & (
                                col(Fields.FIELDS.get("type_page_categorie_de_contenu"))
                                == "Perspectives"
                            )
                            & (col(Fields.FIELDS.get("tag_neva")) == "Perspectives")
                        )
                        | (
                            (
                                col(Fields.FIELDS.get("type_page_type_de_contenu"))
                                == "Articles"
                            )
                            & (
                                col(Fields.FIELDS.get("type_page_categorie_de_contenu"))
                                == "Perspectives"
                            )
                            & (col(Fields.FIELDS.get("tag_neva")) == "Perspectives")
                        )
                        | (
                            (
                                col(Fields.FIELDS.get("type_page_type_de_contenu"))
                                == "Appels à projet"
                            )
                            & (
                                col(Fields.FIELDS.get("type_page_categorie_de_contenu"))
                                == "Appel a projet"
                            )
                            & (col(Fields.FIELDS.get("tag_neva")) == "Appel a projet")
                        )
                        | (
                            (
                                col(Fields.FIELDS.get("type_page_type_de_contenu"))
                                == "Solutions"
                            )
                            & (
                                col(Fields.FIELDS.get("type_page_categorie_de_contenu"))
                                == "Solution"
                            )
                            & (col(Fields.FIELDS.get("tag_neva")) == "Solution")
                        )
                        | (
                            (
                                col(Fields.FIELDS.get("type_page_type_de_contenu"))
                                == "Solutions"
                            )
                            & (
                                col(Fields.FIELDS.get("type_page_categorie_de_contenu"))
                                == "Dispositifs nationaux"
                            )
                            & (col(Fields.FIELDS.get("tag_neva")) == "Programme")
                        )
                        | (
                            (
                                col(Fields.FIELDS.get("type_page_type_de_contenu"))
                                == "Besoin"
                            )
                            & (
                                col(
                                    Fields.FIELDS.get(
                                        "type_page_programme_gouvernemental"
                                    )
                                ).isNotNull()
                            )
                        )
                        | (
                            (
                                col(Fields.FIELDS.get("type_page_type_de_contenu"))
                                == "Page intermediaire"
                            )
                            & (
                                col(Fields.FIELDS.get("type_page_categorie_de_contenu"))
                                == "Dispositifs nationaux"
                            )
                            & (col(Fields.FIELDS.get("tag_neva")) == "Programme")
                        )
                        | (
                            (
                                col(Fields.FIELDS.get("type_page_type_de_contenu"))
                                == "Expériences"
                            )
                            & (
                                col(Fields.FIELDS.get("type_page_categorie_de_contenu"))
                                == "Realisation"
                            )
                            & (col(Fields.FIELDS.get("tag_neva")) == "Realisation")
                        )
                        | (col(Fields.FIELDS.get("url")).isin(self.set_urls()))
                    ),
                    1,
                ).otherwise(0),
            )
            return self
        except Exception as e:
            self.logger.error(e)

    def compute_localtis(self):
        try:
            self.df = self.df.withColumn(
                Fields.TYPE_PAGE.get("localtis"),
                when(
                    (
                        (
                            col(Fields.FIELDS.get("url")).like(
                                "https://www.banquedesterritoires.fr/%"
                            )
                        )
                        & (
                            (
                                col(
                                    Fields.FIELDS.get("type_page_type_de_contenu")
                                ).isin(["edition-localtis", "edition"])
                                | (
                                    col(Fields.FIELDS.get("tag_neva"))
                                    == "Actualite Localtis"
                                )
                            )
                            & (col(Fields.FIELDS.get("type")) == "action")
                        )
                    ),
                    1,
                ).otherwise(0),
            )
            return self
        except Exception as e:
            self.logger.error(e)

    def compute_nbrows_tps_passe_total(self):
        if not isinstance(self.df, DataFrame):
            raise TypeError("df nest pas un dataframe")

        windowSpec = Window.partitionBy(Fields.FIELDS.get("initial_visitor_id"))

        self.df = (
            self.df.withColumn(
                Fields.FIELDS.get("nb_rows"),
                count(Fields.FIELDS.get("id_visit")).over(windowSpec),
            )
            .withColumn(
                Fields.FIELDS.get("tps_passe_visit"),
                when(
                    col(Fields.FIELDS.get("timespent_pretty")).isNotNull(),
                    col(Fields.FIELDS.get("timespent")),
                ).otherwise(0),
            )
            .withColumn(
                Fields.FIELDS.get("tps_passe_total"),
                S(Fields.FIELDS.get("tps_passe_visit")).over(windowSpec),
            )
        )
        return self

    def tracking_detail(self):
        types = [Fields.FIELDS.get("value_action"), Fields.FIELDS.get("value_goal")]
        self.goalnames = Fields.GOALNAMES
        if not isinstance(self.df, DataFrame):
            raise TypeError("Expected a DataFrame")

        self.df = (
            self.df.withColumn(
                Fields.FIELDS.get("fil_directions_regionales"),
                when(
                    col(Fields.FIELDS.get("url"))
                    == "https://www.banquedesterritoires.fr/direction-regionale-votre-contact-en-auvergne-rhone-alpes",
                    "Auvergne-Rhône-Alpes",
                )
                .when(
                    col(Fields.FIELDS.get("url"))
                    == "https://www.banquedesterritoires.fr/direction-regionale-votre-contact-en-bourgogne-franche-comte",
                    "Bourgogne-Franche-Comté",
                )
                .when(
                    col(Fields.FIELDS.get("url"))
                    == "https://www.banquedesterritoires.fr/direction-regionale-votre-contact-en-bretagne",
                    "Bretagne",
                )
                .when(
                    col(Fields.FIELDS.get("url"))
                    == "https://www.banquedesterritoires.fr/direction-regionale-votre-contact-en-centre-val-de-loire",
                    "Centre-Val de Loire",
                )
                .when(
                    col(Fields.FIELDS.get("url"))
                    == "https://www.banquedesterritoires.fr/direction-regionale-votre-contact-en-corse",
                    "Corse",
                )
                .when(
                    col(Fields.FIELDS.get("url"))
                    == "https://www.banquedesterritoires.fr/direction-regionale-votre-contact-dans-le-grand-est",
                    "Grand Est",
                )
                .when(
                    col(Fields.FIELDS.get("url"))
                    == "https://www.banquedesterritoires.fr/direction-regionale-votre-contact-en-antilles-guyane",
                    "Antilles - Guyane",
                )
                .when(
                    col(Fields.FIELDS.get("url"))
                    == "https://www.banquedesterritoires.fr/direction-regionale-votre-contact-dans-les-hauts-de-france",
                    "Hauts-de-France",
                )
                .when(
                    col(Fields.FIELDS.get("url"))
                    == "https://www.banquedesterritoires.fr/direction-regionale-votre-contact-en-ile-de-france",
                    "Île-de-France",
                )
                .when(
                    col(Fields.FIELDS.get("url"))
                    == "https://www.banquedesterritoires.fr/direction-regionale-votre-contact-en-reunion-ocean-indien",
                    "Île de la Réunion",
                )
                .when(
                    col(Fields.FIELDS.get("url"))
                    == "https://www.banquedesterritoires.fr/direction-regionale-votre-contact-en-normandie",
                    "Normandie",
                )
                .when(
                    col(Fields.FIELDS.get("url"))
                    == "https://www.banquedesterritoires.fr/direction-regionale-votre-contact-region-pacifique",
                    "Pacifique",
                )
                .when(
                    col(Fields.FIELDS.get("url"))
                    == "https://www.banquedesterritoires.fr/direction-regionale-votre-contact-en-nouvelle-aquitaine",
                    "Nouvelle-Aquitaine",
                )
                .when(
                    col(Fields.FIELDS.get("url"))
                    == "https://www.banquedesterritoires.fr/direction-regionale-votre-contact-en-occitanie",
                    "Occitanie",
                )
                .when(
                    col(Fields.FIELDS.get("url"))
                    == "https://www.banquedesterritoires.fr/direction-regionale-votre-contact-dans-les-pays-de-la-loire",
                    "Pays de la Loire",
                )
                .when(
                    col(Fields.FIELDS.get("url"))
                    == "https://www.banquedesterritoires.fr/direction-regionale-votre-contact-en-provence-alpes-cote-dazur",
                    "Provence-Alpes-Côte d'Azur",
                )
                .otherwise("Non défini"),
            )
            .withColumn(
                "Segments",
                when(
                    col(Fields.FIELDS.get("url"))
                    == "https://www.banquedesterritoires.fr/collectivites-locales",
                    "Collectivités locales",
                )
                .when(
                    col(Fields.FIELDS.get("url"))
                    == "https://www.banquedesterritoires.fr/entreprises-publiques-locales",
                    "Entreprises publiques locales",
                )
                .when(
                    col(Fields.FIELDS.get("url"))
                    == "https://www.banquedesterritoires.fr/habitat-social",
                    "Habitat social",
                )
                .when(
                    col(Fields.FIELDS.get("url"))
                    == "https://www.banquedesterritoires.fr/professions-juridiques",
                    "Professions juridiques",
                )
                .when(
                    col(Fields.FIELDS.get("url"))
                    == "https://www.banquedesterritoires.fr/entreprises",
                    "Entreprises",
                )
                .when(
                    col(Fields.FIELDS.get("url"))
                    == "https://www.banquedesterritoires.fr/acteurs-financiers",
                    "Acteurs financiers",
                )
                .when(
                    col(Fields.FIELDS.get("url"))
                    == "https://www.banquedesterritoires.fr/espace-partenaires-acteurs-territoire",
                    "Partenaires",
                ),
            )
            .withColumn(
                Fields.FIELDS.get("visiteur_engage"),
                when(
                    (col(Fields.FIELDS.get("type")) == types[1])
                    & (col(Fields.FIELDS.get("goal_name")) == "Demande de contact"),
                    col(Fields.FIELDS.get("id_visit")),
                ),
            )
            .withColumn(
                Fields.FIELDS.get("nb_entree"),
                when(col(Fields.FIELDS.get("page_view_position")) == 1, 1).otherwise(0),
            )
            .withColumn(
                Fields.FIELDS.get("nb_page_vue"),
                when(col(Fields.FIELDS.get("type")) == types[0], 1).otherwise(0),
            )
            .withColumn(
                Fields.FIELDS.get("visit_type_leads_not_sel"),
                when(
                    (col(Fields.FIELDS.get("type")) == types[1])
                    & (~col(Fields.FIELDS.get("goal_name")).isin(self.goalnames)),
                    col(Fields.FIELDS.get("id_visit")),
                ),
            )
            .withColumn(
                Fields.FIELDS.get("visit_type_leads_dmd_pret"),
                when(
                    (col(Fields.FIELDS.get("type")) == types[1])
                    & (
                        col(Fields.FIELDS.get("goal_name"))
                        == "FE- Parcours de demande de prets"
                    ),
                    col(Fields.FIELDS.get("id_visit")),
                ),
            )
            .withColumn(
                Fields.FIELDS.get("visit_type_leads_abo_localtis_quotidien"),
                when(
                    (col(Fields.FIELDS.get("type")) == types[1])
                    & (
                        col(Fields.FIELDS.get("goal_name"))
                        == "Abonnement Localtis Quotidienne"
                    ),
                    col(Fields.FIELDS.get("id_visit")),
                ),
            )
            .withColumn(
                Fields.FIELDS.get("visit_type_leads_abo_localtis_hebdo"),
                when(
                    (col(Fields.FIELDS.get("type")) == types[1])
                    & (
                        col(Fields.FIELDS.get("goal_name"))
                        == "Abonnement Localtis Hebdomadaire"
                    ),
                    col(Fields.FIELDS.get("id_visit")),
                ),
            )
            .withColumn(
                Fields.FIELDS.get("visit_type_leads_crea_compte"),
                when(
                    (col(Fields.FIELDS.get("type")) == types[1])
                    & (col(Fields.FIELDS.get("goal_name")) == "User Account Creation"),
                    col(Fields.FIELDS.get("id_visit")),
                ),
            )
            .withColumn(
                Fields.FIELDS.get("visit_type_leads_dmd_contact"),
                when(
                    (col(Fields.FIELDS.get("type")) == types[1])
                    & (col(Fields.FIELDS.get("goal_name")) == "Demande de contact"),
                    col(Fields.FIELDS.get("id_visit")),
                ),
            )
            .withColumn(
                Fields.FIELDS.get("ind_pa_audience_exposee"),
                when(
                    (col(Fields.FIELDS.get("type")) == types[0])
                    & (
                        col(Fields.FIELDS.get("tag_neva")).isin(
                            [
                                "Programme",
                                "Offre",
                                "Solution",
                                "Plan de relance",
                                "AMI",
                                "Appel a projet",
                                "Realisation",
                                "Perspectives",
                                "Thematique prioritaire",
                            ]
                        )
                    ),
                    col(Fields.FIELDS.get("id_visit")),
                ),
            )
            .withColumn(
                Fields.FIELDS.get("ind_pa_nb_leads"),
                when(
                    (col(Fields.FIELDS.get("type")) == types[1])
                    & (col(Fields.FIELDS.get("goal_name")).isin(self.goalnames)),
                    1,
                ).otherwise(0),
            )
            .withColumn(
                Fields.FIELDS.get("type_site"),
                when(
                    (col(Fields.FIELDS.get("type")) == types[0])
                    & (col(Fields.FIELDS.get("url")).like(f"{self.URL_BDT}%")),
                    "Espace Public",
                )
                .when(
                    (col(Fields.FIELDS.get("type")) == types[0])
                    & (
                        (
                            col(Fields.FIELDS.get("url")).like(
                                "https://mon-compte.banquedesterritoires.fr%"
                            )
                        )
                        | (
                            col(Fields.FIELDS.get("url")).like(
                                "https://www.cdc-net.com%"
                            )
                        )
                        | (
                            col(Fields.FIELDS.get("url")).like(
                                "http://real.cdc-net.com%"
                            )
                        )
                        | (col(Fields.FIELDS.get("url")).like("http://realcdcnet%"))
                    ),
                    "Espace Client",
                )
                .otherwise("Autres sites"),
            )
        )
        self.compute_type_page()
        return self

    def add_column(self, column_name, condition):
        if not isinstance(self.df, DataFrame):
            raise TypeError("Expected a DataFrame")
        return self.df.withColumn(column_name, condition)

    def compute_tracking_indicator(self):
        if not isinstance(self.df, DataFrame):
            raise TypeError("Expected a DataFrame")

        self.df = self.add_column(
            Fields.FIELDS.get("nb_rebond"),
            when(
                (col(Fields.FIELDS.get("actions")) == 1)
                & (col(Fields.FIELDS.get("goal_conversions")) == 0),
                1,
            ).otherwise(0),
        )
        self.df = self.add_column(
            Fields.FIELDS.get("visiteur_connu"),
            when(
                col(Fields.FIELDS.get("visitor_type")) == "returning",
                col(Fields.FIELDS.get("initial_visitor_id")),
            ),
        )
        self.df = self.add_column(
            Fields.FIELDS.get("nb_entree_home"),
            when(
                (
                    col(Fields.FIELDS.get("page_title"))
                    == "Banque des territoires | Groupe Caisse Des dépôts"
                )
                & (col(Fields.FIELDS.get("nb_entree")) == 1),
                1,
            ).otherwise(0),
        )
        self.df = self.add_column(
            Fields.FIELDS.get("nb_rebond_home"),
            when(
                (
                    col(Fields.FIELDS.get("page_title"))
                    == "Banque des territoires | Groupe Caisse Des dépôts"
                )
                & (col(Fields.FIELDS.get("actions")) == 1)
                & (col(Fields.FIELDS.get("goal_conversions")) == 0),
                1,
            ).otherwise(0),
        )
        self.df = self.add_column(
            Fields.FIELDS.get("timespent_home"),
            when(
                col(Fields.FIELDS.get("page_title"))
                == "Banque des territoires | Groupe Caisse Des dépôts",
                col(Fields.FIELDS.get("timespent")),
            ),
        )
        self.df = self.add_column(
            Fields.FIELDS.get("page_vue_home"),
            when(
                col(Fields.FIELDS.get("page_title"))
                == "Banque des territoires | Groupe Caisse Des dépôts",
                col(Fields.FIELDS.get("nb_page_vue")),
            ),
        )
        self.df = self.add_column(
            Fields.FIELDS.get("visiteur_qualifie"),
            when(
                col(Fields.FIELDS.get("tps_passe_total")) > 60,
                col(Fields.FIELDS.get("initial_visitor_id")),
            ),
        )
        self.df = self.df.withColumn(
            Fields.FIELDS.get("visit_duration"),
            col(Fields.FIELDS.get("visit_duration"))
            / col(Fields.FIELDS.get("nb_rows")),
        )

        return self

    def compute_matomo_kpi(self):
        if not isinstance(self.df, DataFrame):
            raise TypeError(
                "Expected a DataFrame, Please check your data type or ensure a DataFrame is passed"
            )
        self.df = (
            self.df.groupby(Fields.KPI_FIELDS_TRACKING)
            .agg(
                S(Fields.FIELDS.get("ind_pa_nb_leads")).alias(
                    Fields.FIELDS.get("nb_leads")
                ),
                countDistinct(Fields.FIELDS.get("ind_pa_audience_exposee")).alias(
                    "nb_Audience_Exposee"
                ),
                S(Fields.FIELDS.get("timespent")).alias("TpsPasse"),
                count(Fields.FIELDS.get("visit_type_leads_dmd_contact")).alias(
                    "nb_Visit_type_leads_Dmd_contact"
                ),
                count(Fields.FIELDS.get("visit_type_leads_crea_compte")).alias(
                    "nb_Visit_type_leads_CreaCompte"
                ),
                count(Fields.FIELDS.get("visit_type_leads_abo_localtis_hebdo")).alias(
                    "nb_Visit_type_leads_AboLocH"
                ),
                count(
                    Fields.FIELDS.get("visit_type_leads_abo_localtis_quotidien")
                ).alias("nb_Visit_type_leads_AboLocQ"),
                count(Fields.FIELDS.get("visit_type_leads_dmd_pret")).alias(
                    "nb_Visit_type_leads_Dmd_pret"
                ),
                count(Fields.FIELDS.get("visit_type_leads_not_sel")).alias(
                    "nb_Visit_type_leads_NotSel"
                ),
                S(Fields.FIELDS.get("nb_page_vue")).alias("NbPageVues"),
                S(Fields.FIELDS.get("page_vue_home")).alias("NbPageVuesHome"),
                S(Fields.FIELDS.get("timespent_home")).alias("TpsPasseHome"),
                S(Fields.FIELDS.get("nb_entree")).alias("nbentree"),
                S(Fields.FIELDS.get("nb_entree_home")).alias("nb_entreehome"),
                S(Fields.FIELDS.get("visit_duration")).alias("SumVisitDuration"),
                S(Fields.TYPE_PAGE.get("marketing_de_loffre")).alias(
                    "nb_page_vu_Marketing_de_loffre"
                ),
                S(Fields.TYPE_PAGE.get("localtis")).alias("nb_page_vu_Localtis"),
            )
            .select(
                *Fields.KPI_FIELDS_TRACKING,
                "nb_Leads",
                "nb_Audience_Exposee",
                "TpsPasse",
                "nb_Visit_type_leads_Dmd_contact",
                "nb_Visit_type_leads_CreaCompte",
                "nb_Visit_type_leads_AboLocH",
                "nb_Visit_type_leads_AboLocQ",
                "nb_Visit_type_leads_Dmd_pret",
                "nb_Visit_type_leads_NotSel",
                "NbPageVues",
                "NbPageVuesHome",
                "TpsPasseHome",
                "nbentree",
                "nb_entreehome",
                "SumVisitDuration",
                "nb_page_vu_Marketing_de_loffre",
                "nb_page_vu_Localtis",
            )
        )
        return self.df

    def compute_type_page(self):
        if self.df is not None:
            try:
                self.compute_marketing_offre().compute_localtis()
                return self
            except Exception as e:
                self.logger.error(f"error orrcured: {e}")
                raise

    def compute_tracking_url_indicator(self):
        types = [Fields.FIELDS.get("value_action"), Fields.FIELDS.get("value_goal")]
        self.goalnames = Fields.GOALNAMES
        if not isinstance(self.df, DataFrame):
            raise TypeError("df nest pas un dataframe")

        self.df = self.add_column(
            Fields.FIELDS.get("nb_rebond"),
            when(
                (col(Fields.FIELDS.get("actions")) == 1)
                & (col(Fields.FIELDS.get("goal_conversions")) == 0),
                1,
            ).otherwise(0),
        )
        self.df = self.add_column(
            Fields.FIELDS.get("ind_pa_nb_leads"),
            when(
                (col(Fields.FIELDS.get("type")) == types[1])
                & (col(Fields.FIELDS.get("goal_name")).isin(self.goalnames)),
                1,
            ).otherwise(0),
        )
        self.df = self.add_column(
            Fields.FIELDS.get("is_second_page"),
            when(
                (col(Fields.FIELDS.get("page_view_position")) == 2)
                & (
                    col(Fields.FIELDS.get("url"))
                    != "https://www.banquedesterritoires.fr/"
                ),
                1,
            ).otherwise(0),
        )
        self.df = self.add_column(
            Fields.FIELDS.get("nb_page_vue"),
            when(col(Fields.FIELDS.get("type")) == types[0], 1).otherwise(0),
        )

        source_traffic_condition = (
            when(
                col(Fields.FIELDS.get("referrer_type_name")) == "Search Engines",
                "Moteurs de recherche (SEO)",
            )
            .when(
                col(Fields.FIELDS.get("referrer_type_name")) == "Social Networks",
                "Réseaux sociaux (organiques)",
            )
            .when(
                col(Fields.FIELDS.get("referrer_type_name")) == "Direct Entry",
                "Accès Direct",
            )
            .when(
                col(Fields.FIELDS.get("referrer_type_name")) == "Websites",
                "Sites référents",
            )
            .when(
                (col(Fields.FIELDS.get("referrer_type_name")) == "Campaigns")
                & ~(col(Fields.FIELDS.get("campaign_name")).like("ACS%"))
                & ~(col(Fields.FIELDS.get("campaign_name")).like("CMP%"))
                & ~(col(Fields.FIELDS.get("campaign_name")).like("newsletter%")),
                "Campagnes (avec tracking utm / mtm ) hors e-mailing",
            )
            .when(
                (col(Fields.FIELDS.get("referrer_type_name")) == "Campaigns")
                & (
                    (col(Fields.FIELDS.get("campaign_name")).like("ACS%"))
                    | (col(Fields.FIELDS.get("campaign_name")).like("CMP%"))
                ),
                "Campagnes e-mailing",
            )
            .otherwise("Autres canaux")
            .alias("SourceTraffic")
        )

        self.df = self.add_column(
            Fields.FIELDS.get("source_traffic"), source_traffic_condition
        )

        self.compute_type_page()
        return self

    def compute_tracking_url_kpi(self) -> DataFrame:
        if not isinstance(self.df, DataFrame):
            raise TypeError("df nest pas un dataframe")

        self.df = self.df.groupBy(Fields.KPI_FIELDS_TRACKING_URL).agg(
            S(Fields.FIELDS.get("ind_pa_nb_leads")).alias("Total_Leads"),
            # Total_Page_Views (SUM of nb_page_vue)
            S(Fields.FIELDS.get("nb_page_vue")).alias("Total_Page_Views"),
            # Total_Rebounds (count of distinct idvisit where NbRebond = 1)
            countDistinct(
                when(
                    col(Fields.FIELDS.get("nb_rebond")) == 1,
                    col(Fields.FIELDS.get("id_visit")),
                )
            ).alias("Total_Rebounds"),
            # nb_visites (count of idvisit)
            countDistinct(Fields.FIELDS.get("id_visit")).alias("nb_visites"),
            # nb_visiteurs (count of distinct idvisit)
            countDistinct(Fields.FIELDS.get("initial_visitor_id")).alias(
                "nb_visiteurs"
            ),
            # TimespentTotal (average of timespent)
            avg(Fields.FIELDS.get("timespent")).alias("TimespentTotal"),
            # nb_entree (count of distinct idvisit where pageviewposition = 1)
            countDistinct(
                when(
                    col(Fields.FIELDS.get("page_view_position")) == 1,
                    col(Fields.FIELDS.get("id_visit")),
                )
            ).alias("nb_entree"),
        )

        return self.df

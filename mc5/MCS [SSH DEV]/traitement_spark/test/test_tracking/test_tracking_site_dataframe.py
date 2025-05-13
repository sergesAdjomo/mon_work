 
from unittest.mock import Mock

import pytest
import tracking_site.tracking_site_fields as Fields
from tracking_site.tracking_site import TraitementTrackingSiteDataFrame



class TestTrackingSiteDataFrameTestCase:

    def test_compute_marketing_offre(self, spark_session):
        # Given
        data = [
            (
                "id1",
                "action",
                "offer",
                "",
                "offre transverse",
                None,
                "https://www.banquedesterritoires.fr",
            ),
            (
                "id1",
                "action",
                "Offres",
                "",
                "Produit",
                None,
                "https://www.banquedesterritoires.fr",
            ),
            (
                "id1",
                "action",
                "Page intermediaire",
                "Produits et services",
                "",
                None,
                "https://www.banquedesterritoires.fr",
            ),
            (
                "id1",
                "action",
                "Page intermediaire",
                "Perspectives",
                "Perspectives",
                None,
                "https://www.banquedesterritoires.fr",
            ),
            (
                "id1",
                "action",
                "Articles",
                "Perspectives",
                "Perspectives",
                None,
                "https://www.banquedesterritoires.fr",
            ),
            (
                "id1",
                "action",
                "Appels à projet",
                "Appel a projet",
                "Appel a projet",
                None,
                "https://www.banquedesterritoires.fr",
            ),
            (
                "id1",
                "action",
                "Solutions",
                "Solution",
                "Solution",
                None,
                "https://www.banquedesterritoires.fr",
            ),
            (
                "id1",
                "action",
                "Solutions",
                "Dispositifs nationaux",
                "Programme",
                None,
                "https://www.banquedesterritoires.fr",
            ),
            (
                "id1",
                "action",
                "Besoin",
                "",
                "",
                "a_programme",
                "https://www.banquedesterritoires.fr",
            ),
            (
                "id1",
                "action",
                "Page intermediaire",
                "Dispositifs nationaux",
                "Programme",
                None,
                "https://www.banquedesterritoires.fr",
            ),
            (
                "id1",
                "action",
                "",
                "",
                "",
                None,
                "https://www.banquedesterritoires.fr/collectivites-locales",
            ),
            (
                "id1",
                "action",
                "",
                "",
                "",
                None,
                "https://www.banquedesterritoires.fr/entreprises-publiques-locales",
            ),
            (
                "id1",
                "action",
                "",
                "",
                "",
                None,
                "https://www.banquedesterritoires.fr/habitat-social",
            ),
            (
                "id1",
                "action",
                "",
                "",
                "",
                None,
                "https://www.banquedesterritoires.fr/professions-juridiques",
            ),
            (
                "id1",
                "action",
                "",
                "",
                "",
                None,
                "https://www.banquedesterritoires.fr/entreprises",
            ),
            (
                "id1",
                "action",
                "",
                "",
                "",
                None,
                "https://www.banquedesterritoires.fr/acteurs-financiers",
            ),
            (
                "id1",
                "action",
                "",
                "",
                "",
                None,
                "https://www.banquedesterritoires.fr/espace-partenaires-acteurs-territoire",
            ),
            ("id1", "action", "", "", "", None, "https://www.banquedesterritoires.fr"),
            (
                "id1",
                "goal",
                "Page intermediaire",
                "Dispositifs nationaux",
                "Programme",
                None,
                "https://www.banquedesterritoires.fr",
            ),
            (
                "id1",
                "action",
                "Solutions",
                "",
                "Appel à projet",
                None,
                "https://www.banquedesterritoires.fr",
            ),
            (
                "id1",
                "action",
                "Besoin",
                "",
                "",
                None,
                "https://www.banquedesterritoires.fr",
            ),
        ]

        expected_data = [
            (
                "id1",
                "action",
                "offer",
                "",
                "offre transverse",
                None,
                "https://www.banquedesterritoires.fr",
                1,
            ),
            (
                "id1",
                "action",
                "Offres",
                "",
                "Produit",
                None,
                "https://www.banquedesterritoires.fr",
                1,
            ),
            (
                "id1",
                "action",
                "Page intermediaire",
                "Produits et services",
                "",
                None,
                "https://www.banquedesterritoires.fr",
                1,
            ),
            (
                "id1",
                "action",
                "Page intermediaire",
                "Perspectives",
                "Perspectives",
                None,
                "https://www.banquedesterritoires.fr",
                1,
            ),
            (
                "id1",
                "action",
                "Articles",
                "Perspectives",
                "Perspectives",
                None,
                "https://www.banquedesterritoires.fr",
                1,
            ),
            (
                "id1",
                "action",
                "Appels à projet",
                "Appel a projet",
                "Appel a projet",
                None,
                "https://www.banquedesterritoires.fr",
                1,
            ),
            (
                "id1",
                "action",
                "Solutions",
                "Solution",
                "Solution",
                None,
                "https://www.banquedesterritoires.fr",
                1,
            ),
            (
                "id1",
                "action",
                "Solutions",
                "Dispositifs nationaux",
                "Programme",
                None,
                "https://www.banquedesterritoires.fr",
                1,
            ),
            (
                "id1",
                "action",
                "Besoin",
                "",
                "",
                "a_programme",
                "https://www.banquedesterritoires.fr",
                1,
            ),
            (
                "id1",
                "action",
                "Page intermediaire",
                "Dispositifs nationaux",
                "Programme",
                None,
                "https://www.banquedesterritoires.fr",
                1,
            ),
            (
                "id1",
                "action",
                "",
                "",
                "",
                None,
                "https://www.banquedesterritoires.fr/collectivites-locales",
                1,
            ),
            (
                "id1",
                "action",
                "",
                "",
                "",
                None,
                "https://www.banquedesterritoires.fr/entreprises-publiques-locales",
                1,
            ),
            (
                "id1",
                "action",
                "",
                "",
                "",
                None,
                "https://www.banquedesterritoires.fr/habitat-social",
                1,
            ),
            (
                "id1",
                "action",
                "",
                "",
                "",
                None,
                "https://www.banquedesterritoires.fr/professions-juridiques",
                1,
            ),
            (
                "id1",
                "action",
                "",
                "",
                "",
                None,
                "https://www.banquedesterritoires.fr/entreprises",
                1,
            ),
            (
                "id1",
                "action",
                "",
                "",
                "",
                None,
                "https://www.banquedesterritoires.fr/acteurs-financiers",
                1,
            ),
            (
                "id1",
                "action",
                "",
                "",
                "",
                None,
                "https://www.banquedesterritoires.fr/espace-partenaires-acteurs-territoire",
                1,
            ),
            (
                "id1",
                "action",
                "",
                "",
                "",
                None,
                "https://www.banquedesterritoires.fr",
                0,
            ),
            (
                "id1",
                "goal",
                "Page intermediaire",
                "Dispositifs nationaux",
                "Programme",
                None,
                "https://www.banquedesterritoires.fr",
                0,
            ),
            (
                "id1",
                "action",
                "Solutions",
                "",
                "Appel à projet",
                None,
                "https://www.banquedesterritoires.fr",
                0,
            ),
            (
                "id1",
                "action",
                "Besoin",
                "",
                "",
                None,
                "https://www.banquedesterritoires.fr",
                0,
            ),
        ]

        columns = [
            Fields.FIELDS.get("id_visit"),
            Fields.FIELDS.get("type"),
            Fields.FIELDS.get("type_page_type_de_contenu"),
            Fields.FIELDS.get("type_page_categorie_de_contenu"),
            Fields.FIELDS.get("tag_neva"),
            Fields.FIELDS.get("type_page_programme_gouvernemental"),
            Fields.FIELDS.get("url"),
        ]
        expected_columns = [
            Fields.FIELDS.get("id_visit"),
            Fields.FIELDS.get("type"),
            Fields.FIELDS.get("type_page_type_de_contenu"),
            Fields.FIELDS.get("type_page_categorie_de_contenu"),
            Fields.FIELDS.get("tag_neva"),
            Fields.FIELDS.get("type_page_programme_gouvernemental"),
            Fields.FIELDS.get("url"),
            Fields.TYPE_PAGE.get("marketing_de_loffre"),
        ]

        df = spark_session.spark.createDataFrame(data, columns)

        # When
        cut = TraitementTrackingSiteDataFrame(spark_session, config=Mock())
        cut.df = df
        result = cut.compute_marketing_offre()

        # Then
        expected_df = spark_session.spark.createDataFrame(
            expected_data, expected_columns
        )

        assert result.df.collect() == expected_df.collect()

    def test_compute_localtis(self, spark_session):
        # Given
        data = [
            (
                "id1",
                "action",
                "edition-localtis",
                "",
                "https://www.banquedesterritoires.fr/a_page",
            ),
            (
                "id1",
                "action",
                "edition",
                "",
                "https://www.banquedesterritoires.fr/a_page",
            ),
            (
                "id1",
                "action",
                "",
                "Actualite Localtis",
                "https://www.banquedesterritoires.fr/a_page",
            ),
            (
                "id1",
                "goal",
                "edition-localtis",
                "Actualite Localtis",
                "https://www.banquedesterritoires.fr/a_page",
            ),
            (
                "id1",
                "action",
                "edition-localtis",
                "Actualite Localtis",
                "https://www.some_page",
            ),
        ]

        expected_data = [
            (
                "id1",
                "action",
                "edition-localtis",
                "",
                "https://www.banquedesterritoires.fr/a_page",
                1,
            ),
            (
                "id1",
                "action",
                "edition",
                "",
                "https://www.banquedesterritoires.fr/a_page",
                1,
            ),
            (
                "id1",
                "action",
                "",
                "Actualite Localtis",
                "https://www.banquedesterritoires.fr/a_page",
                1,
            ),
            (
                "id1",
                "goal",
                "edition-localtis",
                "Actualite Localtis",
                "https://www.banquedesterritoires.fr/a_page",
                0,
            ),
            (
                "id1",
                "action",
                "edition-localtis",
                "Actualite Localtis",
                "https://www.some_page",
                0,
            ),
        ]

        columns = [
            Fields.FIELDS.get("id_visit"),
            Fields.FIELDS.get("type"),
            Fields.FIELDS.get("type_page_type_de_contenu"),
            Fields.FIELDS.get("tag_neva"), 
            Fields.FIELDS.get("url"),
        ]
        expected_columns = [
            Fields.FIELDS.get("id_visit"),
            Fields.FIELDS.get("type"),
            Fields.FIELDS.get("type_page_type_de_contenu"),
            Fields.FIELDS.get("tag_neva"), 
            Fields.FIELDS.get("url"),
            Fields.TYPE_PAGE.get("localtis"),
        ]

        df = spark_session.spark.createDataFrame(data, columns)

        # When
        cut = TraitementTrackingSiteDataFrame(spark_session, config=Mock())
        cut.df = df
        result = cut.compute_localtis()

        # Then
        expected_df = spark_session.spark.createDataFrame(
            expected_data, expected_columns
        )

        assert result.df.collect() == expected_df.collect()

    def test_compute_nb_rows_tps_passe_total(self, spark_session):
        # Given
        data = [
            ("id1", "visitor1", "42s", 42),
            ("id2", "visitor1", "10s", 10),
            ("id3", "visitor1", "1min40s", 100),
            ("id1", "visitor2", "25s", 25),
            ("id1", "visitor3", "10s", 10),
            ("id1", "visitor4", None, 10),
        ]

        expected_data = [
            ("id1", "visitor1", "42s", 42, 3, 42, 152),
            ("id2", "visitor1", "10s", 10, 3, 10, 152),
            ("id3", "visitor1", "1min40s", 100, 3, 100, 152),
            ("id1", "visitor2", "25s", 25, 1, 25, 25),
            ("id1", "visitor3", "10s", 10, 1, 10, 10),
            ("id1", "visitor4", None, 10, 1, 0, 0),
        ]
        columns = [
            Fields.FIELDS.get("id_visit"),
            Fields.FIELDS.get("initial_visitor_id"), 
            Fields.FIELDS.get("timespent_pretty"), 
            Fields.FIELDS.get("timespent"),
        ]
        expected_columns = [
            Fields.FIELDS.get("id_visit"),
            Fields.FIELDS.get("initial_visitor_id"), 
            Fields.FIELDS.get("timespent_pretty"), 
            Fields.FIELDS.get("timespent"),
            Fields.FIELDS.get("nb_rows"),
            Fields.FIELDS.get("tps_passe_visit"),
            Fields.FIELDS.get("tps_passe_total"),
        ]

        df = spark_session.spark.createDataFrame(data, columns)

        # When
        cut = TraitementTrackingSiteDataFrame(spark_session, config=Mock())
        cut.df = df
        result = cut.compute_nbrows_tps_passe_total()

        # Then
        expected_df = spark_session.spark.createDataFrame(
            expected_data, expected_columns
        )

        assert result.df.collect() == expected_df.collect()

    def test_tracking_detail(self, spark_session):
        # Given
        data = [
            (
                "id1",
                "action",
                "https://www.banquedesterritoires.fr/direction-regionale-votre-contact-en-auvergne-rhone-alpes",
                None,
                1,
                "Programme",
            ),
            (
                "id1",
                "goal",
                "https://www.banquedesterritoires.fr/collectivites-locales",
                "Demande de contact",
                4,
                None,
            ),
            (
                "id1",
                "goal",
                "https://www.cdc-net.com/a_page",
                "FE- Parcours de demande de prets",
                1,
                None,
            ),
            (
                "id1",
                "goal",
                "https://www.cdc-net.com/a_page",
                "Abonnement Localtis Quotidienne",
                1,
                None,
            ),
            (
                "id1",
                "goal",
                "https://www.cdc-net.com/a_page",
                "Abonnement Localtis Hebdomadaire",
                1,
                None,
            ),
            (
                "id1",
                "goal",
                "https://www.cdc-net.com/a_page",
                "User Account Creation",
                1,
                None,
            ),
            (
                "id1",
                "goal",
                "https://www.cdc-net.com/a_page",
                "Another Goal Name",
                1,
                None,
            ),
            (
                "id1",
                "action",
                "https://www.cdc-net.com/a_page",
                "FE- Parcours de demande de prets",
                1,
                None,
            ),
        ]

        expected_data = [
            (
                "id1",
                "action",
                "https://www.banquedesterritoires.fr/direction-regionale-votre-contact-en-auvergne-rhone-alpes",
                None,
                1,
                "Programme",
                "Auvergne-Rhône-Alpes",
                None,
                None,
                1,
                1,
                None,
                None,
                None,
                None,
                None,
                None,
                "id1",
                0,
                "Espace Public",
            ),
            (
                "id1",
                "goal",
                "https://www.banquedesterritoires.fr/collectivites-locales",
                "Demande de contact",
                4,
                None,
                "Non défini",
                "Collectivités locales",
                "id1",
                0,
                0,
                None,
                None,
                None,
                None,
                None,
                "id1",
                None,
                1,
                "Autres sites",
            ),
            (
                "id1",
                "goal",
                "https://www.cdc-net.com/a_page",
                "FE- Parcours de demande de prets",
                1,
                None,
                "Non défini",
                None,
                None,
                1,
                0,
                None,
                "id1",
                None,
                None,
                None,
                None,
                None,
                1,
                "Autres sites",
            ),
            (
                "id1",
                "goal",
                "https://www.cdc-net.com/a_page",
                "Abonnement Localtis Quotidienne",
                1,
                None,
                "Non défini",
                None,
                None,
                1,
                0,
                None,
                None,
                "id1",
                None,
                None,
                None,
                None,
                1,
                "Autres sites",
            ),
            (
                "id1",
                "goal",
                "https://www.cdc-net.com/a_page",
                "Abonnement Localtis Hebdomadaire",
                1,
                None,
                "Non défini",
                None,
                None,
                1,
                0,
                None,
                None,
                None,
                "id1",
                None,
                None,
                None,
                1,
                "Autres sites",
            ),
            (
                "id1",
                "goal",
                "https://www.cdc-net.com/a_page",
                "User Account Creation",
                1,
                None,
                "Non défini",
                None,
                None,
                1,
                0,
                None,
                None,
                None,
                None,
                "id1",
                None,
                None,
                1,
                "Autres sites",
            ),
            (
                "id1",
                "goal",
                "https://www.cdc-net.com/a_page",
                "Another Goal Name",
                1,
                None,
                "Non défini",
                None,
                None,
                1,
                0,
                "id1",
                None,
                None,
                None,
                None,
                None,
                None,
                0,
                "Autres sites",
            ),
            (
                "id1",
                "action",
                "https://www.cdc-net.com/a_page",
                "FE- Parcours de demande de prets",
                1,
                None,
                "Non défini",
                None,
                None,
                1,
                1,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                0,
                "Espace Client",
            ),
        ]
        columns = [
            Fields.FIELDS.get("id_visit"),
            Fields.FIELDS.get("type"),
            Fields.FIELDS.get("url"),
            Fields.FIELDS.get("goal_name"),
            Fields.FIELDS.get("page_view_position"),
            Fields.FIELDS.get("tag_neva"),
        ]
        expected_columns = [
            Fields.FIELDS.get("id_visit"),
            Fields.FIELDS.get("type"),
            Fields.FIELDS.get("url"),
            Fields.FIELDS.get("goal_name"),
            Fields.FIELDS.get("page_view_position"),
            Fields.FIELDS.get("tag_neva"),
            Fields.FIELDS.get("fil_directions_regionales"),
            Fields.FIELDS.get("segments"),
            Fields.FIELDS.get("visiteur_engage"),
            Fields.FIELDS.get("nb_entree"),
            Fields.FIELDS.get("nb_page_vue"),
            Fields.FIELDS.get("visit_type_leads_not_sel"),
            Fields.FIELDS.get("visit_type_leads_dmd_pret"),
            Fields.FIELDS.get("visit_type_leads_abo_localtis_quotidien"),
            Fields.FIELDS.get("visit_type_leads_abo_localtis_hebdo"),
            Fields.FIELDS.get("visit_type_leads_crea_compte"),
            Fields.FIELDS.get("visit_type_leads_dmd_contact"),
            Fields.FIELDS.get("ind_pa_audience_exposee"),
            Fields.FIELDS.get("ind_pa_nb_leads"),
            Fields.FIELDS.get("type_site"),
        ]

        df = spark_session.spark.createDataFrame(data, columns)

        # When
        cut = TraitementTrackingSiteDataFrame(spark_session, config=Mock())
        cut.df = df
        cut.compute_type_page = Mock()
        result = cut.tracking_detail()

        # Then
        expected_df = spark_session.spark.createDataFrame(
            expected_data, expected_columns
        )

        cut.compute_type_page.assert_called_once()
        assert result.df.collect() == expected_df.collect()

    def test_compute_tracking_indicator(self, spark_session):
        # Given
        data = [
            (
                "visitor1",
                "new",
                1,
                0,
                "Banque des territoires | Groupe Caisse Des dépôts",
                1,
                30,
                2,
                30,
                30,
                2,
            ),
            ("visitor1", "returning", 3, 0, "Another Page", 4, 63, 3, 90, 63, 3),
        ]

        expected_data = [
            (
                "visitor1",
                "new",
                1,
                0,
                "Banque des territoires | Groupe Caisse Des dépôts",
                1,
                30,
                2,
                30,
                15.0,
                2,
                1,
                None,
                1,
                1,
                30,
                2,
                None,
            ),
            (
                "visitor1",
                "returning",
                3,
                0,
                "Another Page",
                4,
                63,
                3,
                90,
                21.0,
                3,
                0,
                "visitor1",
                0,
                0,
                None,
                None,
                "visitor1",
            ),
        ]

        columns = [
            Fields.FIELDS.get("initial_visitor_id"),
            Fields.FIELDS.get("visitor_type"),
            Fields.FIELDS.get("actions"),
            Fields.FIELDS.get("goal_conversions"),
            Fields.FIELDS.get("page_title"),
            Fields.FIELDS.get("nb_entree"),
            Fields.FIELDS.get("timespent"),
            Fields.FIELDS.get("nb_page_vue"),
            Fields.FIELDS.get("tps_passe_total"),
            Fields.FIELDS.get("visit_duration"),
            Fields.FIELDS.get("nb_rows"),
        ]
        expected_columns = [
            Fields.FIELDS.get("initial_visitor_id"),
            Fields.FIELDS.get("visitor_type"),
            Fields.FIELDS.get("actions"),
            Fields.FIELDS.get("goal_conversions"),
            Fields.FIELDS.get("page_title"),
            Fields.FIELDS.get("nb_entree"),
            Fields.FIELDS.get("timespent"),
            Fields.FIELDS.get("nb_page_vue"),
            Fields.FIELDS.get("tps_passe_total"),
            Fields.FIELDS.get("visit_duration"),
            Fields.FIELDS.get("nb_rows"),
            Fields.FIELDS.get("nb_rebond"),
            Fields.FIELDS.get("visiteur_connu"),
            Fields.FIELDS.get("nb_entree_home"),
            Fields.FIELDS.get("nb_rebond_home"),
            Fields.FIELDS.get("timespent_home"),
            Fields.FIELDS.get("page_vue_home"),
            Fields.FIELDS.get("visiteur_qualifie"),
        ]

        df = spark_session.spark.createDataFrame(data, columns)

        # When
        cut = TraitementTrackingSiteDataFrame(spark_session, config=Mock())
        cut.df = df
        # TODO : add add_column Mock and number of calls assertion
        # cut.add_column = MagicMock()
        result = cut.compute_tracking_indicator()

        # Then
        expected_df = spark_session.spark.createDataFrame(
            expected_data, expected_columns
        )

        # [cut.add_column.assert_called() for _ in range(0,6)]
        assert result.df.collect() == expected_df.collect()

    def test_compute_matomo_kpi(self, spark_session):
        # Given
        data = [
            (
                "visitor1",
                "id1",
                "2024-07-03",
                "campaign1",
                "direct",
                "visitor1",
                "visitor1",
                "visitor1",
                "Collectivités locales",
                1,
                0,
                "Espace Public",
                "Non défini",
                1,
                "id1",
                70,
                "id1",
                "id1",
                "id1",
                "id1",
                "id1",
                "id1",
                2,
                0,
                0,
                1,
                0,
                60,
                3,
                0,
            ),
            (
                "visitor1",
                "id1",
                "2024-07-03",
                "campaign1",
                "direct",
                "visitor1",
                "visitor1",
                "visitor1",
                "Collectivités locales",
                1,
                0,
                "Espace Public",
                "Non défini",
                1,
                "id1",
                34,
                None,
                None,
                "id1",
                None,
                None,
                None,
                5,
                1,
                20,
                1,
                1,
                95,
                0,
                1,
            ),
        ]

        expected_data = [
            (
                "visitor1",
                "id1",
                "2024-07-03",
                "campaign1",
                "direct",
                "visitor1",
                "visitor1",
                "visitor1",
                "Collectivités locales",
                1,
                0,
                "Espace Public",
                "Non défini",
                2,
                1,
                104,
                1,
                1,
                2,
                1,
                1,
                1,
                7,
                1,
                20,
                2,
                1,
                155,
                3,
                1,
            )
        ]

        columns = [
            Fields.FIELDS.get("initial_visitor_id"),
            Fields.FIELDS.get("id_visit"),
            Fields.FIELDS.get("xx_jour"),
            Fields.FIELDS.get("campaign_id"),
            Fields.FIELDS.get("referrer_type"),
            Fields.FIELDS.get("visiteur_qualifie"),
            Fields.FIELDS.get("visiteur_connu"),
            Fields.FIELDS.get("visiteur_engage"),
            Fields.FIELDS.get("segments"),
            Fields.FIELDS.get("nb_rebond"),
            Fields.FIELDS.get("nb_rebond_home"),
            Fields.FIELDS.get("type_site"),
            Fields.FIELDS.get("fil_directions_regionales"),
            Fields.FIELDS.get("ind_pa_nb_leads"),
            Fields.FIELDS.get("ind_pa_audience_exposee"),
            Fields.FIELDS.get("timespent"),
            Fields.FIELDS.get("visit_type_leads_dmd_contact"),
            Fields.FIELDS.get("visit_type_leads_crea_compte"),
            Fields.FIELDS.get("visit_type_leads_abo_localtis_hebdo"),
            Fields.FIELDS.get("visit_type_leads_abo_localtis_quotidien"),
            Fields.FIELDS.get("visit_type_leads_dmd_pret"),
            Fields.FIELDS.get("visit_type_leads_not_sel"),
            Fields.FIELDS.get("nb_page_vue"),
            Fields.FIELDS.get("page_vue_home"),
            Fields.FIELDS.get("timespent_home"),
            Fields.FIELDS.get("nb_entree"),
            Fields.FIELDS.get("nb_entree_home"),
            Fields.FIELDS.get("visit_duration"),
            Fields.TYPE_PAGE.get("marketing_de_loffre"),
            Fields.TYPE_PAGE.get("localtis"),
        ]
        expected_columns = [
            Fields.FIELDS.get("initial_visitor_id"),
            Fields.FIELDS.get("id_visit"),
            Fields.FIELDS.get("xx_jour"),
            Fields.FIELDS.get("campaign_id"),
            Fields.FIELDS.get("referrer_type"),
            Fields.FIELDS.get("visiteur_qualifie"),
            Fields.FIELDS.get("visiteur_connu"),
            Fields.FIELDS.get("visiteur_engage"),
            Fields.FIELDS.get("segments"),
            Fields.FIELDS.get("nb_rebond"),
            Fields.FIELDS.get("nb_rebond_home"),
            Fields.FIELDS.get("type_site"),
            Fields.FIELDS.get("fil_directions_regionales"),
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
            "nb_entree",
            "nb_entree_home",
            "SumVisitDuration",
            "nb_page_vu_Marketing_de_loffre",
            "nb_page_vu_Localtis",
        ]

        df = spark_session.spark.createDataFrame(data, columns)

        # When
        cut = TraitementTrackingSiteDataFrame(spark_session, config=Mock())
        cut.df = df
        result = cut.compute_matomo_kpi()

        # Then
        expected_df = spark_session.spark.createDataFrame(
            expected_data, expected_columns
        )

        assert result.collect() == expected_df.collect()

    def test_compute_tracking_url_indicator(self, spark_session):
        # Given
        data = [
            (
                "action",
                1,
                0,
                None,
                2,
                "https://www.banquedesterritoires.fr/a_page",
                "Search Engines",
                None,
            ),
            (
                "goal",
                3,
                1,
                "Demande de contact",
                1,
                "https://www.banquedesterritoires.fr/a_page",
                "Campaigns",
                "ACSaaaa",
            ),
            (
                "goal",
                3,
                1,
                "Demande de contact",
                1,
                "https://www.banquedesterritoires.fr/a_page",
                "Campaigns",
                "Autre campagne",
            ),
            (
                "goal",
                3,
                1,
                "Demande de contact",
                1,
                "https://www.banquedesterritoires.fr/a_page",
                "Another Type",
                None,
            ),
        ]

        expected_data = [
            (
                "action",
                1,
                0,
                None,
                2,
                "https://www.banquedesterritoires.fr/a_page",
                "Search Engines",
                None,
                1,
                0,
                1,
                1,
                "Moteurs de recherche (SEO)",
            ),
            (
                "goal",
                3,
                1,
                "Demande de contact",
                1,
                "https://www.banquedesterritoires.fr/a_page",
                "Campaigns",
                "ACSaaaa",
                0,
                1,
                0,
                0,
                "Campagnes e-mailing",
            ),
            (
                "goal",
                3,
                1,
                "Demande de contact",
                1,
                "https://www.banquedesterritoires.fr/a_page",
                "Campaigns",
                "Autre campagne",
                0,
                1,
                0,
                0,
                "Campagnes (avec tracking utm / mtm ) hors e-mailing",
            ),
            (
                "goal",
                3,
                1,
                "Demande de contact",
                1,
                "https://www.banquedesterritoires.fr/a_page",
                "Another Type",
                None,
                0,
                1,
                0,
                0,
                "Autres canaux",
            ),
        ]

        columns = [
            Fields.FIELDS.get("type"),
            Fields.FIELDS.get("actions"),
            Fields.FIELDS.get("goal_conversions"),
            Fields.FIELDS.get("goal_name"),
            Fields.FIELDS.get("page_view_position"),
            Fields.FIELDS.get("url"),
            Fields.FIELDS.get("referrer_type_name"),
            Fields.FIELDS.get("campaign_name"),
        ]
        expected_columns = [
            Fields.FIELDS.get("type"),
            Fields.FIELDS.get("actions"),
            Fields.FIELDS.get("goal_conversions"),
            Fields.FIELDS.get("goal_name"),
            Fields.FIELDS.get("page_view_position"),
            Fields.FIELDS.get("url"),
            Fields.FIELDS.get("referrer_type_name"),
            Fields.FIELDS.get("campaign_name"),
            Fields.FIELDS.get("nb_rebond"),
            Fields.FIELDS.get("ind_pa_nb_leads"),
            Fields.FIELDS.get("is_second_page"),
            Fields.FIELDS.get("nb_page_vue"),
            Fields.FIELDS.get("source_traffic"),
        ]

        df = spark_session.spark.createDataFrame(data, columns)

        # When
        cut = TraitementTrackingSiteDataFrame(spark_session, config=Mock())
        cut.df = df
        # TODO : add add_column Mock and number of calls assertion
        # cut.add_column = MagicMock()
        cut.compute_type_page = Mock()
        result = cut.compute_tracking_url_indicator()

        # Then
        expected_df = spark_session.spark.createDataFrame(
            expected_data, expected_columns
        )

        # [cut.add_column.assert_called() for _ in range(0,4)]
        cut.compute_type_page.assert_called_once()
        assert result.df.collect() == expected_df.collect()

    def test_compute_tracking_url_kpi(self, spark_session):
        # Given
        data = [
            (
                "2024-07-03",
                "https://www.banquedesterritoires.fr/a_page_localtis",
                0,
                "Solutions",
                1,
                0,
                "Réseaux sociaux (organiques)",
                "Social Networks",
                0,
                3,
                1,
                "id1",
                "visitor1",
                30,
                3,
            ),
            (
                "2024-07-03",
                "https://www.banquedesterritoires.fr/a_page_localtis",
                0,
                "Solutions",
                1,
                0,
                "Réseaux sociaux (organiques)",
                "Social Networks",
                2,
                2,
                1,
                "id2",
                "visitor1",
                45,
                1,
            ),
        ]

        expected_data = [
            (
                "2024-07-03",
                "https://www.banquedesterritoires.fr/a_page_localtis",
                0,
                "Solutions",
                1,
                0,
                "Réseaux sociaux (organiques)",
                "Social Networks",
                2,
                5,
                2,
                2,
                1,
                37.5,
                1,
            )
        ]

        columns = [
            Fields.FIELDS.get("xx_jour"),
            Fields.FIELDS.get("url"),
            Fields.FIELDS.get("is_second_page"),
            Fields.FIELDS.get("type_page_type_de_contenu"),
            Fields.TYPE_PAGE.get("localtis"),
            Fields.TYPE_PAGE.get("marketing_de_loffre"),
            Fields.FIELDS.get("source_traffic"),
            Fields.FIELDS.get("referrer_type_name"),
            Fields.FIELDS.get("ind_pa_nb_leads"),
            Fields.FIELDS.get("nb_page_vue"),
            Fields.FIELDS.get("nb_rebond"),
            Fields.FIELDS.get("id_visit"),
            Fields.FIELDS.get("initial_visitor_id"),
            Fields.FIELDS.get("timespent"),
            Fields.FIELDS.get("page_view_position"),
        ]
        expected_columns = [
            Fields.FIELDS.get("xx_jour"),
            Fields.FIELDS.get("url"),
            Fields.FIELDS.get("is_second_page"),
            Fields.FIELDS.get("type_page_type_de_contenu"),
            Fields.TYPE_PAGE.get("localtis"),
            Fields.TYPE_PAGE.get("marketing_de_loffre"),
            Fields.FIELDS.get("source_traffic"),
            Fields.FIELDS.get("referrer_type_name"),
            "Total_Leads",
            "Total_Page_Views",
            "Total_Rebounds",
            "nb_visites",
            "nb_visiteurs",
            "TimespentTotal",
            "nb_entree",
        ]

        df = spark_session.spark.createDataFrame(data, columns)

        # When
        cut = TraitementTrackingSiteDataFrame(spark_session, config=Mock())
        cut.df = df
        result = cut.compute_tracking_url_kpi()

        # Then
        expected_df = spark_session.spark.createDataFrame(
            expected_data, expected_columns
        )

        assert result.collect() == expected_df.collect()

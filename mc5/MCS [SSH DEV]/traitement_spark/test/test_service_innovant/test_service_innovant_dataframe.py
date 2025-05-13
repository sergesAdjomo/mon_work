 
from unittest.mock import Mock

import pytest
import service_innovant.service_innovant_fields as Fields
from service_innovant.service_innovant import \
    TraitementServiceInnovantDataFrame


class TestServiceInnovantDataFrameTestCase:

    def test_filter_service_innovant(self, spark_session):
        # Given
        data = [
            (
                "action",
                "PRIORENO - Accueil",
                "https://mon-compte.banquedesterritoires.fr/#/ext/rn1-bdt-wc-prioreno/",
            ),
            ("action", "PRIORENO - Accueil", "https://a_page"),
            (
                "goal",
                "PrioRéno LS - Accueil",
                "https://mon-compte.banquedesterritoires.fr/#/ext/rn2-bdt-wc-prioreno/accueil",
            ),
            (
                "action",
                "PrioRéno LS - Accueil",
                "https://mon-compte.banquedesterritoires.fr/#/ext/rn2-bdt-wc-prioreno/accueil",
            ),
            (
                "action",
                "France Foncier + : le portail national de recherche de terrain",
                "https://some_page",
            ),
            (
                "action",
                "some Title",
                "https://www.banquedesterritoires.fr/produits-services/services-digitaux/mon-comparateur-energetique",
            ),
            (
                "action",
                "some Title",
                "https://www.banquedesterritoires.fr/produits-services/services-digitaux/cartographie-des-structures-innovation-francaises",
            ),
            (
                "action",
                "some Title",
                "https://www.banquedesterritoires.fr/produits-services/services-digitaux/cartographie-lieu-innovation-francais",
            ),
             (
                 "action",
                 "France Foncier + : industrial%",
                 "https://some_page",
             ),

        ]

        expected_data = [
            (
                "action",
                "PRIORENO - Accueil",
                "https://mon-compte.banquedesterritoires.fr/#/ext/rn1-bdt-wc-prioreno/",
            ),
            (
                "action",
                "PrioRéno LS - Accueil",
                "https://mon-compte.banquedesterritoires.fr/#/ext/rn2-bdt-wc-prioreno/accueil",
            ),
            (
                "action",
                "France Foncier + : le portail national de recherche de terrain",
                "https://some_page",
            ),
            (
                "action",
                "some Title",
                "https://www.banquedesterritoires.fr/produits-services/services-digitaux/mon-comparateur-energetique",
            ),
            (
                "action",
                "some Title",
                "https://www.banquedesterritoires.fr/produits-services/services-digitaux/cartographie-des-structures-innovation-francaises",
            ),
            (
                "action",
                "some Title",
                "https://www.banquedesterritoires.fr/produits-services/services-digitaux/cartographie-lieu-innovation-francais",
            ),
             (
                 "action",
                 "France Foncier + : industrial%",
                 "https://some_page",
             ),
        ]

        columns = [
            Fields.FIELDS.get("type"),
            Fields.FIELDS.get("pagetitle"),
            Fields.FIELDS.get("url"),
        ]
        expected_columns = [
            Fields.FIELDS.get("type"),
            Fields.FIELDS.get("pagetitle"),
            Fields.FIELDS.get("url"),
        ]

        df = spark_session.spark.createDataFrame(data, columns)

        # When
        cut = TraitementServiceInnovantDataFrame(spark_session, config=Mock())
        cut.df = df
        result = cut.filter_service_innovant()

        # Then
        expected_df = spark_session.spark.createDataFrame(
            expected_data, expected_columns
        )

        assert result.service_innovant_df.collect() == expected_df.collect()

    def test_add_inno_columns(self, spark_session):
        # Given
        data = [
            (
                "a_title",
                "https://www.banquedesterritoires.fr/produits-services/services-digitaux/mon-comparateur-energetique",
            ),
            ("a_title", "https://a_url"),
            (
                "PRIORENO - Accueil",
                "https://mon-compte.banquedesterritoires.fr/#/ext/rn1-bdt-wc-prioreno/",
            ),
            ("PRIORENO - Accueil", "https://a_url"),
            (
                "a_title",
                "https://mon-compte.banquedesterritoires.fr/#/ext/rn1-bdt-wc-prioreno/",
            ),
            (
                "PrioRéno LS - Accueil",
                "https://mon-compte.banquedesterritoires.fr/#/ext/rn2-bdt-wc-prioreno/accueil",
            ),
            ("PrioRéno LS - Accueil", "https://a_url"),
            (
                "a_title",
                "https://mon-compte.banquedesterritoires.fr/#/ext/rn2-bdt-wc-prioreno/accueil",
            ),
            (
                "France Foncier + : le portail national de recherche de terrainaaaaaa",
                "https://a_url",
            ),
            (
                "a_title",
                "https://www.banquedesterritoires.fr/produits-services/services-digitaux/cartographie-des-structures-innovation-francaises",
            ),
            (
                "a_title",
                "https://www.banquedesterritoires.fr/produits-services/services-digitaux/cartographie-lieu-innovation-francais",
            ),
            (
                "France Foncier + : industrial%",
                "https://a_url",
            ),
        ]

        expected_data = [
            (
                "a_title",
                "https://www.banquedesterritoires.fr/produits-services/services-digitaux/mon-comparateur-energetique",
                "Mon Comparateur Energétique",
                "Services Innovant",
            ),
            ("a_title", "https://a_url", "Autres pages", "Services Innovant"),
            (
                "PRIORENO - Accueil",
                "https://mon-compte.banquedesterritoires.fr/#/ext/rn1-bdt-wc-prioreno/",
                "PrioRéno BP",
                "Services Innovant",
            ),
            (
                "PRIORENO - Accueil",
                "https://a_url",
                "Autres pages",
                "Services Innovant",
            ),
            (
                "a_title",
                "https://mon-compte.banquedesterritoires.fr/#/ext/rn1-bdt-wc-prioreno/",
                "Autres pages",
                "Services Innovant",
            ),
            (
                "PrioRéno LS - Accueil",
                "https://mon-compte.banquedesterritoires.fr/#/ext/rn2-bdt-wc-prioreno/accueil",
                "PrioRéno LS",
                "Services Innovant",
            ),
            (
                "PrioRéno LS - Accueil",
                "https://a_url",
                "Autres pages",
                "Services Innovant",
            ),
            (
                "a_title",
                "https://mon-compte.banquedesterritoires.fr/#/ext/rn2-bdt-wc-prioreno/accueil",
                "Autres pages",
                "Services Innovant",
            ),
            (
                "France Foncier + : le portail national de recherche de terrainaaaaaa",
                "https://a_url",
                "France Foncier +",
                "Services Innovant",
            ),
            (
                "a_title",
                "https://www.banquedesterritoires.fr/produits-services/services-digitaux/cartographie-des-structures-innovation-francaises",
                "Cartographie des structures d'innovation territoriales",
                "Services Innovant",
            ),
            (
                "a_title",
                "https://www.banquedesterritoires.fr/produits-services/services-digitaux/cartographie-lieu-innovation-francais",
                "Cartographie des structures d'innovation territoriales",
                "Services Innovant",
            ),
             (
                "France Foncier + : industrial%",
                "https://a_url",
                "France Foncier +",           
                "Services Innovant",

             ),
        ]

        columns = [
            Fields.FIELDS.get("pagetitle"),
            Fields.FIELDS.get("url"),
        ]
        expected_columns = [
            Fields.FIELDS.get("pagetitle"),
            Fields.FIELDS.get("url"),
            "Nom_Services",
            "Nom_Categorie",
        ]

        df = spark_session.spark.createDataFrame(data, columns)

        # When
        cut = TraitementServiceInnovantDataFrame(spark_session, config=Mock())
        cut.service_innovant_df = df
        result = cut.add_inno_columns()

        # Then
        expected_df = spark_session.spark.createDataFrame(
            expected_data, expected_columns
        )

        assert result.service_innovant_df.collect() == expected_df.collect()

    def test_filter_page_offres(self, spark_session):
        # Given
        data = [
            (
                "action",
                "https://www.banquedesterritoires.fr/produits-services/services-digitaux/dataviz-pvd",
            ),
            (
                "action",
                "https://www.banquedesterritoires.fr/revitalisation-petites-villes-demain/solutions",
            ),
            (
                "action",
                "https://www.banquedesterritoires.fr/produits-services/services-digitaux/dataviz-territoires-dindustrie",
            ),
            (
                "action",
                "https://www.banquedesterritoires.fr/produits-services/services-digitaux/prioreno-ponts",
            ),
            ("action", "https://www.banquedesterritoires.fr/renovation-ouvrages-d-art"),
            (
                "action",
                "https://www.banquedesterritoires.fr/produits-services/services-digitaux/offre-prioreno",
            ),
            (
                "action",
                "https://www.banquedesterritoires.fr/produits-services/services-digitaux/mon-diag-ecoles",
            ),
            (
                "action",
                "https://www.banquedesterritoires.fr/produits-services/services-digitaux",
            ),
            (
                "action",
                "https://www.banquedesterritoires.fr/produits-services/services-digitaux/service-prioreno-renovation-logements-sociaux",
            ),
            (
                "goal",
                "https://www.banquedesterritoires.fr/produits-services/services-digitaux/service-prioreno-renovation-logements-sociaux",
            ),
            ("action", "https://a_url"),
        ]

        expected_data = [
            (
                "action",
                "https://www.banquedesterritoires.fr/produits-services/services-digitaux/dataviz-pvd",
            ),
            (
                "action",
                "https://www.banquedesterritoires.fr/revitalisation-petites-villes-demain/solutions",
            ),
            (
                "action",
                "https://www.banquedesterritoires.fr/produits-services/services-digitaux/dataviz-territoires-dindustrie",
            ),
            (
                "action",
                "https://www.banquedesterritoires.fr/produits-services/services-digitaux/prioreno-ponts",
            ),
            ("action", "https://www.banquedesterritoires.fr/renovation-ouvrages-d-art"),
            (
                "action",
                "https://www.banquedesterritoires.fr/produits-services/services-digitaux/offre-prioreno",
            ),
            (
                "action",
                "https://www.banquedesterritoires.fr/produits-services/services-digitaux/mon-diag-ecoles",
            ),
            (
                "action",
                "https://www.banquedesterritoires.fr/produits-services/services-digitaux",
            ),
            (
                "action",
                "https://www.banquedesterritoires.fr/produits-services/services-digitaux/service-prioreno-renovation-logements-sociaux",
            ),
        ]

        columns = [
            Fields.FIELDS.get("type"),
            Fields.FIELDS.get("url"),
        ]
        expected_columns = [
            Fields.FIELDS.get("type"),
            Fields.FIELDS.get("url"),
        ]

        df = spark_session.spark.createDataFrame(data, columns)

        # When
        cut = TraitementServiceInnovantDataFrame(spark_session, config=Mock())
        cut.df = df
        result = cut.filter_page_offres()

        # Then
        expected_df = spark_session.spark.createDataFrame(
            expected_data, expected_columns
        )

        assert result.page_offres_df.collect() == expected_df.collect()

    def test_add_page_offres_columns(self, spark_session):
        # Given
        data = [
            (
                "id1",
                "https://www.banquedesterritoires.fr/produits-services/services-digitaux/dataviz-pvd",
            ),
            (
                "id1",
                "https://www.banquedesterritoires.fr/revitalisation-petites-villes-demain/solutions",
            ),
            (
                "id1",
                "https://www.banquedesterritoires.fr/produits-services/services-digitaux/dataviz-territoires-dindustrie",
            ),
            (
                "id1",
                "https://www.banquedesterritoires.fr/produits-services/services-digitaux/prioreno-ponts",
            ),
            ("id1", "https://www.banquedesterritoires.fr/renovation-ouvrages-d-art"),
            (
                "id1",
                "https://www.banquedesterritoires.fr/produits-services/services-digitaux/offre-prioreno",
            ),
            (
                "id1",
                "https://www.banquedesterritoires.fr/produits-services/services-digitaux/mon-diag-ecoles",
            ),
            (
                "id1",
                "https://www.banquedesterritoires.fr/produits-services/services-digitaux",
            ),
            (
                "id1",
                "https://www.banquedesterritoires.fr/produits-services/services-digitaux/service-prioreno-renovation-logements-sociaux",
            ),

            ("id1", "https://a_url"),
        ]

        expected_data = [
            (
                "id1",
                "https://www.banquedesterritoires.fr/produits-services/services-digitaux/dataviz-pvd",
                "Dataviz PVD",
                "Page Offres",
            ),
            (
                "id1",
                "https://www.banquedesterritoires.fr/revitalisation-petites-villes-demain/solutions",
                "Dataviz PVD",
                "Page Offres",
            ),
            (
                "id1",
                "https://www.banquedesterritoires.fr/produits-services/services-digitaux/dataviz-territoires-dindustrie",
                "Dataviz TI",
                "Page Offres",
            ),
            (
                "id1",
                "https://www.banquedesterritoires.fr/produits-services/services-digitaux/prioreno-ponts",
                "PrioReno Ponts",
                "Page Offres",
            ),
            (
                "id1",
                "https://www.banquedesterritoires.fr/renovation-ouvrages-d-art",
                "PrioReno Ponts",
                "Page Offres",
            ),
            (
                "id1",
                "https://www.banquedesterritoires.fr/produits-services/services-digitaux/offre-prioreno",
                "PrioRéno BP",
                "Page Offres",
            ),
            (
                "id1",
                "https://www.banquedesterritoires.fr/produits-services/services-digitaux/mon-diag-ecoles",
                "Mon Diag Ecoles",
                "Page Offres",
            ),
            (
                "id1",
                "https://www.banquedesterritoires.fr/produits-services/services-digitaux",
                "Page Produits - services digitaux",
                "Page Offres",
            ),
            (
                "id1",
                "https://www.banquedesterritoires.fr/produits-services/services-digitaux/service-prioreno-renovation-logements-sociaux",
                "PrioRéno LS",
                "Page Offres",
            ),

            ("id1", "https://a_url", "Other", "Page Offres"),
        ]

        columns = [
            Fields.FIELDS.get("idvisit"),
            Fields.FIELDS.get("url"),
        ]
        expected_columns = [
            Fields.FIELDS.get("idvisit"),
            Fields.FIELDS.get("url"),
            "Nom_Services",
            "Nom_Categorie",
        ]

        df = spark_session.spark.createDataFrame(data, columns)

        # When
        cut = TraitementServiceInnovantDataFrame(spark_session, config=Mock())
        cut.page_offres_df = df
        result = cut.add_page_offres_columns()

        # Then
        expected_df = spark_session.spark.createDataFrame(
            expected_data, expected_columns
        )

        assert result.page_offres_df.collect() == expected_df.collect()

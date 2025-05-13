 
from unittest.mock import Mock

import pytest
import qualification_ols.qualification_ols_fields as Fields
from qualification_ols.qualification_ols import \
    TraitementQualificationOLSDataFrame


class TestQualificationOLSDataFrameTestCase:

    def test_compute_df_with_priority_code_tiers(self, spark_session):
        # Given
        data = [
            (
                "siren1",
                "siret1",
                "denom1",
                "sous_cat1",
                1,
                "t1",
                "t2",
                None,
                None,
                "t1",
            ),
            (
                "siren2",
                "siret2",
                "denom2",
                "sous_cat2",
                0,
                None,
                "t3",
                None,
                None,
                "t2",
            ),
            (
                "siren3",
                "siret3",
                "denom3",
                "sous_cat3",
                1,
                None,
                None,
                "t4",
                "t3",
                None,
            ),
            (
                "siren4",
                "siret4",
                "denom4",
                "sous_cat4",
                0,
                None,
                None,
                None,
                None,
                None,
            ),
        ]
        expected_data = [
            ("siren1", "siret1", "t1", "denom1", "sous_cat1", 1),
            ("siren2", "siret2", "t3", "denom2", "sous_cat2", 0),
            ("siren3", "siret3", "t4", "denom3", "sous_cat3", 1),
            ("siren4", "siret4", None, "denom4", "sous_cat4", 0),
        ]

        columns = [
            "base_siren",
            "base_siret",
            Fields.FIELDS.get("denom_unite_legale"),
            Fields.FIELDS.get("sous_cat"),
            Fields.FIELDS.get("is_tete_groupe"),
            "t0_code_tiers",
            "t1_code_tiers",
            "t2_code_tiers",
            "t3_code_tiers",
            "t4_code_tiers",
        ]

        expected_columns = [
            Fields.FIELDS.get("siren"),
            Fields.FIELDS.get("siret"),
            Fields.FIELDS.get("code_tiers"),
            Fields.FIELDS.get("denom_unite_legale"),
            Fields.FIELDS.get("sous_cat"),
            Fields.FIELDS.get("is_tete_groupe"),
        ]

        df = spark_session.spark.createDataFrame(data, columns)

        # When
        cut = TraitementQualificationOLSDataFrame(spark_session, config=Mock())
        cut.df = df
        result = cut.compute_df_with_priority_code_tiers()

        # Then
        expected_df = spark_session.spark.createDataFrame(
            expected_data, expected_columns
        )

        assert result.df.collect() == expected_df.collect()

    def test_compute_siret_par_date(self, spark_session):
        # Given
        data = [
            ("siren1", "siret1", "2024-03-11 00:00:00"),
            ("siren1", "siret1", "2013-02-09 15:34:12"),
            ("siren1", "siret1", "2021-11-10 07:13:01"),
            ("siren1", "siret2", "2024-03-11 00:00:00"),
            ("siren2", "siret2", "2024-03-11 00:00:00"),
        ]

        expected_data = [
            ("siren1", "siret1", "2024-03-11 00:00:00", 1),
            ("siren1", "siret1", "2021-11-10 07:13:01", 2),
            ("siren1", "siret1", "2013-02-09 15:34:12", 3),
            ("siren1", "siret2", "2024-03-11 00:00:00", 1),
            ("siren2", "siret2", "2024-03-11 00:00:00", 1),
        ]

        columns = [
            Fields.FIELDS.get("siren"),
            Fields.FIELDS.get("siret"),
            Fields.FIELDS.get("dat_horodat"),
        ]
        expected_columns = [
            Fields.FIELDS.get("siren"),
            Fields.FIELDS.get("siret"),
            Fields.FIELDS.get("dat_horodat"),
            Fields.FIELDS.get("siret_par_date"),
        ]

        df = spark_session.spark.createDataFrame(data, columns)

        # When
        cut = TraitementQualificationOLSDataFrame(spark_session, config=Mock())
        cut.df = df
        result = cut.compute_siret_par_date()

        # Then
        expected_df = spark_session.spark.createDataFrame(
            expected_data, expected_columns
        )

        assert result.df.collect() == expected_df.collect()

    def test_compute_qualif_ols_dataframe(self, spark_session):
        # Given
        data = [
            (
                "siren1",
                "denom1",
                "sous_cat1",
                "Compiègne",
                "Hauts-de-France",
                "code1",
                0,
                1,
            ),
            (
                "siren2",
                "denom2",
                "sous_cat2",
                "Bagneux",
                "Hauts-de-Seine",
                "code2",
                1,
                1,
            ),
            (
                "siren3",
                "denom3",
                "sous_cat3",
                "Bagneux",
                "Hauts-de-Seine",
                "code3",
                1,
                3,
            ),
        ]

        expected_data = [
            (
                "siren1",
                "denom1",
                "sous_cat1",
                "Compiègne",
                "Hauts-de-France",
                "code1",
                0,
            ),
            ("siren2", "denom2", "sous_cat2", "Bagneux", "Hauts-de-Seine", "code2", 1),
        ]

        columns = [
            Fields.FIELDS.get("siren"),
            Fields.FIELDS.get("denom_unite_legale"),
            Fields.FIELDS.get("sous_cat"),
            Fields.FIELDS.get("lib_bureau_distrib"),
            Fields.FIELDS.get("lib_clair_region"),
            Fields.FIELDS.get("code_tiers"),
            Fields.FIELDS.get("is_tete_groupe"),
            Fields.FIELDS.get("siret_par_date"),
        ]
        expected_columns = [
            "siren",
            "Raison sociale",
            Fields.FIELDS.get("sous_cat"),
            "Ville",
            "Région",
            Fields.FIELDS.get("code_tiers"),
            "Tête_de_groupe",
        ]

        df = spark_session.spark.createDataFrame(data, columns)

        # When
        cut = TraitementQualificationOLSDataFrame(spark_session, config=Mock())
        cut.df = df
        result = cut.compute_qualif_ols_dataframe()

        # Then
        expected_df = spark_session.spark.createDataFrame(
            expected_data, expected_columns
        )

        assert result.df.collect() == expected_df.collect()

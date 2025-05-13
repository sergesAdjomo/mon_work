 
from unittest.mock import ANY, Mock

import campagne_marketing.campagne_marketing_fields as Fields
import pytest
from campagne_marketing.campagne_marketing import CampagneMarketingDataFrame
from pyspark.sql.functions import col


class TestCampagneMarketingDataFrameTestCase:

    def test_compute_adobe_indicator(self, spark_session):
        # Given
        data = [
            ("campaign1", 1, "id_delivery1", "id_client1", "libelle_url1"),
            ("campaign1", 0, "id_delivery1", "id_client1", "libelle_url2"),
            ("campaign1", None, "id_delivery1", "id_client1", "libelle_url3"),
            ("campaign1", 1, "id_delivery1", "id_client3", "libelle_url1"),
            ("campaign1", 1, "id_delivery2", "id_client1", "libelle_url1"),
            ("campaign1", 1, "id_delivery2", "id_client2", "libelle_url1"),
        ]

        expected_data = [
            ("campaign1", 1, "id_delivery1", "id_client1", "libelle_url1", "OUI", 3),
            ("campaign1", 0, "id_delivery1", "id_client1", "libelle_url2", "NON", 3),
            ("campaign1", None, "id_delivery1", "id_client1", "libelle_url3", None, 3),
            ("campaign1", 1, "id_delivery1", "id_client3", "libelle_url1", "OUI", 1),
            ("campaign1", 1, "id_delivery2", "id_client1", "libelle_url1", "OUI", 1),
            ("campaign1", 1, "id_delivery2", "id_client2", "libelle_url1", "OUI", 1),
        ]

        columns = [
            Fields.JOINT_KEYS.get("id_campagne"),
            Fields.FIELDS.get("itempsfort"),
            Fields.FIELDS.get("id_delivery"),
            Fields.FIELDS.get("hub_id_client"),
            Fields.FIELDS.get("libelle_url"),
        ]
        expected_columns = [
            Fields.JOINT_KEYS.get("id_campagne"),
            Fields.FIELDS.get("itempsfort"),
            Fields.FIELDS.get("id_delivery"),
            Fields.FIELDS.get("hub_id_client"),
            Fields.FIELDS.get("libelle_url"),
            Fields.FIELDS.get("libelle_temps_fort_operation"),
            Fields.FIELDS.get("nbRows"),
        ]

        df = spark_session.spark.createDataFrame(data, columns)

        # When
        cut = CampagneMarketingDataFrame(spark_session, config=Mock())
        cut.df = df
        result = cut.compute_adobe_indicator()

        # Then
        expected_df = spark_session.spark.createDataFrame(
            expected_data, expected_columns
        )

        assert result.df.collect() == expected_df.collect()

    def test_compute_adobe_kpi(self, spark_session):
        # Given
        data = [
            (
                "op1",
                "lib_op1",
                "typ_1",
                "2024-07-03",
                1,
                "id_del1",
                "lib_del1",
                "id_cl1",
                "fonct1",
                "code_seg1",
                "lib_seg1",
                "reg1",
                "OUI",
                "lib_url1",
                1,
                3,
                0,
            ),
            (
                "op2",
                "lib_op2",
                "typ_2",
                "2024-07-03",
                0,
                "id_del2",
                "lib_del2",
                "id_cl2",
                "fonct2",
                "code_seg2",
                "lib_seg2",
                "reg2",
                "NON",
                "lib_url2",
                3,
                1,
                0,
            ),
            (
                "op1",
                "lib_op1",
                "typ_1",
                "2024-07-03",
                1,
                "id_del1",
                "lib_del1",
                "id_cl1",
                "fonct1",
                "code_seg1",
                "lib_seg1",
                "reg1",
                "OUI",
                "lib_url1",
                2,
                3,
                0,
            ),
            (
                "op1",
                "lib_op1",
                "typ_1",
                "2024-07-03",
                1,
                "id_del1",
                "lib_del1",
                "id_cl1",
                "fonct1",
                "code_seg1",
                "lib_seg1",
                "reg1",
                "OUI",
                "lib_url1",
                1,
                3,
                1,
            ),
        ]

        expected_data = [
            (
                "op1",
                "lib_op1",
                "typ_1",
                "2024-07-03",
                1,
                "id_del1",
                "lib_del1",
                "id_cl1",
                "fonct1",
                "code_seg1",
                "lib_seg1",
                "reg1",
                "OUI",
                "lib_url1",
                1,
                1/3,
                1/3,
                2/3,
                0.0,
                0.0,
            ),
            (
                "op2",
                "lib_op2",
                "typ_2",
                "2024-07-03",
                0,
                "id_del2",
                "lib_del2",
                "id_cl2",
                "fonct2",
                "code_seg2",
                "lib_seg2",
                "reg2",
                "NON",
                "lib_url2",
                3,
                1.0,
                1.0,
                0.0,
                0.0,
                1.0,
            ),
            (
                "op1",
                "lib_op1",
                "typ_1",
                "2024-07-03",
                1,
                "id_del1",
                "lib_del1",
                "id_cl1",
                "fonct1",
                "code_seg1",
                "lib_seg1",
                "reg1",
                "OUI",
                "lib_url1",
                2,
                1/3,
                1/3,
                0.0,
                1/3,
                0.0,
            ),
        ]

        columns = [
            Fields.FIELDS.get("nom_operation"),
            Fields.FIELDS.get("libelle_operation"),
            Fields.FIELDS.get("typologie_operation"),
            Fields.FIELDS.get("date_creation"),
            Fields.FIELDS.get("itempsfort"),
            Fields.FIELDS.get("id_delivery"),
            Fields.FIELDS.get("sinternalname"),
            Fields.FIELDS.get("hub_id_client"),
            Fields.FIELDS.get("libelle_fonction"),
            Fields.FIELDS.get("code_segment"),
            Fields.FIELDS.get("libelle_segment"),
            Fields.FIELDS.get("nom_region"),
            Fields.FIELDS.get("libelle_temps_fort_operation"),
            Fields.FIELDS.get("libelle_url"),
            Fields.FIELDS.get("type_de_click"),
            Fields.FIELDS.get("nbRows"),
            Fields.FIELDS.get("raison_echec"),
        ]

        expected_columns = [
            "code_operation",
            "label_operation",
            "Typologie_operation",
            "date_diffusion",
            "temps_fort_operation",
            "id_diffusion",
            "label_diffusion",
            "id_destinataire",
            "fonction_destinataire",
            "code_segment_destinataire",
            "segment_destinataire",
            "region_destinataire",
            "libelle_temps_fort_operation",
            "libelle_url",
            "type_de_click",
            "itodeliver",
            "volume_emails_delivres",
            "volume_ouverture",
            "volume_click",
            "volume_desinscription",
        ]

        df = spark_session.spark.createDataFrame(data, columns)

        # When
        cut = CampagneMarketingDataFrame(spark_session, config=Mock())
        cut.df = df
        result = cut.compute_adobe_kpi()

        # Then
        expected_df = spark_session.spark.createDataFrame(
            expected_data, expected_columns
        )

        assert result.df.collect() == expected_df.collect()

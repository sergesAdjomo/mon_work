 
from unittest.mock import ANY, MagicMock, Mock

import pytest
from campagne_marketing.campagne_marketing import TraitementCampagneMarketing
from campagne_marketing.campagne_marketing_fields import FIELDS
from pyspark.sql.functions import col, sum as S
from pyspark.sql.types import (DateType, DoubleType, IntegerType, LongType,
                               StringType, StructField, StructType)


class TestCampagneMarketingTestCase:

    def test_campagne_marketing_job(self, spark_session):
        # Given
        ressource_path: str = f"src/traitement_spark/test/resources/"
        bv_operation_df = spark_session.spark.read.csv(
            f"{ressource_path}/bv_operation_marketing.csv", sep=",", header=True
        )
        bv_diffusion_df = spark_session.spark.read.csv(
            f"{ressource_path}/bv_diffusion_id_delivery_111494871.csv",
            sep=",",
            header=True,
        )
        pv_diffusion_log_df = spark_session.spark.read.csv(
            f"{ressource_path}/pv_diffusion_log_id_delivery_111494871_iurlid_2635086.csv",
            sep=",",
            header=True,
        )
        bv_log_message_df = spark_session.spark.read.csv(
            f"{ressource_path}/bv_log_message_sample.csv", sep=",", header=True
        )
        bv_recipient_df = spark_session.spark.read.csv(
            f"{ressource_path}/bv_recipient_fonction_sample.csv", sep=",", header=True
        )

        schema = StructType(
            [
                StructField("code_operation", StringType(), True),
                StructField("label_operation", StringType(), True),
                StructField("typologie_operation", StringType(), True),
                StructField("date_diffusion", StringType(), True),
                StructField("temps_fort_operation", StringType(), True),
                StructField("id_diffusion", StringType(), True),
                StructField("label_diffusion", StringType(), True),
                StructField("id_destinataire", StringType(), True),
                StructField("fonction_destinataire", StringType(), True),
                StructField("code_segment_destinataire", StringType(), True),
                StructField("segment_destinataire", StringType(), True),
                StructField("region_destinataire", StringType(), True),
                StructField("libelle_temps_fort_operation", StringType(), True),
                StructField("libelle_url", StringType(), True),
                StructField("type_de_click", StringType(), True),
                StructField("itodeliver", DoubleType(), True),
                StructField("volume_emails_delivres", DoubleType(), True),
                StructField("volume_ouverture", DoubleType(), True),
                StructField("volume_click", DoubleType(), True),
                StructField("volume_desinscription", DoubleType(), True),
            ]
        )

        # When
        cut = TraitementCampagneMarketing(spark_session, config=Mock())
        cut.read_table = MagicMock()
        cut.read_table.side_effect = [
            bv_operation_df,
            bv_diffusion_df,
            pv_diffusion_log_df,
            bv_log_message_df,
            bv_recipient_df,
        ]
        result = cut.submit()
        res = result.collect()

        # Then
        # Check schema
        assert result.schema == schema
        # Check number of rows
        assert len(res) == 16
        # Check values
        assert result.select(S(col("itodeliver"))).collect()[0][0] == 16.0
        assert result.select(S(col("volume_emails_delivres"))).collect()[0][0] == 15.0
        assert result.select(S(col("volume_ouverture"))).collect()[0][0] == 0.0
        assert result.select(S(col("volume_click"))).collect()[0][0] == 0.0
        assert result.select(S(col("volume_desinscription"))).collect()[0][0] == 16.0
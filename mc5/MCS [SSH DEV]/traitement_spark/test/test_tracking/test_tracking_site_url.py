 
from math import exp
from unittest.mock import ANY, MagicMock, Mock

import pytest
from pyspark.sql import Row
from pyspark.sql.functions import col, lit, regexp_replace
from pyspark.sql.types import (BooleanType, DoubleType, IntegerType, LongType,
                               StringType, StructField, StructType)
from tracking_site.tracking_site_fields import FIELDS, TYPE_PAGE
from tracking_site.tracking_site_url import TraitementTrackingSiteUrl


class TestTrackingSiteUrlTestCase:

    def test_tracking_site_url_job(self, spark_session):
        # Given
        ressource_path: str = f"src/traitement_spark/test/resources/"
        bv_tracking_df = spark_session.spark.read.csv(
            f"{ressource_path}/bv_tracking_sample.csv", sep=",", header=True
        )

        schema = StructType(
            [
                StructField(FIELDS.get("xx_jour"), StringType(), True),
                StructField(FIELDS.get("url"), StringType(), True),
                StructField(FIELDS.get("is_second_page"), IntegerType(), False),
                StructField(
                    FIELDS.get("type_page_type_de_contenu"), StringType(), True
                ),
                StructField(TYPE_PAGE.get("localtis"), IntegerType(), False),
                StructField(TYPE_PAGE.get("marketing_de_loffre"), IntegerType(), False),
                StructField(FIELDS.get("source_traffic"), StringType(), False),
                StructField(FIELDS.get("referrer_type_name"), StringType(), True),
                StructField("Total_Leads", LongType(), True),
                StructField("Total_Page_Views", LongType(), True),
                StructField("Total_Rebounds", LongType(), False),
                StructField("nb_visites", LongType(), False),
                StructField("nb_visiteurs", LongType(), False),
                StructField("TimespentTotal", DoubleType(), True),
                StructField("nb_entree", LongType(), False),
            ]
        )

        # When
        cut = TraitementTrackingSiteUrl(spark_session, config=Mock())
        cut.read_table = MagicMock()
        cut.read_table.side_effect = [bv_tracking_df]
        result = cut.submit()
        result = result.filter(
            col(FIELDS.get("url")) == "https://www.banquedesterritoires.fr/edition-localtis"
        )
        res = result.collect()

        # Then
        # Check schema
        assert result.schema == schema
        # Check number of rows
        assert len(res) == 2
        # Check values
        assert res[0].__getitem__("TimespentTotal") == 7.0
        assert res[0].__getitem__("SourceTraffic") == "Accès Direct"
        assert res[0].__getitem__("Localtis") == 1

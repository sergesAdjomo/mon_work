 
from math import exp
from unittest.mock import ANY, MagicMock, Mock

import pytest
from pyspark.sql.functions import col, lit, regexp_replace
from pyspark.sql.types import (DoubleType, IntegerType, LongType, StringType,
                               StructField, StructType)
from tracking_site.tracking_site import TraitementTrackingSite
import tracking_site.tracking_site_fields as Fields


class TestTrackingSiteTestCase:

    def test_tracking_site_job(self, spark_session):
        # Given
        ressource_path: str = f"src/traitement_spark/test/resources/"
        bv_tracking_df = spark_session.spark.read.csv(
            f"{ressource_path}/bv_tracking_sample.csv", sep=",", header=True
        )

        schema = StructType(
            [
                StructField(Fields.FIELDS.get("initial_visitor_id"), StringType(), True),
                StructField(Fields.FIELDS.get("id_visit"), StringType(), True),
                StructField(Fields.FIELDS.get("xx_jour"), StringType(), True),
                StructField(Fields.FIELDS.get("campaign_id"), StringType(), True),
                StructField(Fields.FIELDS.get("referrer_type"), StringType(), True),
                StructField(Fields.FIELDS.get("visiteur_qualifie"), StringType(), True),
                StructField(Fields.FIELDS.get("visiteur_connu"), StringType(), True),
                StructField(Fields.FIELDS.get("visiteur_engage"), StringType(), True),
                StructField(Fields.FIELDS.get("segments"), StringType(), True),
                StructField(Fields.FIELDS.get("nb_rebond"), IntegerType(), False),
                StructField(Fields.FIELDS.get("nb_rebond_home"), IntegerType(), False),
                StructField(Fields.FIELDS.get("type_site"), StringType(), False),
                StructField(Fields.FIELDS.get("fil_directions_regionales"), StringType(), False),
                StructField("nb_Leads", LongType(), True),
                StructField("nb_Audience_Exposee", LongType(), False),
                StructField("TpsPasse", LongType(), True),
                StructField("nb_Visit_type_leads_Dmd_contact", LongType(), False),
                StructField("nb_Visit_type_leads_CreaCompte", LongType(), False),
                StructField("nb_Visit_type_leads_AboLocH", LongType(), False),
                StructField("nb_Visit_type_leads_AboLocQ", LongType(), False),
                StructField("nb_Visit_type_leads_Dmd_pret", LongType(), False),
                StructField("nb_Visit_type_leads_NotSel", LongType(), False),
                StructField("NbPageVues", LongType(), True),
                StructField("NbPageVuesHome", LongType(), True),
                StructField("TpsPasseHome", LongType(), True),
                StructField("nbentree", LongType(), True),
                StructField("nb_entreehome", LongType(), True),
                StructField("SumVisitDuration", DoubleType(), True),
                StructField("nb_page_vu_Marketing_de_loffre", LongType(), True),
                StructField("nb_page_vu_Localtis", LongType(), True),
            ]
        )

        # When
        cut = TraitementTrackingSite(spark_session, config=Mock())
        cut.read_table = MagicMock()
        cut.read_table.side_effect = [bv_tracking_df]
        result = cut.submit()
        result = result.filter(col("idvisit") == "196730879")
        res = result.collect()

        # Then
        # Check schema
        assert result.schema == schema
        # Check number of rows
        assert len(res) == 1
        # Check values
        assert res[0].__getitem__(Fields.FIELDS.get("visiteur_connu")) is None
        assert res[0].__getitem__(Fields.FIELDS.get("type_site")) == "Espace Public"
        assert res[0].__getitem__("nb_page_vu_Localtis") == 1

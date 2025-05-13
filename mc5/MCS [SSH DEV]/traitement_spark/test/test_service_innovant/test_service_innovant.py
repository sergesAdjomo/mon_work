 
from unittest.mock import ANY, MagicMock, Mock

import pytest
import service_innovant.service_innovant_fields as Fields
from pyspark.sql.functions import col
from pyspark.sql.types import (DoubleType, IntegerType, LongType, StringType,
                               StructField, StructType)
from service_innovant.service_innovant import TraitementServiceInnovant


class TestServiceInnovantTestCase:

    def test_service_innovant_france_foncier(self, spark_session):
        # Given
        ressource_path: str = f"src/traitement_spark/test/resources/"
        test_foncier_df = spark_session.spark.read.csv(
            f"{ressource_path}/francefoncier.csv", sep=",", header=True
        )

        schema = StructType(
            [
                StructField("idvisit", StringType(), True),
                StructField("pagetitle", StringType(), True),
                StructField("timespent", StringType(), True),
                StructField("url", StringType(), True),
                StructField("xx_jour", StringType(), True),
                StructField("type", StringType(), True),
                StructField("Nom_Services", StringType(), False),
                StructField("Nom_Categorie", StringType(), False),
            ]
        )

        # When
        cut = TraitementServiceInnovant(spark_session, config=Mock())
        cut.read_table = MagicMock()
        cut.read_table.side_effect = [test_foncier_df]
        result = cut.submit()
        res = result.collect()
        
        # Then
        # Check schema
        assert result.schema == schema
        # Check number of rows
        assert len(res) == 1
        # Check values
        assert (
            res[0].__getitem__("pagetitle")
            == "France Foncier + : industrial warehouse and economic land search in France"
        )
        assert res[0].__getitem__("Nom_Services") == "France Foncier +"
        assert res[0].__getitem__("Nom_Categorie") == "Services Innovant"
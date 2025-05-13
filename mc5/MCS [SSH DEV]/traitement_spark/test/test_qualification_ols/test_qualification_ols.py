 
from unittest.mock import ANY, MagicMock, Mock

import pytest
import qualification_ols.qualification_ols_fields as Fields
from pyspark.sql.functions import col
from pyspark.sql.types import (DoubleType, IntegerType, LongType, StringType,
                               StructField, StructType)
from qualification_ols.qualification_ols import TraitementQualificationOLS


class TestQualificationOLSTestCase:

    def test_qualification_ols(self, spark_session):
        # Given
        ressource_path: str = "src/traitement_spark/test/resources/"
        bv_pm_df = spark_session.spark.read.csv(
            f"{ressource_path}/bv_personne_morale_bdt.csv", sep=",", header=True
        )
        bv_tiers_df = spark_session.spark.read.csv(
            f"{ressource_path}/bv_tiers_siren_006650089.csv", sep=",", header=True
        )
        bv_coord_postales_df = spark_session.spark.read.csv(
            f"{ressource_path}/bv_coord_postales_sample.csv", sep=",", header=True
        )
        bv_departement_df = spark_session.spark.read.csv(
            f"{ressource_path}/bv_departement.csv", sep=",", header=True
        )
        bv_region_df = spark_session.spark.read.csv(
            f"{ressource_path}/bv_region.csv", sep=",", header=True
        )

        schema = StructType(
            [
                StructField(Fields.FIELDS.get("siren"), StringType(), True),
                StructField("Raison_sociale", StringType(), True),
                StructField(Fields.FIELDS.get("sous_cat"), StringType(), True),
                StructField("Ville", StringType(), True),
                StructField("Région", StringType(), True),
                StructField(Fields.FIELDS.get("code_tiers"), StringType(), True),
                StructField("Tête_de_groupe", StringType(), True),
            ]
        )

        # When
        cut = TraitementQualificationOLS(spark_session, config=Mock())
        cut.read_table = MagicMock()
        cut.read_table.side_effect = [
            bv_pm_df,
            bv_tiers_df,
            bv_coord_postales_df,
            bv_departement_df,
            bv_region_df,
        ]
        result = cut.submit()
        result = result.filter(col(Fields.FIELDS.get("siren")) == "006650089")
        res = result.collect()

        # Then
        # Check schema
        assert result.schema == schema
        # Check number of rows
        assert len(res) == 1
        # Check values
        assert res[0].__getitem__("siren") == "006650089"
        assert res[0].__getitem__("Raison_sociale") == "HABITATIONS DE HAUTE-PROVENCE"
        assert res[0].__getitem__("sous_categorie") == "ESH"
        assert res[0].__getitem__("Ville") == "DIGNE LES BAINS CEDEX"
        assert res[0].__getitem__("Région") == "PROVENCE ALPES COTE D AZUR"
        assert res[0].__getitem__("code_tiers") == "277005"
        assert res[0].__getitem__("Tête_de_groupe") == "0"

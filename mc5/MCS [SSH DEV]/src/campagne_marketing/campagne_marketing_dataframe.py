 
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, countDistinct
from pyspark.sql.functions import sum as S
from pyspark.sql.functions import when
from pyspark.sql.window import Window
from traitement_spark.code.settings import Settings
from traitement_spark.code.utils import CommonUtils

import campagne_marketing.campagne_marketing_fields as Fields


class CampagneMarketingDataFrame(CommonUtils):
    def __init__(self, spark, config):

        self.spark = spark
        self.config = config
        self.df: DataFrame = None

        super().__init__(self.spark, self.config)

        self.settings = Settings(self.config, self.logger, self.date_ctrlm)

    def compute_adobe_indicator(self):
        if not isinstance(self.df, DataFrame):
            raise TypeError("Expected a DataFrame")

        window_spec = Window.partitionBy(
            Fields.FIELDS.get("id_delivery"), Fields.FIELDS.get("hub_id_client")
        )

        self.df = self.df.withColumn(
            Fields.FIELDS.get("libelle_temps_fort_operation"),
            when(col(Fields.FIELDS.get("itempsfort")) == 1, "OUI").otherwise(
                when(col(Fields.FIELDS.get("itempsfort")) == 0, "NON")
            ),
        ).withColumn(
            Fields.FIELDS.get("nbRows"),
            count(Fields.FIELDS.get("libelle_url")).over(window_spec),
        )
        return self

    def compute_adobe_kpi(self):
        if not isinstance(self.df, DataFrame):
            raise TypeError("Expected a DataFrame")
        self.df = (
            self.df.groupBy(*Fields.KPI_FIELDS)
            .agg(
                (
                    countDistinct(Fields.FIELDS.get("hub_id_client"))
                    / col(Fields.FIELDS.get("nbRows"))
                ).alias(Fields.FIELDS.get("itodeliver")),
                S(
                    when(
                        col(Fields.FIELDS.get("raison_echec")) == 0,
                        1 / col(Fields.FIELDS.get("nbRows")),
                    ).otherwise(0)
                ).alias(Fields.FIELDS.get("volume_emails_delivres")),
                S(
                    when(
                        col(Fields.FIELDS.get("type_de_click")) == 1,
                        1 / col(Fields.FIELDS.get("nbRows")),
                    ).otherwise(0)
                ).alias(Fields.FIELDS.get("volume_ouverture")),
                S(
                    when(
                        col(Fields.FIELDS.get("type_de_click")) == 2,
                        1 / col(Fields.FIELDS.get("nbRows")),
                    ).otherwise(0)
                ).alias(Fields.FIELDS.get("volume_click")),
                S(
                    when(
                        col(Fields.FIELDS.get("type_de_click")) == 3,
                        1 / col(Fields.FIELDS.get("nbRows")),
                    ).otherwise(0)
                ).alias(Fields.FIELDS.get("volume_desinscription")),
            )
            .select(
                col(Fields.FIELDS.get("nom_operation")).alias("code_operation"),
                col(Fields.FIELDS.get("libelle_operation")).alias("label_operation"),
                col(Fields.FIELDS.get("typologie_operation")).alias(
                    "typologie_operation"
                ),
                col(Fields.FIELDS.get("date_creation")).alias("date_diffusion"),
                col(Fields.FIELDS.get("itempsfort")).alias("temps_fort_operation"),
                col(Fields.FIELDS.get("id_delivery")).alias("id_diffusion"),
                col(Fields.FIELDS.get("sinternalname")).alias("label_diffusion"),
                col(Fields.FIELDS.get("hub_id_client")).alias("id_destinataire"),
                col(Fields.FIELDS.get("libelle_fonction")).alias(
                    "fonction_destinataire"
                ),
                col(Fields.FIELDS.get("code_segment")).alias(
                    "code_segment_destinataire"
                ),
                col(Fields.FIELDS.get("libelle_segment")).alias("segment_destinataire"),
                col(Fields.FIELDS.get("nom_region")).alias("region_destinataire"),
                col(Fields.FIELDS.get("libelle_temps_fort_operation")),
                col(Fields.FIELDS.get("libelle_url")),
                col(Fields.FIELDS.get("type_de_click")),
                col(Fields.FIELDS.get("itodeliver")),
                col(Fields.FIELDS.get("volume_emails_delivres")),
                col(Fields.FIELDS.get("volume_ouverture")),
                col(Fields.FIELDS.get("volume_click")),
                col(Fields.FIELDS.get("volume_desinscription")),
            )
        )
        return self

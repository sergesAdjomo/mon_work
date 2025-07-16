from pyspark.sql import *
from pyspark.sql.functions import col, max, lit
from datetime import datetime
import time
import socket
import json

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import (
    SerializationContext,
    MessageField,
    StringDeserializer,
)

from traitement_spark.code.appsec import AppSec
from traitement_spark.code.schema_registry import get_schema_registry

from icdc.hdputils.hive import hiveUtils
from icdc.hdputils.hdfs import hdfsUtils
import os


class ConsumerCiam:

    def __init__(self, spark, config, flux): 

        os.environ["HTTPS_PROXY"] = "http://pxy-http-srv.serv.cdc.fr:8080"
        os.environ["HTTP_PROXY"] = "http://pxy-http-srv.serv.cdc.fr:8080"
        self.spark = spark
        self.conf = config.config
        self.logger = config.logger
        self.flux = flux

        self.appsec = AppSec(self.spark, config)

        self.hive_utils = hiveUtils(self.spark)
        self.hdfs_utils = hdfsUtils()

        self.date_ctrlm = config.dateCrtlm()

        self.start_time = time.time()
        self.timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")

        self.app_name = self.conf.get("DEFAULT", "APP_NAME")

        self.logger.info("INFO: recuperation des variables de configuration")
        self.logger.info(f"INFO: app_name : {self.app_name}")
        self.logger.info(f"INFO: DRIVER :  {socket.gethostname()}")

        self.nom_table = self.flux
        self.db = self.conf.get("HIVE", "DB_HIVE_BRUTE")
        self.db_brute_path = self.conf.get("HIVE", "DB_HIVE_BRUTE_PATH")
        self.hdfs_brute_path = self.conf.get("HDFS", "HDFS_PATH_BRUTE")

        # informations technique
        self.topic = self.conf.get("KAFKA", "TOPIC")
        
        # informations technique
        self.kafka_env = self.conf.get("KAFKA", "KAFKA_ENV")
        self.checkpoint_path = f"{self.hdfs_brute_path}/checkpoint/{self.nom_table}"
        self.bootstrap_server = self.conf.get("KAFKA", "BOOTSTRAP_SERVERS")
        self.kafka_user = self.conf.get("KAFKA", "KAFKA_USER")
        self.kafka_password = self.appsec.get_credential(
            self.conf.get("KAFKA", "CREDENTIAL_PATH"),
            self.conf.get("KAFKA", "CREDENTIAL_PROVIDER_ALIAS"),
        )

        self.certification_authority = self.conf.get("KAFKA", "CERTIFICATION")
        self.schema_registry_url = self.conf.get("KAFKA", "SCHEMA_REGISTRY_URL")
        self.schema_registry = get_schema_registry()
        self.DATE_DEB_ALIM = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        self.topic_partition_offset_df: DataFrame = None

    def get_json_format_schema(self):
        schema_value = self.schema_registry.get_latest_version(f"{self.topic}-value")
        return schema_value.schema.schema_str

    def consume(self, from_offset):
        self.logger.info(from_offset)
        dsraw = (
            self.spark.spark.read.format("kafka")
            .option("kafka.bootstrap.servers", self.bootstrap_server)
            .option("kafka.security.protocol", "SASL_SSL")
            .option("kafka.ssl.ca.location", self.certification_authority)
            .option("kafka.sasl.mechanism", "PLAIN")
            .option("startingOffsets", from_offset)
            .option(
                "kafka.group.id", f"{self.kafka_env}.01.ci6.ciam-data.{self.date_ctrlm}"
            )  # TODO variabe a gerer avec var env
            .option("failOnDataLoss", "false")
            .option(
                "kafka.sasl.jaas.config",
                f"org.apache.kafka.common.security.plain.PlainLoginModule required username='{self.kafka_user}' password='{self.kafka_password}';",
            )
        )
        self.logger.info("selection of reader mode")
        if from_offset != "earliest":
            if len(json.loads(from_offset)[self.topic]) < 2:
                self.logger.info("Bascule en mode assign du topic")
                dsraw.option(
                    "assign",
                    f"{{{self.topic}: {str([int(i) for i in json.loads(from_offset)[self.topic].keys()])}}}",
                )
            else:
                self.logger.info("Bascule en mode subscribe du topic")
                dsraw.option("subscribe", self.topic)
        else:
            self.logger.info("Bascule en mode subscribe du topic")
            dsraw.option("subscribe", self.topic)
        self.logger.info("dsraw.load")
        dsraw = dsraw.load()
        self.logger.info("dsraw.load.ok")
        return dsraw

    def consume_avro(self, df: DataFrame):
        self.logger.info("consume_avro")
        header = [
            "key",
            "value",
            "partition",
            "offset",
            "topic",
            "timestamp",
            "timestampType",
        ]
        kafka_messages_list_deserialized = []

        self.logger.info("le df devrait etre show au dessus")
        rdd_coll = df.rdd.map(
            lambda x: [
                x["key"],
                x["value"],
                x["partition"],
                x["offset"],
                x["topic"],
                x["timestamp"],
                x["timestampType"],
            ]
        ).collect()
        for element in rdd_coll:
            json_key = self.deserializer_string(element[0])
            json_value = self.deserializer_avro(element[1], self.schema_registry)
            kafka_messages_list_deserialized.append(
                [
                    json_key,
                    json_value,
                    element[2],
                    element[3],
                    element[4],
                    element[5],
                    element[6],
                ]
            )
        self.logger.info("La file kafka a été consommée")

        deserialized_df = self.spark.spark.createDataFrame(
            kafka_messages_list_deserialized
        ).toDF(*header)

        return deserialized_df

    def deserializer_avro(self, value_serialized, schema_registry):
        schema_value = self.get_json_format_schema()
        deserializer_in = AvroDeserializer(schema_registry, schema_value)
        string_value = deserializer_in(
            value_serialized, SerializationContext(self.topic, MessageField.VALUE)
        )
        return json.dumps(string_value, default=str)

    def deserializer_string(self, msg_serialized):
        string_deserializer = StringDeserializer("utf_8")
        key_deser = string_deserializer(msg_serialized, "")
        return json.dumps(key_deser)

    def submit(self):
        """Ecriture dans la zone brute"""

        try:
            self.set_checkpoint()
            spark_starting_offsets = self.set_starting_offsets()
            df = self.consume(spark_starting_offsets)

            if len(df.take(1)) > 0:  # Check first row, if empty return df
                consumer_df = self.consume_avro(df)
                self.store_offset_metadata(consumer_df)
                consumer_df = consumer_df.withColumn("xx_date_insertion", lit(datetime.now()))
                self.hive_utils.hive_overwrite_table(
                    consumer_df,
                    self.db,
                    self.nom_table,
                    f"{self.db_brute_path}/{self.nom_table}",
                    drop_and_recreate_table=False,
                    partitionByColumns=None,
                    hive_format="parquet",
                    overwrite_only_partitions=False,
                )
        except Exception as e:
            raise Exception(f"Unable to consume Kafka topic: {str(e)}")

    def set_checkpoint(self):
        self.logger.info("set_checkpoint")
        checkpoint_path_bool = self.hdfs_utils.test_exist_hdfs_dir(f"{self.checkpoint_path}")
        if not checkpoint_path_bool:
            self.hdfs_utils.mkdir_in_hdfs(f"{self.checkpoint_path}")

    def set_starting_offsets(self) -> str:
        self.logger.info("set_starting_offsets")
        try:
            self.topic_partition_offset_df = self.spark.spark.read.json(f"{self.checkpoint_path}")
            tpo = self.construct_json_str_offset_md(self.topic_partition_offset_df)
            return tpo
        except Exception as Fnfe:
            self.logger.error(Fnfe)
            return "earliest"

    def store_offset_metadata(self, df):
        self.logger.info("store_offset_metadata")
        # get the lastest offsets and partition
        # store it in json
        self.logger.info(self.topic_partition_offset_df)

        df = (df.select(col("topic"), col('partition'), col('offset')))
        
        if self.topic_partition_offset_df is not None:
            df = (df
                .union(self.topic_partition_offset_df))
                
        df = (df
                .groupBy("topic", 'partition')
                .agg(max(col("offset")).alias("offset")))
              
        df.coalesce(1).write.mode("overwrite").format("json").option("compression", "none").save(f"{self.checkpoint_path}/")

    def construct_json_str_offset_md(self, df):
        """
        retrieve dataframe
        |topic | partition | offset |
        |topicA|     0     |  5     |
        |topicA|     1     |  6     |

        and convert into:
        {"topicA": {"0":5, "1": 6}}
        ref: https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html
        """
        self.logger.info("construct_json_str_offset_md")
        tpo = []
        dic = {}
        partition_offset = {}

        for i in df.collect():
            partition_offset[i[1]] = i[0]

        for i, j in partition_offset.items():
            tpo.append({str(i): j})
            for x in tpo:
                dic.update(x)
        # update offset +1
        dic.update({k: v + 1 for k, v in dic.items()})

        return json.dumps({self.topic: dic}, sort_keys=True)

    def create_file_in_hdfs(self, hdfs_dir_path):
        self.logger.info(
            "DEBUT: fonctions: mkdir_in_hdfs pour le dir: {0} ".format(hdfs_dir_path)
        )

        hdfs_cmd = f"hdfs dfs -put checkpoint.json {hdfs_dir_path}"
        s_output, s_err, s_return = self.hdfs_utils.run_edge_cmd(hdfs_cmd)

        if s_return == 0:
            self.logger.info("le dir {0} a bien ete cree ".format(hdfs_dir_path))
            return True

        elif (
            ("ERROR" in s_err) or ("Failed" in s_err) or ("Permission denied" in s_err)
        ):
            self.logger.error(" Erreur lors de la creation du dir : {0}".format(s_err))
            raise

        return False

from confluent_kafka.schema_registry import SchemaRegistryClient

from icdc.hdputils.configuration import confUtils
from icdc.hdputils.hdfs import hdfsUtils
from icdc.hdputils.spark import sparkUtils

from traitement_spark.code.appsec import AppSec

conf_utils = confUtils()
hdfs_utils = hdfsUtils()
spark_utils = sparkUtils()

conf = conf_utils.config

appsec = AppSec(spark_utils, conf_utils) 
topic = conf.get("KAFKA", "TOPIC")

kafka_user = conf.get("KAFKA", "KAFKA_USER")
kafka_password = appsec.get_credential(conf.get("KAFKA", "CREDENTIAL_PATH"), conf.get("KAFKA", "CREDENTIAL_PROVIDER_ALIAS"))
certification_authority = conf.get("KAFKA", "CERTIFICATION")
schema_registry_url = conf.get("KAFKA", "SCHEMA_REGISTRY_URL")

creds = appsec.get_credential(conf.get("KAFKA", "CREDENTIAL_PATH"), conf.get("KAFKA", "CREDENTIAL_PROVIDER_ALIAS"))
def get_json_format_schema():
        schema_value= get_schema_registry().get_latest_version(f"{topic}-value")
        return schema_value.schema.schema_str

def get_schema_registry():
        print(f"schema_registry_url:{schema_registry_url}")
        return SchemaRegistryClient({
            "url": schema_registry_url,
            "ssl.ca.location": certification_authority,
            "basic.auth.user.info": f"{kafka_user}:{kafka_password}"
            })

        

class AppSec:

    def __init__(self, spark, config):

        self.spark = spark
        self.conf = config.config
        self.logger = config.logger 

    def get_credential(self, credential_provider_path, credential_alias):
        conf = self.spark.spark.sparkContext._jsc.hadoopConfiguration()
        conf.set('hadoop.security.credential.provider.path', f"jceks://hdfs{credential_provider_path}")
        try:
            credential_raw = conf.getPassword(credential_alias)
        except Exception as e:
            self.logger.error(f"error: {e}")
        credential_str: str = ''
        for i in range(credential_raw.__len__()):
            credential_str = credential_str + str(credential_raw.__getitem__(i))
        return credential_str
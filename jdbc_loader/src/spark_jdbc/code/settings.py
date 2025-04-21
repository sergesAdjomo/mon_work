import datetime
from icdc.hdputils.spark import sparkUtils
from icdc.hdputils.hdfs import hdfsUtils
from icdc.hdputils.configuration import confUtils

class Settings:

    def init(self):
        self.conf_utils = confUtils()
        self.spark_utils = sparkUtils()
        self.spark = self.spark_utils.getSparkInstance()
        self.hdfs_utils = hdfsUtils()
        self.logger = self.conf_utils.logger
        self.config = self.conf_utils.config
        self.date_ctrlm = self.conf_utils.dateCrtlm()
        self.__setRunDate()
        self.load_settings()
        self.__get_credential()

    def load_settings(self):
        for section in self.config.sections():
            self.logger.info(f"Liste des options pour la section {section}")
            for option in self.config.options(section):
                self.logger.debug("{0} : {1}".format(option, self.config.get(section, option)))
                setattr(self, option, self.config.get(section, option))

    def __setRunDate(self):
        date_objet = datetime.date(int(self.date_ctrlm[0:4]),
                                  int(self.date_ctrlm[4:6]),
                                  int(self.date_ctrlm[6:8]))
        self.run_date = date_objet.strftime("%Y-%m-%d")

    def __get_credential(self):
        conf = self.spark.sparkContext._jsc.hadoopConfiguration()
        conf.set('hadoop.security.credential.provider.path', self.src_jks_path)
        credential_raw = conf.getPassword(self.src_jks_alias)
        credential_str = ''
        for i in range(credential_raw.__len__()):
            credential_str = credential_str + str(credential_raw.__getitem__(i))
        self.src_password = credential_str
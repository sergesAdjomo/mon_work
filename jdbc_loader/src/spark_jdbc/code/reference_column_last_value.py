from spark_jdbc.code.settings import Settings
from spark_jdbc.code.constants import SCHEMA_LAST_VALUE, KEY_LAST_VALUE

class ReferenceColumnLastValue:

    def init(self, settings: Settings):
        self.settings = settings
        self.input_last_value_df = self.read_dataframe_last_value()
        self.output_last_value_df = self.init_dataframe_last_value()
        self.new_last_value_df = self.init_dataframe_last_value()

    def read_dataframe_last_value(self):
        self.settings.spark.conf.get("spark.sql.session.timeZone")
        # Checker le répertoire de last_value et créer si il n'existe pas déjà
        fs = self.settings.spark._jvm.org.apache.hadoop.fs.FileSystem.get(self.settings.spark._jsc.hadoopConfiguration())
        path_snapshot = self.settings.spark._jvm.org.apache.hadoop.fs.Path(f"{self.settings.last_value_file_snapshot}")
        if not fs.exists(path_snapshot):
            self.settings.logger.info(f"Le répertoire {path_snapshot} n'existe pas. Il sera créé.")
            fs.mkdirs(path_snapshot)
        path_full = self.settings.spark._jvm.org.apache.hadoop.fs.Path(f"{self.settings.last_value_file_full}")
        if not fs.exists(path_full):
            self.settings.logger.info(f"Le répertoire {path_full} n'existe pas. Il sera créé.")
            fs.mkdirs(path_full)
        self.settings.logger.info(f"lecture de la last_value sur le path {self.settings.last_value_file_snapshot}")
        return self.settings.spark.read.format("csv") \
            .schema(SCHEMA_LAST_VALUE) \
            .option("header", True) \
            .option("inferSchema", True) \
            .option("delimiter", ";") \
            .option("ignoreLeadingWhiteSpace", True) \
            .load(self.settings.last_value_file_snapshot).persist()

    def write_all_dataframe_last_value(self):
        self.settings.spark.conf.set("mapred.output.compress", "false")
        df_new_take = self.new_last_value_df.take(1)
        if (len(df_new_take)==0 or df_new_take is None):
            self.settings.logger.debug(f"Pas d'écriture dans le fichier last_value : {self.settings.last_value_file_full} ")
        else:
            self.new_last_value_df.coalesce(1).write.format("csv") \
                .mode("append") \
                .option("header", True) \
                .option("delimiter", ";") \
                .option("ignoreLeadingWhiteSpace", True) \
                .save(self.settings.last_value_file_full)

        df_output_take = self.output_last_value_df.take(1)
        if (len(df_output_take)==0 or df_output_take is None):
            self.settings.logger.debug(f"Pas d'écriture dans le fichier last_value : {self.settings.last_value_file_snapshot} ")
        else:
            self.output_last_value_df.coalesce(1).write.format("csv") \
                .mode("overwrite") \
                .option("header", True) \
                .option("delimiter", ";") \
                .option("ignoreLeadingWhiteSpace", True) \
                .save(self.settings.last_value_file_snapshot)
        self.settings.spark.conf.set("mapred.output.compress", "true")

    def init_dataframe_last_value(self):
        return self.settings.spark.createDataFrame(data=[], schema=SCHEMA_LAST_VALUE)

    def generate_dataframe_last_value(self):
        delta_last_value_df = self.input_last_value_df.join(self.new_last_value_df, on=KEY_LAST_VALUE, how="left_anti")
        self.output_last_value_df = self.new_last_value_df.union(delta_last_value_df)

    def add_dataframe_row_last_value(self, row_last_value_df):
        self.new_last_value_df = self.new_last_value_df.union(row_last_value_df)
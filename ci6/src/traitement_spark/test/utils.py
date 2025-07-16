import json

def read_csv_to_tmp_table(spark, path_file, tablename, database):
   df = spark.read.format("csv").option("header", True).option("delimiter", ";").option("ignoreLeadingWhiteSpace", True).load("file://"+path_file)
   #df.show()
   df.write.mode("overwrite").option("TRANSLATED_TO_EXTERNAL", "TRUE").saveAsTable(f"{database}.{tablename}")
   return df

def get_content_filtred(json_file, filter_item, filter_value=None):
   content_filtred = []
   with open(json_file) as param_file:
      json_content = json.load(param_file)
      mapping_table = json_content["mapping_table"]
      content = list(json_content["tables"])
   for item in content:
        if item.get(filter_item) and (filter_value is None or item[filter_item]==filter_value):
            content_filtred.append(item)
   return mapping_table, content_filtred
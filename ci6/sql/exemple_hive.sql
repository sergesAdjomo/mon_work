# Creation 
CREATE EXTERNAL TABLE IF NOT EXISTS db_${env}_${code_app}.tb_test_hive (a string,b string )
STORED AS ORC  
LOCATION
  'hdfs://bddev01/${env}/ep/traitement/${code_app}/app_db/db_${env}_${code_app}_travail.db/tb_test_hive';


# Insertion
INSERT INTO TABLE db_${env}_${code_app}.tb_test_hive values 
('a', 'b');

# Count
SELECT COUNT(1) FROM db_${env}_${code_app}.tb_test_hive;
# Creation 
CREATE EXTERNAL TABLE IF NOT EXISTS db_${env}_${code_app}.tb_test_hive_bis (a string,b string, c bigint )
STORED AS ORC  
LOCATION
  'hdfs://bddev01/${env}/ep/traitement/${code_app}/app_db/db_${env}_${code_app}_travail.db/tb_test_hive';


# Insertion
INSERT INTO TABLE db_${env}_${code_app}.tb_test_hive_bis values 
('a', 'b', 1),
('z', 'y', 5),
('s', 'b', 9);

# Select avec clause where avec test_hivevar qui est passé en paramètre du KSH
SELECT * FROM db_${env}_${code_app}.tb_test_hive_bis WHERE c=${test_hivevar};
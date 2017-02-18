# Exporting from Hive

This is not completely easy, because Sqoop can only handle files. So we need to create a MySQL table which matches the
Hive table. This should not be too difficult.

## Prepareing MySQL target table

First we have to infer the required schema from Hive (where we already registered a table using the file).
```
hive> SHOW CREATE TABLE ish;
OK
CREATE TABLE `ish`(
  `usaf` string COMMENT 'from deserializer', 
  `wban` string COMMENT 'from deserializer', 
  `name` string COMMENT 'from deserializer', 
  `country` string COMMENT 'from deserializer', 
  `state` string COMMENT 'from deserializer', 
  `icao` string COMMENT 'from deserializer', 
  `latitude` string COMMENT 'from deserializer', 
  `longitude` string COMMENT 'from deserializer', 
  `elevation` string COMMENT 'from deserializer', 
  `date_begin` string COMMENT 'from deserializer', 
  `date_end` string COMMENT 'from deserializer')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ( 
  'escapeChar'='\\', 
  'quoteChar'='\"', 
  'separatorChar'=',') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://quickstart.cloudera:8020/user/hive/warehouse/training.db/ish'
TBLPROPERTIES (
  'COLUMN_STATS_ACCURATE'='true', 
  'numFiles'='1', 
  'numRows'='0', 
  'rawDataSize'='0', 
  'totalSize'='3047573', 
  'transient_lastDdlTime'='1448195115')
```

So this means we can use the following statement for MySQL:

```sql
USE training;
CREATE TABLE `ish`(
  `usaf` TEXT COMMENT 'from deserializer', 
  `wban` TEXT COMMENT 'from deserializer', 
  `name` TEXT COMMENT 'from deserializer', 
  `country` TEXT COMMENT 'from deserializer', 
  `state` TEXT COMMENT 'from deserializer', 
  `icao` TEXT COMMENT 'from deserializer', 
  `latitude` TEXT COMMENT 'from deserializer', 
  `longitude` TEXT COMMENT 'from deserializer', 
  `elevation` TEXT COMMENT 'from deserializer', 
  `date_begin` TEXT COMMENT 'from deserializer', 
  `date_end` TEXT COMMENT 'from deserializer');
```


## Export Data 

Now we finally can export the data from HDFS into MySQL

```
sqoop export \
    --connect jdbc:mysql://localhost/training \
    --username root --password cloudera \
    --table ish \
    --export-dir /user/hive/warehouse/training.db/ish \
    --input-optionally-enclosed-by '"' \
    --input-fields-terminated-by ',' \
    --columns usaf,wban,name,country,state,icao,latitude,longitude,elevation,date_begin,date_end
```

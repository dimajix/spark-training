# Spark WordCount

This is the Spark DataFrame based implementation of WordCount.

## Running

        run.sh
        
## Checking Results
        
Since the job will create a Parquet file, we can use Hive to look into the file
        
```sql
CREATE TEMPORARY EXTERNAL TABLE alice_wordcount
(word STRING, count BIGINT)
STORED AS PARQUET
LOCATION '/user/cloudera/alice_wordcount';        
```

Or this can also be done using Impala:
```sql
CREATE EXTERNAL TABLE alice_wordcount
LIKE PARQUET '/user/cloudera/alice_wordcount/part-r-00001.parquet'
STORED AS PARQUET
LOCATION '/user/cloudera/alice_wordcount';        
```


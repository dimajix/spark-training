# Spark Weather Analysis

This is the second Spark implementation of a simple weather analysis using Spark DataFrames.

## Preparing

This example requires weather data and station data to be present in HDFS.

## Running

    ./run.sh --input data/weather/2011 --stations data/weather/ish-history.csv --output data/weather/minmax
     
## Checking Result

This can be done within Hive or Impala

```sql
CREATE EXTERNAL TABLE weather_minmax 
LIKE PARQUETFILE '/user/cloudera/weather/minmax/part-r-00001.parquet'
STORED AS PARQUET
LOCATION '/user/cloudera/weather/minmax';
```

```sql
CREATE EXTERNAL TABLE temp.weather_minmax (                                                                                                                
   year STRING,                                                                                         
   country STRING,                                                                                  
   temp_min FLOAT,                                                                                         
   temp_max FLOAT,                                                                                         
   wind_min FLOAT,                                                                                         
   wind_max FLOAT                                                                                          
)                                                                                                                                                          
STORED AS PARQUET                                                                                                                                          
LOCATION 'hdfs://quickstart.cloudera:8020/user/cloudera/weather/minmax'
```

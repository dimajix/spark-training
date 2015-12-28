## Create Database

```sql
CREATE DATABASE training;
USE training;
```

## Create Tables
```sql
CREATE TABLE weather_2011(data STRING) STORED AS TEXTFILE;
```

```sql
LOAD DATA LOCAL INPATH 'data/weather/2011*.gz' INTO TABLE weather_2011;
```

```sql
SELECT * FROM weather_2011 limit 10;
```

```sql
SELECT SUBSTR(data,5,6) AS usaf FROM weather_2011 LIMIT 10;
```

```sql
SELECT 
    SUBSTR(data,5,6) AS usaf,
    SUBSTR(data,11,5) AS wban, 
    SUBSTR(data,16,8) AS date, 
    SUBSTR(data,24,4) AS time,
    SUBSTR(data,42,5) AS report_type,
    SUBSTR(data,61,3) AS wind_direction, 
    SUBSTR(data,64,1) AS wind_direction_qual, 
    SUBSTR(data,65,1) AS wind_observation, 
    SUBSTR(data,66,4) AS wind_speed,
    SUBSTR(data,70,1) AS wind_speed_qual,
    SUBSTR(data,88,5) AS air_temperature, 
    SUBSTR(data,93,1) AS air_temperature_qual 
FROM weather_2011 
LIMIT 10;
```

```sql
DROP TABLE weather_2011;
```


## Using Partitions and Views

First we recreate the table with

```sql
CREATE TABLE weather_raw(data STRING) PARTITIONED BY(year STRING) STORED AS TEXTFILE;
```

We load local data into the table using the correct partition

```sql
LOAD DATA LOCAL INPATH 'data/weather/2009/*.gz' OVERWRITE INTO TABLE weather_raw PARTITION(year=2009);
LOAD DATA LOCAL INPATH 'data/weather/2010/*.gz' OVERWRITE INTO TABLE weather_raw PARTITION(year=2010);
LOAD DATA LOCAL INPATH 'data/weather/2011/*.gz' OVERWRITE INTO TABLE weather_raw PARTITION(year=2011);
```

Then we'll create a new view

```sql
CREATE VIEW weather AS
    SELECT 
        year,
        SUBSTR(`data`,5,6) AS `usaf`,
        SUBSTR(`data`,11,5) AS `wban`, 
        SUBSTR(`data`,16,8) AS `date`, 
        SUBSTR(`data`,24,4) AS `time`,
        SUBSTR(`data`,42,5) AS report_type,
        SUBSTR(`data`,61,3) AS wind_direction, 
        SUBSTR(`data`,64,1) AS wind_direction_qual, 
        SUBSTR(`data`,65,1) AS wind_observation, 
        CAST(SUBSTR(`data`,66,4) AS FLOAT)/10 AS wind_speed,
        SUBSTR(`data`,70,1) AS wind_speed_qual,
        CAST(SUBSTR(`data`,88,5) AS FLOAT)/10 AS air_temperature, 
        SUBSTR(`data`,93,1) AS air_temperature_qual 
    FROM weather_raw; 
```

Et voila:
```sql
    select * from weather limit 10;
```

## Import ish Table

We also want to import the ish table, so we can lookup country names.

```sql
CREATE TABLE ish_raw(
    usaf STRING,
    wban STRING,
    name STRING,
    country STRING,
    fips STRING,
    state STRING,
    call STRING,
    latitude INT,
    longitude INT,
    elevation INT,
    date_begin STRING,
    date_end STRING) 
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\"",
   "escapeChar"    = "\\"
)
STORED AS TEXTFILE
LOAD DATA LOCAL INPATH 'data/weather/ish-history.csv' OVERWRITE INTO TABLE ish_raw;
```

Now we can perform exactly the same query as in Java examples:
```sql
SELECT 
    ish.country,
    MIN(w.air_temperature) as tmin,
    MAX(w.air_temperature) as tmax 
FROM weather w
INNER JOIN ish_raw ish 
    ON w.usaf=ish.usaf 
    AND w.wban=ish.wban
WHERE
    w.air_temperature_qual = "1"
GROUP BY ish.country;
```

But we can also group by year:
```sql
SELECT 
    ish.country,
    w.year,
    MIN(w.air_temperature) as tmin,
    MAX(w.air_temperature) as tmax 
FROM weather w
INNER JOIN ish_raw ish 
    ON w.usaf=ish.usaf 
    AND w.wban=ish.wban
WHERE
    w.air_temperature_qual = "1"
GROUP BY w.year,ish.country
```

## For Impala

Since Impala cannot use the SERDE from above, we need to convert the table
in Hive. We use Parquet as file format.


```sql
CREATE EXTERNAL TABLE ish(
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
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/cloudera/data/weather/ish';
```


```sql
CREATE TABLE ish 
    STORED AS PARQUET 
AS 
    SELECT 
        * 
    FROM ish_raw 
    WHERE usaf <> 'USAF';
```

Then we can perform a similar query:
```sql
SELECT 
    ish.country,
    w.year,
    MIN(w.air_temperature) as tmin,
    MAX(w.air_temperature) as tmax 
FROM weather w
INNER JOIN ish
    ON w.usaf=ish.usaf 
    AND w.wban=ish.wban
WHERE
    w.air_temperature_qual = "1"
GROUP BY w.year,ish.country;
```

## Compare with Parquet
```sql
CREATE TABLE weather_parquet 
    STORED AS PARQUET 
AS 
    SELECT 
        * 
    FROM weather;
```

```sql
SELECT 
    ish.country,
    w.year,
    MIN(w.air_temperature) as tmin,
    MAX(w.air_temperature) as tmax 
FROM weather_parquet w
INNER JOIN ish
    ON w.usaf=ish.usaf 
    AND w.wban=ish.wban
WHERE
    w.air_temperature_qual = "1"
GROUP BY w.year,ish.country;
```
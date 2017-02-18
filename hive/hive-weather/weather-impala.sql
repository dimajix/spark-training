-- Since Impala cannot use the SERDE from above, we need to convert the table
-- in Hive. We use Parquet as file format.

-- This has to be executed in Hive
CREATE EXTERNAL TABLE isd_raw(
    usaf STRING,
    wban STRING,
    name STRING,
    country STRING,
    state STRING,
    icao STRING,
    latitude FLOAT,
    longitude FLOAT,
    elevation FLOAT,
    date_begin STRING,
    date_end STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/cloudera/data/weather/isd-history';


-- This also needs to be executed in Hive!
CREATE TABLE isd
    STORED AS PARQUET 
AS 
    SELECT 
        * 
    FROM isd_raw 
    WHERE usaf <> 'USAF';


-- Do not forget to update Impala
INVALIDATE METADATA training.isd;

-- Then we can perform a similar query:
SELECT
    isd.country,
    w.year,
    MIN(w.air_temperature) as tmin,
    MAX(w.air_temperature) as tmax 
FROM weather w
INNER JOIN isd
    ON w.usaf=isd.usaf 
    AND w.wban=isd.wban
WHERE
    w.air_temperature_qual = "1"
GROUP BY w.year,isd.country;


-- Compare with Parquet
CREATE TABLE weather_parquet
    STORED AS PARQUET 
AS 
    SELECT 
        * 
    FROM weather;

SELECT
    isd.country,
    w.year,
    MIN(w.air_temperature) as tmin,
    MAX(w.air_temperature) as tmax 
FROM weather_parquet w
INNER JOIN isd
    ON w.usaf=isd.usaf 
    AND w.wban=isd.wban
WHERE
    w.air_temperature_qual = "1"
GROUP BY w.year,isd.country;

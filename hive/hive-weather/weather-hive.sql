-- noinspection SqlNoDataSourceInspectionForFile
--------------------------------------------------------------------------------------------
-- Create Database

CREATE DATABASE IF NOT EXISTS training;
USE training;

--------------------------------------------------------------------------------------------
-- Load some data to play with
CREATE EXTERNAL TABLE training.weather_2011(data STRING)
STORED AS TEXTFILE
LOCATION 's3://dimajix-training/data/weather/2011';

-- Look inside
SELECT * FROM training.weather_2011 limit 10;
SELECT SUBSTR(data,5,6) AS usaf FROM weather_2011 LIMIT 10;

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

-- Tidy up again
DROP TABLE weather_2011;


--------------------------------------------------------------------------------------------
-- Create new table for all weather data. The table is partitioned by year.
CREATE EXTERNAL TABLE IF NOT EXISTS training.weather_raw(
    data STRING
)
PARTITIONED BY(year STRING)
STORED AS TEXTFILE;

-- Add all partitions
ALTER TABLE training.weather_raw ADD PARTITION(year=2004) LOCATION 's3://dimajix-training/data/weather/2004';
ALTER TABLE training.weather_raw ADD PARTITION(year=2005) LOCATION 's3://dimajix-training/data/weather/2005';
ALTER TABLE training.weather_raw ADD PARTITION(year=2006) LOCATION 's3://dimajix-training/data/weather/2006';
ALTER TABLE training.weather_raw ADD PARTITION(year=2007) LOCATION 's3://dimajix-training/data/weather/2007';
ALTER TABLE training.weather_raw ADD PARTITION(year=2008) LOCATION 's3://dimajix-training/data/weather/2008';
ALTER TABLE training.weather_raw ADD PARTITION(year=2009) LOCATION 's3://dimajix-training/data/weather/2009';
ALTER TABLE training.weather_raw ADD PARTITION(year=2010) LOCATION 's3://dimajix-training/data/weather/2010';
ALTER TABLE training.weather_raw ADD PARTITION(year=2011) LOCATION 's3://dimajix-training/data/weather/2011';
ALTER TABLE training.weather_raw ADD PARTITION(year=2012) LOCATION 's3://dimajix-training/data/weather/2012';
ALTER TABLE training.weather_raw ADD PARTITION(year=2013) LOCATION 's3://dimajix-training/data/weather/2013';
ALTER TABLE training.weather_raw ADD PARTITION(year=2014) LOCATION 's3://dimajix-training/data/weather/2014';


-- Create View for extracting relevant measurements
CREATE VIEW training.weather AS
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
    FROM training.weather_raw;

-- Look into VIEW
SELECT * FROM training.weather LIMIT 10;

----------------------------------------------------------------------------------------------
-- Import isd Table using external table
CREATE EXTERNAL TABLE training.stations(
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
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\"",
   "escapeChar"    = "\\"
)
STORED AS TEXTFILE
LOCATION 's3://dimajix-training/data/weather/isd-history'
TBLPROPERTIES ("skip.header.line.count"="1");


----------------------------------------------------------------------------------------------
-- Performing Queries
SELECT
    isd.country,
    MIN(w.air_temperature) as tmin,
    MAX(w.air_temperature) as tmax 
FROM training.weather w
INNER JOIN training.stations isd
    ON w.usaf=isd.usaf 
    AND w.wban=isd.wban
WHERE
    w.air_temperature_qual = "1"
GROUP BY isd.country;

SELECT
    isd.country,
    w.year,
    MIN(w.air_temperature) as tmin,
    MAX(w.air_temperature) as tmax 
FROM training.weather w
INNER JOIN training.stations isd
    ON w.usaf=isd.usaf 
    AND w.wban=isd.wban
WHERE
    w.air_temperature_qual = "1"
GROUP BY w.year,isd.country


----------------------------------------------------------------------------------------------
-- Backup

-- Import isd Table using normal Table and LOAD DATA with local data
CREATE TABLE training.stations(
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
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\"",
   "escapeChar"    = "\\"
)
STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH 'data/weather/isd-history' OVERWRITE INTO TABLE isd_raw;

/* Load raw data */
weather_raw = LOAD 'data/weather/2011' AS (data:CHARARRAY);

/* Extract columns from raw data - note different indexes in SUBSTRING compared to Hive/Impala! */
weather = FOREACH weather_raw GENERATE
    SUBSTRING(data,4,10) AS usaf,
    SUBSTRING(data,10,15) AS wban,
    SUBSTRING(data,15,23) AS date,
    SUBSTRING(data,23,27) AS time,
    SUBSTRING(data,41,46) AS report_type,
    SUBSTRING(data,60,63) AS wind_direction,
    SUBSTRING(data,63,64) AS wind_direction_qual,
    SUBSTRING(data,64,65) AS wind_observation,
    ((FLOAT)SUBSTRING(data,65,69))/10.0F AS wind_speed,
    (INT)SUBSTRING(data,69,70) AS wind_speed_qual,
    ((FLOAT)SUBSTRING(data,87,92))/10.0F AS air_temperature,
    (INT)SUBSTRING(data,92,93) AS air_temperature_qual;

/* Have a look at the result so far */
ILLUSTRATE weather;


/* Register JAR file containing more storage backends */
REGISTER '/usr/lib/pig/piggybank.jar';

/* Now load ish lookup table */
ish = LOAD 'data/weather/ish-history.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage() AS (
    usaf:CHARARRAY,
    wban:CHARARRAY,
    name:CHARARRAY,
    country:CHARARRAY,
    state:CHARARRAY,
    icao:CHARARRAY,
    latitude:INT,
    longitude:INT,
    elevation:INT,
    date_begin:CHARARRAY,
    date_end:CHARARRAY);

/* Check how table looks like */
ILLUSTRATE ish;

/* Join weather data with station data */
ish_weather = JOIN weather BY (usaf,wban), ish BY(usaf,wban);
/* Check how table looks like */
ILLUSTRATE ish_weather;

/* Tidy up result, rename some columns etc */
geo_weather = FOREACH ish_weather GENERATE
    weather::date AS date,
    weather::time AS time,
    weather::wind_speed AS wind_speed,
    weather::wind_speed_qual AS wind_speed_qual,
    weather::air_temperature AS air_temperature,
    weather::air_temperature_qual AS air_temperature_qual,
    ish::country AS country;
/* Check how table looks like */
ILLUSTRATE geo_weather;

/* Filter invalid temperatures */
filtered_weather = FILTER geo_weather BY air_temperature_qual == 1;

/* Group weather by country */
weather_by_country = GROUP filtered_weather BY country;

/* Calculate some simple metrics */
minmax_by_country = FOREACH weather_by_country GENERATE
    group AS country,
    MIN(filtered_weather.air_temperature) AS min_temp,
    MAX(filtered_weather.air_temperature) AS max_temp;
ILLUSTRATE minmax_by_country;

/* Store results */
STORE minmax_by_country INTO 'data/output/minmax' USING org.apache.pig.piggybank.storage.CSVExcelStorage();

/* Load raw data */
weather_raw = LOAD 'weather/2011' AS (data:CHARARRAY);

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


/* Register JAR file containing more storage backends */
REGISTER '/usr/lib/pig/piggybank.jar';

/* Now load ish lookup table */
ish = LOAD 'weather/ish' USING org.apache.pig.piggybank.storage.CSVExcelStorage() AS (
    usaf:CHARARRAY,
    wban:CHARARRAY,
    name:CHARARRAY,
    country:CHARARRAY,
    fips:CHARARRAY,
    state:CHARARRAY,
    call:CHARARRAY,
    latitude:INT,
    longitude:INT,
    elevation:INT,
    date_begin:CHARARRAY,
    date_end:CHARARRAY);


/* EXERCISE: Create a table called 'minmax_weather' with the following columns:
    country,
    min_temp,
    max_temp,
    min_wind,
    max_wind

    Step 1. Join weather and ish using usaf and wban

    Step 2. Extract required fields from last dataset

    Step 3. Filter only valid temperature entries (air_temperature_qual == 1)

    Step 4. Group result by country code

    Step 5. Generate min/max temperature

    Step 6. Filter only valid temperature entries (air_temperature_qual == 1)

    Step 7. Group result by country code

    Step 8. Generate min/max wind

    Step 9: Rename columns to nicer names

    Step 10: Save result as CSV
*/


/* Store results */
STORE minmax_weather INTO 'weather/minmax' USING org.apache.pig.piggybank.storage.CSVExcelStorage();

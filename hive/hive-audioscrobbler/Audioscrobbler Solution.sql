-- 1. Create Database
--
-- Create a new (empty) database called "audioscrobbler"
--
CREATE DATABASE IF NOT EXISTS audioscrobbler;


-- 2. Create Table "artist"
--
-- Inside the database "audioscrobbler", create a table called "artist". The table
-- should point to the directory 's3://dimajix-training/data/audioscrobbler/artist_data'
--
-- The table contains the id of an artist together with its name.
--
-- The data inside the directory has two columns
--    id of type string
--    name of type string
-- the columns are separated by a tab (\t)!
--
CREATE EXTERNAL TABLE audioscrobbler.artist(
  id STRING,
  name STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = "\t",
   "quoteChar"     = "\"",
   "escapeChar"    = "\\"
)
STORED AS TEXTFILE
LOCATION 's3://dimajix-training/data/audioscrobbler/artist_data';

-- Look inside table, take 10 elements from table to make sure everything is correct.
SELECT
  *
FROM audioscrobbler.artist
LIMIT 10;

-- 3. Create Table "artist_alias"
--
-- Inside the database "audioscrobbler" create a table called "artist_alias". The table
-- should point to the directory 's3://dimajix-training/data/audioscrobbler/artist_alias'
--
-- The table contains incorrectly spelt artists and the correct artist id.
--
-- The data inside the directory has two columns
--    bad_id of type string
--    good_id of type string
-- the columns are separated by a tab (\t)!
--
CREATE EXTERNAL TABLE audioscrobbler.artist_alias(
  bad_id STRING,
  good_id STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = "\t",
   "quoteChar"     = "\"",
   "escapeChar"    = "\\"
)
STORED AS TEXTFILE
LOCATION 's3://dimajix-training/data/audioscrobbler/artist_alias';

-- Look inside table, take 10 elements from table to make sure everything is correct.
SELECT
  *
FROM audioscrobbler.artist_alias
LIMIT 10;


-- 4. Create Table "user_artist"
--
-- Inside the database "audioscrobbler" create a table called "user_artist". The table
-- should point to the directory 's3://dimajix-training/data/audioscrobbler/user_artist_data'
--
-- The table contains for the count of plays for specific artist and user combinations
--
-- The data inside the directory has two columns
--    user_id of type string
--    artist_id of type string
--    play_count of type integer
-- the columns are separated by a space! ATTENTION: This is different from the two other tables!
--
CREATE EXTERNAL TABLE audioscrobbler.user_artist(
  user_id STRING,
  artist_id STRING,
  play_count INT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = " ",
   "quoteChar"     = "\"",
   "escapeChar"    = "\\"
)
STORED AS TEXTFILE
LOCATION 's3://dimajix-training/data/audioscrobbler/user_artist_data';

-- Look inside table, take 10 elements from table to make sure everything is correct.
SELECT
  *
FROM audioscrobbler.user_artist
LIMIT 10;


-- 5. Query top artists
--
-- Ignoring the issue with misspelled names, get the IDs of the 10 most freuqently played artists by
-- performing an approiate aggregation on the table "user_artist". Sort the result by the aggregated play count
-- in descending order and return the top 10 played artists.
--
SELECT
  ua.artist_id,
  SUM(ua.play_count) AS play_count
FROM audioscrobbler.user_artist ua
GROUP BY ua.artist_id
ORDER BY play_count DESC
LIMIT 10;

-- Now perform the same task, but this time fix wrong artist IDs. This can be done elegantly by using a
-- common table expression (CTE) which fixes wrong artist IDs using the table artist_alias.
--
WITH fixed_user_artist AS (
  SELECT
    ua.user_id,
    COALESCE(alias.good_id, ua.artist_id) AS artist_id,
    ua.play_count
  FROM audioscrobbler.user_artist ua
  LEFT JOIN audioscrobbler.artist_alias alias
    ON alias.bad_id = ua.artist_id
)
SELECT
  ua.artist_id,
  SUM(ua.play_count) AS play_count
FROM fixed_user_artist ua
GROUP BY ua.artist_id
ORDER BY play_count DESC
LIMIT 10;

-- Finally also look up the artists name using the table "artist"
--
WITH fixed_user_artist AS (
  SELECT
    ua.user_id,
    COALESCE(alias.good_id, ua.artist_id) AS artist_id,
    ua.play_count
  FROM audioscrobbler.user_artist ua
  LEFT JOIN audioscrobbler.artist_alias alias
    ON alias.bad_id = ua.artist_id
),
artist_counts AS (
  SELECT
    ua.artist_id,
    SUM(ua.play_count) AS play_count
  FROM fixed_user_artist ua
  GROUP BY ua.artist_id
)
SELECT
  a.name,
  ac.play_count
FROM artist_counts ac
LEFT JOIN audioscrobbler.artist a
  ON a.id = ac.artist_id
ORDER BY ac.play_count DESC
LIMIT 10;

-- A similar (not completely equivalent) logic without CTEs
--
SELECT
  a.name,
  SUM(ua.play_count) AS play_count
FROM audioscrobbler.user_artist ua
LEFT JOIN audioscrobbler.artist_alias alias
  ON alias.bad_id = ua.artist_id
LEFT JOIN audioscrobbler.artist a
  ON a.id = COALESCE(alias.good_id, ua.artist_id)
GROUP BY a.name
ORDER BY play_count DESC
LIMIT 10;
;

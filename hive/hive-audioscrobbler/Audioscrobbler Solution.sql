-- 1. Create Database
--
-- Create a new database called "audioscrobbler"
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
-- the columns are separated by a space!
--
CREATE EXTERNAL TABLE audioscrobbler.artist(
  id STRING,
  name STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = " ",
   "quoteChar"     = "\"",
   "escapeChar"    = "\\"
)
STORED AS TEXTFILE
LOCATION 's3://dimajix-training/data/audioscrobbler/artist_data';


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
-- the columns are separated by a space!
--
CREATE EXTERNAL TABLE audioscrobbler.artist_alias(
  bad_id STRING,
  good_id STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = " ",
   "quoteChar"     = "\"",
   "escapeChar"    = "\\"
)
STORED AS TEXTFILE
LOCATION 's3://dimajix-training/data/audioscrobbler/artist_alias';


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
-- the columns are separated by a space!
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


-- 5. Query top artists

SELECT
  n.artist_name

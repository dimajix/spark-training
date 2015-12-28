-- Hive version using ish_raw table
SELECT
  l.country,
  l.tmin,
  l.tmax,
  r.wmin,
  r.wmax
FROM (
  SELECT
      ish.country as country,
      MIN(w.air_temperature) as tmin,
      MAX(w.air_temperature) as tmax
  FROM weather w
  INNER JOIN ish_raw ish
      ON w.usaf=ish.usaf
      AND w.wban=ish.wban
  WHERE
      w.air_temperature_qual = "1"
  GROUP BY ish.country) l
FULL OUTER JOIN (
  SELECT
      ish.country as country,
      MIN(w.wind_speed) as wmin,
      MAX(w.wind_speed) as wmax
  FROM weather w
  INNER JOIN ish_raw ish
      ON w.usaf=ish.usaf
      AND w.wban=ish.wban
  WHERE
      w.wind_speed_qual = "1"
  GROUP BY ish.country) r
ON l.country = r.country
;


-- Impala version using different table
SELECT
  l.country,
  l.tmin,
  l.tmax,
  r.wmin,
  r.wmax
FROM (
  SELECT
      ish.country as country,
      MIN(w.air_temperature) as tmin,
      MAX(w.air_temperature) as tmax
  FROM weather w
  INNER JOIN ish
      ON w.usaf=ish.usaf
      AND w.wban=ish.wban
  WHERE
      w.air_temperature_qual = "1"
  GROUP BY ish.country) l
FULL OUTER JOIN (
  SELECT
      ish.country as country,
      MIN(w.wind_speed) as wmin,
      MAX(w.wind_speed) as wmax
  FROM weather w
  INNER JOIN ish
      ON w.usaf=ish.usaf
      AND w.wban=ish.wban
  WHERE
      w.wind_speed_qual = "1"
  GROUP BY ish.country) r
ON l.country = r.country
;
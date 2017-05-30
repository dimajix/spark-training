-- Solution using a FULL OUTER JOIN
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
  FROM training.weather w
  INNER JOIN training.stations ish
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
  FROM training.weather w
  INNER JOIN training.stations ish
      ON w.usaf=ish.usaf
      AND w.wban=ish.wban
  WHERE
      w.wind_speed_qual = "1"
  GROUP BY ish.country) r
ON l.country = r.country
;

-- Solution using CASE WHEN
SELECT
    ish.country as country,
    MIN(CASE WHEN w.air_temperature_qual = "1" THEN w.air_temperature END) as tmin,
    MAX(CASE WHEN w.air_temperature_qual = "1" THEN w.air_temperature END) as tmax,
    MIN(CASE WHEN w.wind_speed_qual = "1" THEN w.wind_speed END) as wmin,
    MAX(CASE WHEN w.wind_speed_qual = "1" THEN w.wind_speed END) as wmax
FROM training.weather w
INNER JOIN training.stations ish
    ON w.usaf=ish.usaf
    AND w.wban=ish.wban
GROUP BY ish.country
;

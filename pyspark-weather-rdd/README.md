## Prerequisites

You need isd-history.csv and some weather data

## Using spark-submit

    spark-submit \
        --master yarn-client \
        --py-files weather.py \
        --executor-memory 1G \
        --executor-cores 2 \
        driver.py \
        --stations=data/weather/isd-history.csv \
        --weather=data/weather/2014 \
        --output weather_result

## Using provided Script

    run.sh \
        --stations=data/weather/isd-history.csv \
        --weather=data/weather/2014 \
        --output weather_result
        
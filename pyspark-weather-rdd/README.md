## Prerequisites

You need isd-history.csv



    spark-submit \
        --master yarn-client \
        --py-files weather.py \
        --executor-memory 1G \
        --executor-cores 2 \
        weather-rdd.py \
        --stations=data/weather/isd-history.csv \
        --weather=data/weather/2014 \
        --output weather_result

# Weather Analysis

## Prepare Data

You first need to upload the weather data into hadoop. This can also be done inside pig:

    fs -mkdir -p data/weather
    fs -put data/weather/2011 data/weather
    
    fs -mkdir -p data/weather/isd-history
    fs -put data/weather/isd-history/isd-history.csv data/weather/isd-history
    
## Cleanup output directory
    
    fs -rm -R data/weather/minmax

## Run Pig Script

    exec weather.pig
    
## Look at results
    
    fs -cat data/weather/minmax/*
    
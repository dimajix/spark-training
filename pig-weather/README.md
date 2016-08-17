# Weather Analysis

## Prepare Data

You first need to upload the weather data into hadoop. This can also be done inside pig:

    fs -mkdir weather/
    fs -put data/weather/2011 weather
    
    fs -mkdir weather/ish
    fs -put data/weather/isd-history.csv weather/isd-history
    
## Cleanup output directory
    
    fs -rm -R weather/minmax

## Run Pig Script

    exec weather.pig
    
## Look at results
    
    fs -cat weather/minmax/*
    
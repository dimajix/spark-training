## Debug Mode

You can perform a simple simulation using shell tools

        cp ../../data/weather/isd-history/isd-hostory.csv .
        zcat ../../data/weather/2011/*.gz | ./mapper.py | sort | ./reducer.py

# Uploading data

        hdfs dfs -mkdir -p data/weather
        hdfs dfs -put ~/data/weather/2011 data/weather
        ./run.sh data/weather/2011 weather_minmax

## Retrieve Results

You can retrieve the results via `hdfs dfs -germerge`

        hdfs dfs -getmerge weather_minmax weather_minmax
        cat weather_minmax

Or you can also view the results on the console via

        hdfs dfs -cat weather_minmax/*
        
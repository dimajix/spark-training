# Spark Weather Analysis

This is the second Spark implementation of a simple weather analysis using Spark DataFrames.

## Preparing

This example requires the station data to be present in HDFS.

You need a NetCat server running with some sample data:

    cat data/weather_sample | nc  -k -i1 -l 0.0.0.0 9977
    
or better
    
    cat data/weather_sample | spark-training/utils/pynetcat.py -P9977 -B50

## Running (stations in HDFS)

    ./run.sh --host quickstart --port 9977 --stations data/weather/isd

## Running (stations on S3)  
   
    ./run.sh --host quickstart --port 9977 --stations s3://is24-data-dev-spark-training/data/weather/isd-history/
     

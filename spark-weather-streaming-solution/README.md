# Spark Weather Analysis

This is the second Spark implementation of a simple weather analysis using Spark DataFrames.

## Preparing

This example requires the station data to be present in HDFS.

You need a NetCat server running with some sample data:

    zcat data/weather/2011/*.gz | nc  -k -i1 -l 0.0.0.0 9977

## Running

    ./run.sh --host quickstart --port 9977 --stations data/weather/isd
     

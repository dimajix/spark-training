# Spark Streaming WordCount

This is the first Spark Streaming implementation of a simple word count.

## Preparing

You need a NetCat server running with some sample data:

    nc  -k -i1 -l 0.0.0.0 9977 < alice.txt

## Running

    ./run.sh --host quickstart --port 9977

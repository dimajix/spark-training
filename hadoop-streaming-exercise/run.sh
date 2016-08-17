#!/bin/bash

hadoop  jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -files mapper.py,reducer.py,../../data/weather/ish-history.csv \
    -input $1 \
    -output $2 \
    -mapper mapper.py \
    -reducer reducer.py

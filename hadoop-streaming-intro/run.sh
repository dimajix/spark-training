#!/bin/bash

hadoop  jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -files mapper.py,reducer.py \
    -input $1 \
    -output $2 \
    -mapper mapper.py \
    -reducer reducer.py

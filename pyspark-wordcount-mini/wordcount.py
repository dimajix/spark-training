#!/usr/bin/python
# -*- coding: utf-8 -*-

from pyspark import SparkContext
from pyspark import SparkConf

conf = SparkConf()
sc = SparkContext(appName="WordCount", conf=conf)

sc.textFile("alice.txt") \
    .flatMap(lambda x: x.split()) \
    .filter(lambda x: x != "") \
    .map(lambda x: (x,1)) \
    .reduceByKey(lambda x,y: x+y) \
    .sortBy(lambda x: x[1], ascending=False) \
    .saveAsTextFile("alice_wordcount")

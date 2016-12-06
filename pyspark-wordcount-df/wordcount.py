#!/usr/bin/python
# -*- coding: utf-8 -*-

import optparse
import logging

from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.functions import *

logger = logging.getLogger(__name__)


def create_context(appName):
    """
    Creates Spark HiveContext, with WebUI disabled and logging minimized
    """
    logger.info("Creating Spark context - may take some while")

    # Create SparkConf with UI disabled
    conf = SparkConf()
    conf.set("spark.hadoop.validateOutputSpecs", "false")
    #conf.set('spark.ui.enabled','false')
    #conf.set('spark.executor.memory','8g')
    #conf.set('spark.executor.cores','6')

    sc = SparkContext(appName=appName, conf=conf)
    return sc


def parse_options():
    """
    Parses all command line options and returns an approprate Options object
    :return:
    """

    parser = optparse.OptionParser(description='PySpark WordCount.')
    parser.add_option('-i', '--input', action='store', nargs=1, help='Input file or directory')
    parser.add_option('-o', '--output', action='store', nargs=1, help='Output file or directory')

    (opts, args) = parser.parse_args()

    return opts


def main():
    opts = parse_options()

    logger.info("Creating Spark Context")
    sc = create_context(appName="WordCount")
    sqlContext = SQLContext(sc)

    logger.info("Starting processing")
    text = sqlContext.read.text('/user/cloudera/alice.txt')
    words = text.select(explode(split(text.value,' ')).alias('word')).filter(col('word') != '')
    counts = words.groupBy(words.word).count().orderBy('count',ascending=False)
    csv = counts.select(concat('word',lit(','),'count'))
    csv.write.mode('overwrite').text(opts.output)

    logger.info("Successfully finished processing")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.getLogger('').setLevel(logging.INFO)

    logger.info("Starting main")
    main()
    logger.info("Successfully finished main")

#!/usr/bin/python
# -*- coding: utf-8 -*-

import optparse
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

logger = logging.getLogger(__name__)


def create_session(appName):
    """
    Creates SparkSession for SQL operations. Ths will implicitly also create
    a SparkContext and a SQLContext.
    """
    logger.info("Creating Spark context - may take some while")

    # Create SparkConf with UI disabled
    spark = SparkSession.builder \
                .appName(appName) \
                .config("spark.hadoop.validateOutputSpecs", "false") \
                .getOrCreate()
    return spark


def parse_options():
    """
    Parses all command line options and returns an approprate Options object
    :return:
    """

    parser = optparse.OptionParser(description='PySpark WordCount.')
    parser.add_option('-i', '--input', action='store', nargs=1,
                        default='s3://dimajix-training/data/alice/',
                        help='Input file or directory')
    parser.add_option('-o', '--output', action='store', nargs=1,
                        default='alice-counts',
                        help='Output file or directory')

    (opts, args) = parser.parse_args()

    return opts


def main():
    opts = parse_options()

    logger.info("Creating Spark Context")
    session = create_session(appName="WordCount")

    logger.info("Starting processing")
    text = session.read.text(opts.input)
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

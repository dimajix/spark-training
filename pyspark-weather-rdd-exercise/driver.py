#!/usr/bin/python
# -*- coding: utf-8 -*-

import optparse
import logging

from pyspark.java_gateway import launch_gateway
from pyspark import SparkContext
from pyspark import SparkConf

from weather import StationData
from weather import WeatherData
from weather import WeatherMinMax


logger = logging.getLogger(__name__)
gateway = None


def get_py4j_gateway():
    """
    This creates the Py4j gateway used by Spark. We create it here, so we can silence logging
    activity.
    """
    global gateway
    if not gateway:
        logger.info("Creating Py4j gateway")
        gateway = launch_gateway()
        jvm = gateway.jvm

        # Reduce verbosity of logging
        l4j = jvm.org.apache.log4j
        l4j.LogManager.getRootLogger(). setLevel( l4j.Level.WARN )
        l4j.LogManager.getLogger("org"). setLevel( l4j.Level.WARN )
        l4j.LogManager.getLogger("akka").setLevel( l4j.Level.WARN )

    return gateway


def create_context(appName):
    """
    Creates Spark HiveContext, with WebUI disabled and logging minimized
    """
    logger.info("Creating Spark context - may take some while")

    # Create SparkConf with UI disabled
    conf = SparkConf()
    conf.set("spark.hadoop.validateOutputSpecs", "false")

    gateway = get_py4j_gateway()
    sc = SparkContext(appName=appName, conf=conf, gateway=gateway)
    return sc


def parse_options():
    """
    Parses all command line options and returns an approprate Options object
    :return:
    """

    parser = optparse.OptionParser(description='PySpark WordCount.')
    parser.add_option('-s', '--stations', action='store', nargs=1, help='Input file or directory containing station data')
    parser.add_option('-w', '--weather', action='store', nargs=1, help='Input file or directory containing weather data')
    parser.add_option('-o', '--output', action='store', nargs=1, help='Output file or directory')

    (opts, args) = parser.parse_args()

    return opts


def main():
    opts = parse_options()

    logger.info("Creating Spark Context")
    sc = create_context(appName="WordCount")

    logger.info("Starting processing")

    # 1. Load the weather stations data from HDFS as a text file. Then extract a StationData object from every text
    #    line via an appropriate 'rdd.map'. You can simply use the constructor of StationData to perform the extraction,
    #    i.e.
    #           StationData(text_line)
    #
    #    Store the result in 'stations'
    stations = ...

    # 2, Load the weather data itself from HDFS as a text file. Extract WeatherData objects similar as you have
    #    extracted the StationData.
    weather = ...

    # 3. Create a key for stations consisting of usaf + wban. This can be easily done with the RDD method keyBy
    #    which will create a tuple (key, value) for every value in the RDD. You only need to specify a function
    #    for extracting the key from a given data object
    station_index = stations.keyBy(...)

    # 4. Create a key for weather data consisting of usaf + wvban
    weather_index = ...

    # 5. Now join together both the weather data and the stations data using the keys previously constructed
    # joined_weather will contain tuples (usaf_wban, (weather, station))
    # i.e. [0] = usaf_wban
    #      [1][0] = weather
    #      [1][1] = station
    joined_weather = weather_index.join(station_index)

    # 6. Create a helper method for extracting a tuple (country, date) and the wather data from the joined data
    #    Pay attentiuon to the layout of the data in joined_weather as described above.
    #
    #    In order to extract the year from a date like date='20151012' you can use date[0:4]
    def extract_country_year_weather(data):
        weather = ...
        station = ...
        country = station.country
        year = ...
        return ((country, year, weather)

    # Now extract country and year using the function above
    weather_per_country_and_year = \
        joined_weather.map(extract_country_year_weather)

    # 7. Aggregate min/max information per year and country. We use a custom WeatherMinMax() class as a helper
    #    for storing the currently aggregated state.
    #
    #   We use the RDD method aggregateByKey(zero, reduce, combine)
    #       zero - neutral value
    #       reduce(a,v) - function to add another value v to an aggregator a.
    #       combine(a1,a2) - function to combine two aggregators a1 and a2
    weather_minmax = weather_per_country_and_year \
        .aggregateByKey(..., ..., ...)

    # 8. Finally we want to output a nicely formatted CSV file. For that we need a function which maps the
    #    resulting WeatherMinMax objects to a line with the following entries:
    #
    #   country,year,minTemperature,maxTemperature,minWindSpeed,maxWindSpeed
    def format_result(row):
        # Every row contains the key and the data.
        #   key is (country, year)
        #   value is WeatherMinMax
        (k,v) = row
        # Create CSV line here
        line = ...
        # Encode as UTF-8, or we might experience some problems
        return line.encode('utf-8')

    # Store results
    weather_minmax \
        .map(format_result) \
        .saveAsTextFile(opts.output)

    logger.info("Successfully finished processing")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.getLogger('').setLevel(logging.INFO)

    logger.info("Starting main")
    main()
    logger.info("Successfully finished main")

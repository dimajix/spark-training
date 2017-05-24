#!/usr/bin/python
# -*- coding: utf-8 -*-

import optparse
import logging

from pyspark import SparkContext
from pyspark import SparkConf

from weather import StationData
from weather import WeatherData
from weather import WeatherMinMax


logger = logging.getLogger(__name__)



def create_context(appName):
    """
    Creates Spark HiveContext, with WebUI disabled and logging minimized
    """
    logger.info("Creating Spark context - may take some while")

    # Create SparkConf with UI disabled
    conf = SparkConf()
    conf.set("spark.hadoop.validateOutputSpecs", "false")

    sc = SparkContext(appName=appName, conf=conf)
    return sc


def parse_options():
    """
    Parses all command line options and returns an approprate Options object
    :return:
    """

    parser = optparse.OptionParser(description='PySpark Weather Analysis.')
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

    stations = sc.textFile(opts.stations) \
        .map(lambda line: StationData(line)) \
        .keyBy(lambda data: data.usaf + data.wban) \
        .collectAsMap()

    weather = sc.textFile(opts.weather) \
        .map(lambda line: WeatherData(line))

    weather_index = weather.keyBy(lambda data: data.usaf + data.wban)

    # joined_weather will contain tuples weather, station)
    #      [0] = weather
    #      [1] = station
    stations_bc = sc.broadcast(stations)
    joined_weather = weather_index.map(lambda (key,data): (data,stations_bc.value[key]))

    # Helper method for extracting (country, date) and weather
    def extract_country_year_weather(data):
        return ((data[1].country, data[0].date[0:4]), data[0])

    # Now extract country and year using the function above
    weather_per_country_and_year = \
        joined_weather.map(extract_country_year_weather)

    # Aggregate min/max information per year and country
    weather_minmax = weather_per_country_and_year \
        .aggregateByKey(WeatherMinMax(),lambda a,v:a.reduce(v), lambda l,r:l.combine(r))

    # Helper method for pretty printing
    def format_result(row):
        (k,v) = row
        country = k[0]
        year = k[1]
        minT = v.minTemperature or 0.0
        maxT = v.maxTemperature or 0.0
        minW = v.minWindSpeed or 0.0
        maxW = v.maxWindSpeed or 0.0
        line = "%s,%s,%f,%f,%f,%f" % (country, year, minT, maxT, minW, maxW)
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

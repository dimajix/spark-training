#!/usr/bin/python
# -*- coding: utf-8 -*-

class StationData(object):
    """
    Class which holds station data details for convenient access
    """
    def _get_float(self,str):
        if len(str) == 0:
            return None
        try:
            return float(str)
        except ValueError:
            return None

    def __init__(self, line):
        raw_columns = line.split(',')
        columns = [c.replace('"','') for c in raw_columns]

        self.usaf = columns[0]
        self.wban = columns[1]
        self.name = columns[2]
        self.country = columns[3]
        self.state = columns[4]
        self.icao = columns[5]
        self.latitude = self._get_float(columns[6])
        self.longitude = self._get_float(columns[7])
        self.elevation = self._get_float(columns[8])
        self.date_begin = columns[9]
        self.date_end = columns[10]


class WeatherData(object):
    """
    Class which holds weather data details for convenient access
    """
    def __init__(self, line):
        self.date = line[15:23]
        self.time = line[23:27]
        self.usaf = line[4:10]
        self.wban = line[10:15]
        self.airTemperatureQuality = line[92] == '1'
        self.airTemperature = float(line[87:92]) / 10
        self.windSpeedQuality = line[69] == '1'
        self.windSpeed = float(line[65:69]) / 10


class WeatherMinMax(object):
    """
    Helper class used for aggregating weather information
    """
    def __init__(self, minT=None, maxT=None, minW=None, maxW=None):
        self.minTemperature = minT
        self.maxTemperature = maxT
        self.minWindSpeed = minW
        self.maxWindSpeed = maxW


def min2(a,b):
    """
    Helper method which is a None-aware version of 'min'
    """
    if a is None:
        return b
    if b is None:
        return a
    return min(a,b)


def max2(a,b):
    """
    Helper method which is a None-aware version of 'max'
    """
    if a is None:
        return b
    if b is None:
        return a
    return max(a,b)


def reduce_wmm(wmm, data):
    """
    Used for merging in a new weather data set into an existing WeatherMinMax object. The incoming
    objects will not be modified, instead a new object will be returned.
    :param wmm: WeatherMinMax object
    :param data: WeatherData object
    :returns: A new WeatherMinMax object
    """
    if data.airTemperatureQuality:
        minTemperature = min2(wmm.minTemperature, data.airTemperature)
        maxTemperature = max2(wmm.maxTemperature, data.airTemperature)
    else:
        minTemperature = wmm.minTemperature
        maxTemperature = wmm.maxTemperature

    if data.windSpeedQuality:
        minWindSpeed = min2(wmm.minWindSpeed, data.windSpeed)
        maxWindSpeed = max2(wmm.maxWindSpeed, data.windSpeed)
    else:
        minWindSpeed = wmm.minWindSpeed
        maxWindSpeed = wmm.maxWindSpeed

    return WeatherMinMax(minTemperature, maxTemperature, minWindSpeed, maxWindSpeed)


def combine_wmm(left, right):
    """
    Used for combining two WeatherMinMax objects into a new WeatherMinMax object
    :param left: First WeatherMinMax object
    :param right: Second WeatherMinMax object
    :returns: A new WeatherMinMax object
    """
    minTemperature = min2(left.minTemperature, right.minTemperature)
    maxTemperature = max2(left.maxTemperature, right.maxTemperature)
    minWindSpeed = min2(left.minWindSpeed, right.minWindSpeed)
    maxWindSpeed = max2(left.maxWindSpeed, right.maxWindSpeed)
    return WeatherMinMax(minTemperature, maxTemperature, minWindSpeed, maxWindSpeed)

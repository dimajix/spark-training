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


def nullsafe_min(a, b):
    """
    Helper method which is a None-aware version of 'min'
    """
    if a is None:
        return b
    if b is None:
        return a
    return min(a,b)


def nullsafe_max(a, b):
    """
    Helper method which is a None-aware version of 'max'
    """
    if a is None:
        return b
    if b is None:
        return a
    return max(a,b)


class WeatherMinMax(object):
    """
    Helper class used for aggregating weather information
    """
    def __init__(self, minT=None, maxT=None, minW=None, maxW=None):
        self.minTemperature = minT
        self.maxTemperature = maxT
        self.minWindSpeed = minW
        self.maxWindSpeed = maxW


    def reduce(self, data):
        """
        Used for merging in a new weather data set into an existing WeatherMinMax object. The incoming
        objects will not be modified, instead a new object will be returned.
        :param self: WeatherMinMax object
        :param data: WeatherData object
        :returns: A new WeatherMinMax object
        """

        # Get minimum wind speed from current wind speed and from incoming data. Also pay attention
        # to airTemperatureQuality and windSpeedQuality!
        minTemperature = ...
        maxTemperature = ...
        minWindSpeed = ...
        maxWindSpeed = ...

        return WeatherMinMax(minTemperature, maxTemperature, minWindSpeed, maxWindSpeed)


    def combine(self, other):
        """
        Used for combining two WeatherMinMax objects into a new WeatherMinMax object
        :param self: First WeatherMinMax object
        :param other: Second WeatherMinMax object
        :returns: A new WeatherMinMax object
        """
        # Get minimum wind speed from current wind speed and from incoming data. Note that all values
        # in both self and incoming other might be None!
        minTemperature = ...
        maxTemperature = ...
        minWindSpeed = ...
        maxWindSpeed = ...

        return WeatherMinMax(minTemperature, maxTemperature, minWindSpeed, maxWindSpeed)

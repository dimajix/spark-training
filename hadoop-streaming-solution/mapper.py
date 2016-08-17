#!/usr/bin/python

import sys
import csv

INVENTORY_FILE = "ish-history.csv"


def read_countries():
    """
    Reads station code to country code lookup table from file
    :return:
    """
    countries = {}

    with open(INVENTORY_FILE) as file:
        # Skip header
        file.readline()

        reader = csv.reader(file, delimiter=',', quotechar='"')
        for row in reader:
            # USAF code of station
            usaf = row[0]
            # WBAN code of station
            wban = row[1]
            # Country code
            country = row[3]

            # Fully concatenated station code
            station_code = usaf + wban
            countries[station_code] = country

    return countries


# input comes from STDIN (standard input)
def run():
    countries = read_countries()

    for line in sys.stdin:
        station = line[4:15]
        date = line[15:23]
        time = line[23:27]
        wind = line[65:69]
        air_temp_qual = line[92]
        air_temp = line[87:92]

        # Only emit if quality is okay
        if (air_temp_qual == '1'):
            country = countries[station]
            print(country + '\t'+ air_temp)


run()

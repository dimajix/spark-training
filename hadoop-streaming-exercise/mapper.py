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
        # Extract station and temperature from raw data
        station = line[4:15]
        air_temp_qual = line[92]
        air_temp = line[87:92]

        # Lookup country in map

        # Check if measurement is ok (air_temp_qual == '1')

        # Print country and temperature, seperated by tab ('\t')


run()

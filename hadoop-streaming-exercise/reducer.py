#!/usr/bin/python

import sys


# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    # parse the input we got from mapper.py
    country, temp = line.split('\t', 1)

    # Chceck if still received same country than last time
    # if so, then update global min/max values
    # ...otherwise emit country and last min/max values and init global min/max values and emit


# Finally emit last min/max value

#!/usr/bin/python

import sys

current_country = None
current_min_temp = 10000
current_max_temp = -10000


def emit_aggregate():
    if current_country:
        # write result to STDOUT
        print '%s\t%s\t%s' % (current_country, current_min_temp, current_max_temp)


# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    # parse the input we got from mapper.py
    country, temp = line.split('\t', 1)
    temp = float(temp) / 10.

    # this IF-switch only works because Hadoop sorts map output
    # by key (here: word) before it is passed to the reducer
    if current_country == country:
        if (temp > current_max_temp):
            current_max_temp = temp
        if (temp < current_min_temp):
            current_min_temp = temp
    else:
        emit_aggregate()
        current_country = country
        current_min_temp = temp
        current_max_temp = temp

# do not forget to output the last word if needed!
emit_aggregate()

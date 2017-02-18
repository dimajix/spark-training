#!/usr/bin/python

import sys

# Helper variables which hold current country/maximum/minum
current_country = None
current_min_temp = 10000
current_max_temp = -10000


# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    # parse the input we got from mapper.py
    country, temp = line.split('\t', 1)
    temp = float(temp) / 10.

    # Chceck if still received same country than last time
    # if so, then update global min/max values
    if current_country == country:
        # YOUR CODE HERE
        pass
    # ...otherwise emit country and last min/max values and init global min/max values and emit
    else:
        # YOUR CODE HERE
        pass

# Finally emit last min/max value
# YOUR CODE_HERE

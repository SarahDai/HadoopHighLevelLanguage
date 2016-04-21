#!/usr/bin/env python

import sys

current_cust = "-1"
current_count = 0

# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    
    try:
        # this IF-switch only works because Hadoop sorts map output
        # by key (here: custID) before it is passed to the reducer
        # parse the input we got from mapper.py
        custID, name, countrycode, transID = line.split(',')

        if current_cust == custID:
                   current_count += 1
                   if (transID =="-1" and countrycode == "5"):
                          print '%s,%s,%s'% (custID, name, current_count)
        else:
               current_cust = custID
               current_count = 0

    except:
        pass

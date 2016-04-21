#!/usr/bin/env python

import sys

# input comes from STDIN (standard input)
for line in sys.stdin:
    try:   
        custID = "-1"
        name = "-1"
        countrycode = "-1"
        transID = "-1"#default sorted as first
        # remove leading and trailing whitespace
        line = line.strip()
        # split the line into words
        split = line.split(",")
   
        if len(split[1]) > 6:
             custID = split[0]
             name = split[1]
             countrycode = split[3]
        else:
             transID = split[0]
             custID = split[1]
             # write the results to STDOUT (standard output);
             # what we output here will be the input for the
             # Reduce step, i.e. the input for reducer.py
             #
             # ','-delimited; 

        print '%s,%s,%s,%s' % (custID, name, countrycode, transID)
    except:#errors are going to make your job fail which you may or may not want
        pass


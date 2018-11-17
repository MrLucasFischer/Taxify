%%file mapper.py
#!/usr/bin/env python

import sys
import traceback
import datetime
from datetime import datetime as dt
import calendar
import time
import numpy as np
    
def get_duration(pick_up_datetime, drop_off_datetime):
    """
        Get duration of trip in minutes from pick up and drop off times
    """

    d1 = time.mktime(dt.strptime(drop_off_datetime, '%Y-%m-%d %H:%M:%S').timetuple())
    d2 = time.mktime(dt.strptime(pick_up_datetime, '%Y-%m-%d %H:%M:%S').timetuple())
    return int((d1 - d2) / 60)


def create_key_value(splitted_line):
    """
        Function that creates the key value structure for every line of interest

        Params:
            A non-filtered raw line of the CSV file
    """
    pick_up_datetime = splitted_line[1]

    #Convert a weekday index (0, 1, etc.) to a weekday string ('monday', 'tuesday', etc.)
    week_day = (calendar.day_name[dt.strptime(splitted_line[1], '%Y-%m-%d %H:%M:%S').weekday()]).lower()
    
    #Obtain just the hour from the pick_up_datetime string
    hour =  pick_up_datetime[11:13]

    pick_up_id = splitted_line[7]
    
    dropoff_up_id = splitted_line[8]

    key = (week_day, hour, pick_up_id, dropoff_up_id)

    duration = get_duration(pick_up_datetime, splitted_line[2])
    
    total_amount = float(splitted_line[16])
    
    value = (duration, total_amount)

    return (key, value)

#Iterating every line from the input file
is_first_line = True

for line in sys.stdin:
    
    if(not is_first_line): #Filtering out the first line
        if(len(line) > 0): #Filtering out non empty lines

            splitted_line = line.split(",")
            if(len(splitted_line) == 17):
                
                #((Weekday, pick-up ID, drop-off ID), ([duration], [total_amount]))
                key, value = create_key_value(splitted_line)

                print("{}\t{}\t{}".format(key, value[0], value[1]))

    else:
        is_first_line = False
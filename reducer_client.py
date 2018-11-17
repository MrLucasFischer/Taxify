%%file reducer_client.py
#!/usr/bin/env python

import sys
import numpy as np
import datetime
from datetime import datetime as dt
import calendar

#Simulating user options
user_weekday, user_puid, user_doid, user_hour = (1, "246", "239", 2)

#user_weekday ranges from 1 to 7
user_weekday = (calendar.day_name[user_weekday - 1]).lower()

for line in sys.stdin:
    line = line.replace("(", "").replace("\'", "").replace(" ", "").replace("\t", "").replace("\n", "")
    splitted_line = line.split(",")
    
    if(len(splitted_line) == 5):
        weekday = splitted_line[0]
        hour = int(splitted_line[1])
        pu_id = splitted_line[2]
        do_id, duration = (splitted_line[3].split(")")[0], splitted_line[3].split(")")[1])
        amount = splitted_line[4].split(")")[0]
        
        if(pu_id == user_puid and do_id == user_doid and weekday == user_weekday and hour == user_hour):
            print("{},{}".format(line.replace(")", ",")[:-1], 1))
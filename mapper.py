%%file mapper.py
#!/usr/bin/env python

import sys
import traceback
import datetime
from datetime import datetime as dt
import calendar
import time
import numpy as np

def filter_dates(input_date_time, user_weekday, user_hour, user_minutes):
    """
        Predicate function that returns true if input_date_time is within 30 minutes radius of user's desired time, false otherwise

        Params:
            input_date - String in YYYY-MM-DD HH:MM format
            user_weekday - Integer ranging from 1 to 7 representing the weekday
            user_hour - Integer representing the hour
            user_minutes - Integer representing the minutes

        Returns:
            True if input_date_time is within 30 minutes radius of user's desired time, false otherwise
    """

    #First check if input_date_time week day is at the maximum one more day than users desired time
    date_obj = dt.strptime(input_date_time, '%Y-%m-%d %H:%M:%S')

    input_weekday = date_obj.weekday()
    user_weekday -= 1   #since input_weekday is between [0, 6] we need to subtract 1 to our user_weekday

    input_date = input_date_time[0:10] #Getting the characters that represent the date
    user_date = dt.strptime(input_date + " {}:{}:00".format(user_hour, user_minutes), '%Y-%m-%d %H:%M:%S') #Creating a new date time object with the date of the input date, and time of the user

    if(user_hour == 23 and user_minutes > 29):
        user_date = dt.strptime(input_date + " {}:{}:00".format(user_hour, user_minutes), '%Y-%m-%d %H:%M:%S') - datetime.timedelta(days = 1)

    if(input_weekday == user_weekday or (input_weekday == 0 and user_weekday == 6) or (input_weekday == user_weekday + 1)):
        time_plus_30_min = (user_date + datetime.timedelta(minutes = 30))
        return user_date <= date_obj <= time_plus_30_min
    else:
        return False
    
    
    
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

    week_day = (calendar.day_name[dt.strptime(splitted_line[1], '%Y-%m-%d %H:%M:%S').weekday()]).lower()
    hour =  pick_up_datetime[11:13]
    minute = pick_up_datetime[14:16]

    pick_up_id = splitted_line[7]
    dropoff_up_id = splitted_line[8]

    key = (week_day, pick_up_id, dropoff_up_id)

    duration = get_duration(pick_up_datetime,splitted_line[2])
    total_amount = float(splitted_line[16])
    
    value = (duration, total_amount)

    return (key, value)

user_weekday, user_puid, user_doid, user_hour, user_minutes = (1, 246, 239, 2, 8)

#Iterating every line from the input file
is_first_line = True

beforeT = dt.now()
for line in sys.stdin:
    
    if(not is_first_line): #Filtering out the first line
        if(len(line) > 0): #Filtering out non empty lines

            splitted_line = line.split(",")
            if(len(splitted_line) == 17):
                
                if(splitted_line[7] == str(user_puid) and splitted_line[8] == str(user_doid)): #Filtering out lines that don't match pick-up/dropoff ID sent by the user

                    if(filter_dates(splitted_line[1], user_weekday, user_hour, user_minutes)): #Filtering out lines that are not between a 30 min radius of the user date and time

                        #((Weekday, pick-up ID, drop-off ID), ([duration], [total_amount]))
                        key,value = create_key_value(splitted_line)
                        
                        print("{}\t{}\t{}\t{}".format(key, value[0], value[1], beforeT))
                        

    else:
        is_first_line = False

#!hadoop jar /opt/hadoop-2.9.1/share/hadoop/tools/lib/hadoop-*streaming*.jar -files mapper.py,reducer.py -mapper mapper.py -reducer reducer.py -input hdfs:/user/jovyan/SPBD-1819/yellow_tripdata_2018-01.csv -output results_words
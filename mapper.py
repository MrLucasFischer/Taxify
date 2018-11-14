%%file mapper.py
#!/usr/bin/env python

import sys
import traceback
import datetime
from datetime import datetime as dt
import calendar
import time
import numpy as np
import pandas as pd


def get_user_options():
    """
        Function that gets all the users input for creating the inverted index.
        This function gets the desired weekday, time, pickup and dropoff zone
    """

    pickup_correct = False
    dropoff_correct = False
    weekday_correct = False
    time_correct = False
    pickup_id = ""
    dropoff_id = ""
    weekday = ""
    hour = ""
    minutes = ""                                                

    #Continue asking the user until he/she gives us a weekday
    while(not weekday_correct):
        weekday = input("\nPlease insert you weekday (1- Monday, 2- Tuesday, ..., 7- Sunday): ")
        try:
            if(int(weekday) >= 1 and int(weekday) <= 7):
                weekday_correct = True
        except:
            #User didn't sent us a number
            print("\nPlease insert a number between 1 - 7\n")

    #Continue asking the user until he/she gives us an hour
    while(not time_correct):
        time_input = input("\nPlease insert the desired time (hh:mm): ")
        try:
            user_time = time.strptime(time_input, '%H:%M') # Check time is in proper format
            time_correct = True
            hour = user_time.tm_hour #Get hour
            minutes = user_time.tm_min #Get minutes

        except:
            #User didn't sent us a number
            print("\nPlease insert a time in the format hh:mm where hh (00-23) and mm (00:59) \n")    



    #Continue asking the user until he/she gives us a number between 1 and 265
    while(not pickup_correct):
        pickup_id = input("\nPlease insert you Pick-Up location ID (1 - 265): ")
        try:
            if(int(pickup_id) >= 1 and int(pickup_id) <= 265):
                pickup_correct = True
        except:
            #User didn't sent us a number
            print("\nPlease insert a number between 1 - 265\n")

        

    #Continue asking the user until he/she gives us a number between 1 and 265
    while(not dropoff_correct):
        dropoff_id = input("\nPlease insert you Drop-Off location ID (1 - 265): ")
        try:
            if(int(dropoff_id) >= 1 and int(dropoff_id) <= 265):
                dropoff_correct = True
        except:
            #User didn't sent us a number
            print("\nPlease insert a number between 1 - 265\n")


    return(weekday, pickup_id, dropoff_id, hour, minutes)


for line in sys.stdin:
    print(line)
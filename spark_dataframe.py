from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark import SparkContext
import traceback
import datetime
from datetime import datetime as dt
import calendar
import time
import numpy as np
import pandas as pd

spark = SparkSession.builder.master('local[*]').appName('taxify').getOrCreate()
sc = spark.sparkContext

locations = pd.read_csv("taxi_zone_lookup.csv")

#PU/DO zone ids range from 1 to 265, see taxi_zone_lookup.csv
#date(position 1, maybe split datetime into weekday and time), PU_ID (position 7), DO_ID (position 8), totalammount (position 16)

#Main implementation

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


def create_key_value(line):
    """
        Function that creates the key value structure for every line of interest

        Params:
            A non-filtered raw line of the CSV file
    """
    splitted = line.split(",")
    pick_up_datetime = splitted[1]

    week_day = (calendar.day_name[dt.strptime(splitted[1], '%Y-%m-%d %H:%M:%S').weekday()]).lower()
    hour =  pick_up_datetime[11:13]
    minute = pick_up_datetime[14:16]

    pick_up_id = splitted[7]
    dropoff_up_id = splitted[8]

    key = (week_day, pick_up_id, dropoff_up_id)

    duration = get_duration(pick_up_datetime,splitted[2])
    total_amount = float(splitted[16])
    
    value = ([duration], [total_amount])

    return (key, value)



def get_duration(pick_up_datetime, drop_off_datetime):
    """
        Get duration of trip in minutes from pick up and drop off times
    """

    d1 = time.mktime(dt.strptime(drop_off_datetime, '%Y-%m-%d %H:%M:%S').timetuple())
    d2 = time.mktime(dt.strptime(pick_up_datetime, '%Y-%m-%d %H:%M:%S').timetuple())
    return int((d1 - d2) / 60)


def create_inverted_index(user_weekday = 1, user_puid = 41, user_doid = 24, user_hour = 0, user_minutes = 21, filename = 'yellow_tripdata_2018-01_sample.csv'):
    try :
        lines = sc.textFile(filename) #read csv file (change this to the full dataset instead of just the sample) (this is local to my machine)
        first_line = lines.first()
        
        #Filtering out the first line, empty lines
        non_empty_lines = lines.filter(lambda line: len(line) > 0 and line != first_line)

        #Filter out lines that don't match user's pickup-ID and dropoff-ID
        lines_with_piud_doid = non_empty_lines.filter(lambda line: line.split(",")[7] == str(user_puid) and line.split(",")[8] == str(user_doid))

        # # My attempt to transform above code to use dataFrames
        # # Get fields from lines
        # fields_in_lines = non_empty_lines.map( lambda line : Row( pu_dt = line.split(',')[1], do_dt = line.split(',')[2], pu_id = line.split(',')[7], do_id = line.split(',')[8], amount = line.split(',')[16] ));
        # # Create dataFrame from non-empty splitted lines
        # df_pu_do_dt_id_and_amount = spark.createDataFrame( fields_in_lines )
        # df_pu_do_dt_id_and_amount.show(10)
        # #Filter out rows that don't match user's pickup-ID and dropoff-ID from dataFrame
        # df_pu_do_dt_id_and_amount = df_pu_do_dt_id_and_amount.where( col('pu_id') == str(user_puid) ).where( col('do_id') == str(user_doid) )
        # df_pu_do_dt_id_and_amount.show(10)

        #Filter out lines that are not within the user's time radius
        lines_with_hour = lines_with_piud_doid.filter(lambda line: filter_dates(line.split(",")[1], user_weekday, user_hour, user_minutes))

        # ((weekday, hour, minute, PU_ID, DO_ID), (vendorID, duration, Total_Ammount))
        organized_lines = lines_with_hour.map(lambda line: create_key_value(line))
        
        #Reduce everything by key returning a 3 column tuple
        #(vendor_ID, list of durations, list of amounts)
        grouped = organized_lines.reduceByKey(lambda accum, elem: (accum[0] + elem[0], accum[1] + elem[1]))
        
        for k, v in grouped.collect():
            # pick_up_taxi_zones = locations.loc[locations["LocationID"] == int(k[1]), ["Zone", "Borough"]]
            # drop_off_taxi_zones = locations.loc[locations["LocationID"] == int(k[2]), ["Zone", "Borough"]]
            average_duration = np.mean(v[0])
            average_amount = np.mean(v[1])
            print(v)
            print("\nFor {} at {0:02d}:{0:02d}, a trip from (ID: {}) to (ID: {}) takes an average of {} minutes and costs about ${}"\
            .format(k[0], user_hour, user_minutes,k[1], k[2], average_duration, average_amount))

        sc.stop()
    except:
        traceback.print_exc()
        sc.stop()



user_weekday, user_puid, user_doid, user_hour, user_minutes = get_user_options()

create_inverted_index(int(user_weekday), user_puid, user_doid, int(user_hour), int(user_minutes))
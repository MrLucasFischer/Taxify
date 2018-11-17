import pyspark
from pyspark.sql import SparkSession
import traceback
import datetime
from datetime import datetime as dt
import calendar
import time
import numpy as np

spark = SparkSession.builder.master('local[*]').appName('taxify').getOrCreate()
sc = spark.sparkContext

#PU/DO zone ids range from 1 to 265, see taxi_zone_lookup.csv

#Main implementation

def create_key_value(line):
    """
        Function that creates the key value structure for every line of interest

        Params:
            A non-filtered raw line of the CSV file
    """
    splitted = line.split(",")
    pick_up_datetime = splitted[1]

    week_day = (calendar.day_name[dt.strptime(splitted[1], '%Y-%m-%d %H:%M:%S').weekday()]).lower()
    time = pick_up_datetime[11:13]

    pick_up_id = splitted[7]
    dropoff_up_id = splitted[8]

    key = (week_day, time, pick_up_id, dropoff_up_id)

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


def create_inverted_index(filename = 'yellow_tripdata_2018-01_sample.csv'):
    """
        Function that creates the inverted index. This function holds the main implementation of spark code to create the inverted index

        Params:
            user_weekday - An integer ranging from 1 to 7 representing the day of the week chosen by the user
            user_puid - An integer ranging from 1 to 265 representing the pick-up zone ID chosen by the user
            user_doid - An integer ranging from 1 to 265 representing the drop off zone ID chosen by the user
            user_hour - An integer representing the hour chosen by the user
            user_hour - An integer representing the minutes chosen by the user
            filename - Name of the file to read the information from
    """

    try :
        time_before = dt.now()
        lines = sc.textFile(filename) #read csv file (change this to the full dataset instead of just the sample)
        first_line = lines.first()

        #Filtering out the first line, empty lines
        non_empty_lines = lines.filter(lambda line: len(line) > 0 and line != first_line)
        
        # ((weekday, time, PU_ID, DO_ID), (duration, Total_Ammount))
        organized_lines = non_empty_lines.map(lambda line: create_key_value(line))
        
        #Reduce everything by key returning a 3 column tuple
        #(vendor_ID, list of durations, list of amounts)
        grouped = organized_lines.reduceByKey(lambda accum, elem: (accum[0] + elem[0], accum[1] + elem[1]))

        #Obtain the average of the 
        grouped_with_averages = grouped.mapValues(lambda tup: (np.mean(tup[0]), np.mean(tup[1])))
        
        grouped_with_averages.saveAsTextFile("spark_rdd_results/inverted_index")

        time_after = dt.now()
        seconds = (time_after - time_before).total_seconds()
        print("Execution time {} seconds".format(seconds))

        sc.stop()
    except:
        traceback.print_exc()
        sc.stop()


create_inverted_index()
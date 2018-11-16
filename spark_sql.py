from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.functions import sum as sum_
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

#PU/DO zone ids range from 1 to 265, see taxi_zone_lookup.csv
#date(position 1, maybe split datetime into weekday and time), PU_ID (position 7), DO_ID (position 8), totalammount (position 16)

#Main implementation

def get_duration(pick_up_datetime, drop_off_datetime):
    """
        Get duration of trip in minutes from pick up and drop off times
    """

    d1 = time.mktime(dt.strptime(drop_off_datetime, '%Y-%m-%d %H:%M:%S').timetuple())
    d2 = time.mktime(dt.strptime(pick_up_datetime, '%Y-%m-%d %H:%M:%S').timetuple())
    return int((d1 - d2) / 60)



def convert_to_weekday(date):
    """
        Function that converts a date to weekday
    """
    date_obj = dt.strptime(date, '%Y-%m-%d %H:%M:%S')
    return (calendar.day_name[date_obj.weekday()]).lower()



def convert_to_hour(date):
    """
        Function that gets the hour from a date
    """
    return date[11:13]



def create_inverted_index(user_weekday = 1, user_puid = 41, user_doid = 24, user_hour = 0, user_minutes = 21, filename = 'yellow_tripdata_2018-01_sample.csv'):
    try :
        lines = sc.textFile(filename) #read csv file (change this to the full dataset instead of just the sample) (this is local to my machine)
        first_line = lines.first()

        #USER DEFINED FUNCTION CREATION

        # convert_to_weekday_udf = udf(lambda pickup_date: convert_to_weekday(pickup_date), StringType())
        spark.udf.register("convert_to_weekday_udf", lambda pickup_date: convert_to_weekday(pickup_date), StringType())
        spark.udf.register("convert_to_hour_udf", lambda pickup_date: convert_to_hour(pickup_date), StringType())
        spark.udf.register("convert_to_duration", lambda pickup_date, dropoff_date: get_duration(pickup_date, dropoff_date), IntegerType())

        #Filtering out the first line, empty lines
        non_empty_lines = lines.filter(lambda line: len(line) > 0 and line != first_line)

        # Create a Row object with pickup_datetime, dropoff_datetime, pickup_id, dropoff_id and amount
        fields = non_empty_lines.map(lambda line : Row(pickup_datetime = line.split(',')[1], dropoff_datetime = line.split(',')[2], pickup_id = line.split(',')[7], dropoff_id = line.split(',')[8], amount = line.split(',')[16]));
        
        # Transform fields to dataframe
        fields_df = spark.createDataFrame(fields)

        #Create a temporary table called fields_table
        fields_df.createOrReplaceTempView("fields_table")

        inverted_index = spark.sql(
            """SELECT convert_to_weekday_udf(pickup_datetime) AS weekday,
             convert_to_hour_udf(pickup_datetime) AS hour,
             pickup_id,
             dropoff_id,
             AVG(convert_to_duration(pickup_datetime, dropoff_datetime)) AS average_duration,
             AVG(amount) AS average_amount 
             FROM fields_table 
             GROUP BY weekday, hour, pickup_id, dropoff_id"""
        )
        
        inverted_index.show(10)

        sc.stop()
    except:
        traceback.print_exc()
        sc.stop()



# user_weekday, user_puid, user_doid, user_hour, user_minutes = get_user_options()

# create_inverted_index(int(user_weekday), user_puid, user_doid, int(user_hour), int(user_minutes))

create_inverted_index()
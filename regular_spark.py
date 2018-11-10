import pyspark
import traceback
from datetime import datetime
import calendar
import time

sc = pyspark.SparkContext('local[*]') #Create spark context

#PU/DO zone ids range from 1 to 265, see taxi_zone_lookup.csv
#date(position 2, maybe split datetime into weekday and time), PU_ID (position 7), DO_ID (position 8), totalammount (position 16)

#Para converter a data que vem no ficheiro csv para um weekday fazer o seuginte
#date_str = data que vem do ficheiro
#date_obj = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
#weekday = lower(calendar.day_name[date_obj.weekday()])


#Main implementation

def get_user_options():
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
        weekday = input("Please insert you weekday (1- Monday, 2- Tuesday, ..., 7- Sunday):")
        try:
            if(int(weekday) >= 1 and int(weekday) <= 7):
                weekday_correct = True
        except:
            #User didn't sent us a number
            print("Please insert a number between 1 - 7\n")

    #Continue asking the user until he/she gives us an hour
    while(not time_correct):
        time_input = input("Please insert the desired time (hh:mm):")
        try:
            user_time = time.strptime(time_input, '%H:%M') # Check time is in proper format
            time_correct = True
            hour = user_time.tm_hour #Get hour
            minutes = user_time.tm_min #Get minutes

        except:
            #User didn't sent us a number
            print("Please insert a time in the format hh:mm where hh (00-23) and mm (00:59) \n")    



    #Continue asking the user until he/she gives us a number between 1 and 265
    while(not pickup_correct):
        pickup_id = input("Please insert you Pick-Up location ID (1 - 265):")
        try:
            if(int(pickup_id) >= 1 and int(pickup_id) <= 265):
                pickup_correct = True
        except:
            #User didn't sent us a number
            print("Please insert a number between 1 - 265\n")

        

    #Continue asking the user until he/she gives us a number between 1 and 265
    while(not dropoff_correct):
        dropoff_id = input("Please insert you Drop-Off location ID (1 - 265):")
        try:
            if(int(dropoff_id) >= 1 and int(dropoff_id) <= 265):
                dropoff_correct = True
        except:
            #User didn't sent us a number
            print("Please insert a number between 1 - 265\n")


    return(weekday, pickup_id, dropoff_id, hour, minutes)



user_weekday ,user_puid, user_doid, user_hour, user_minutes = get_user_options()

try :
    lines = sc.textFile('yellow_tripdata_2018-01_sample.csv') #read csv file (change this to the full dataset instead of just the sample) (this is local to my machine)

    non_empty_lines = lines.filter(lambda line: len(line) > 0 and line != lines.first())
    print(non_empty_lines.take(10))
    #Filter out empty lines and the first line
# and line.split(",")[7] == str(user_puid) and line.split(",")[8] == str(user_doid)
    # print(non_empty_lines.count())

    # organized_lines = non_empty_lines.map(lambda line: ((line.split(",")[2], line.split(",")[7], line.split(",")[8]), line.split(",")[16])) # (Pickup-Date, PU_ID, DO_ID, Total_Ammount)
    # grouped = organized_lines.groupByKey().mapValues(list)

    # for a in grouped.take(10):
        # print(a)

    # sc.stop()
except:
    traceback.print_exc()
    sc.stop()
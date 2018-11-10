import pyspark
import traceback
import datetime
from datetime import datetime as dt
import calendar
import time

sc = pyspark.SparkContext('local[*]') #Create spark context

#PU/DO zone ids range from 1 to 265, see taxi_zone_lookup.csv
#date(position 1, maybe split datetime into weekday and time), PU_ID (position 7), DO_ID (position 8), totalammount (position 16)

#Para converter a data que vem no ficheiro csv para um weekday fazer o seuginte
#date_str = data que vem do ficheiro
#date_obj = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
#weekday = lower(calendar.day_name[date_obj.weekday()])


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


def filter_dates(input_date_time, user_weekday, user_hour, user_minutes):
    """
        Predicate function that returns true if input_date is within 30 minutes radius of user's desired time, false otherwise

        Params:
            input_date - String in YYYY-MM-DD HH:MM format
    """
    #First check if input_date week day is at the maximum one more day than users desired time
    date_obj = dt.strptime(input_date_time, '%Y-%m-%d %H:%M:%S')

    input_weekday = date_obj.weekday()
    user_weekday -= 1

    input_date = input_date_time[0:10] #ver o substring
    user_date = dt.strptime(input_date + " {}:{}:00".format(user_hour, user_minutes), '%Y-%m-%d %H:%M:%S')

    if(user_hour == 23 and user_minutes > 29):
        user_date = dt.strptime(input_date + " {}:{}:00".format(user_hour, user_minutes), '%Y-%m-%d %H:%M:%S') - datetime.timedelta(days = 1)

    if(input_weekday == user_weekday or (input_weekday == 0 and user_weekday == 6) or (input_weekday == user_weekday + 1)):
        time_plus_30_min = (user_date + datetime.timedelta(minutes = 30))
        return user_date <= date_obj <= time_plus_30_min
    else:
        return False



#TODO ADD default params to weekday, puid, doid, hour and minutes
def create_inverted_index(user_weekday, user_puid, user_doid, user_hour, user_minutes, filename = 'yellow_tripdata_2018-01_sample.csv'):
    try :
        lines = sc.textFile(filename) #read csv file (change this to the full dataset instead of just the sample) (this is local to my machine)
        first_line = lines.first()

        #Filtering out the first line, empty lines
        non_empty_lines = lines.filter(lambda line: len(line) > 0 and line != first_line)

        #Filter out lines that don't match user's pickup-ID and dropoff-ID
        lines_with_piud_doid = non_empty_lines.filter(lambda line: line.split(",")[7] == str(user_puid) and line.split(",")[8] == str(user_doid))

        #Filter out lines that are not within the user's time radius
        lines_with_hour = lines_with_piud_doid.filter(lambda line: filter_dates(line.split(",")[1], user_weekday, user_hour, user_minutes))

        organized_lines = lines_with_hour.map(lambda line: ((line.split(",")[1], line.split(",")[7], line.split(",")[8]), line.split(",")[16])) # (Pickup-Date, PU_ID, DO_ID, Total_Ammount)
        grouped = organized_lines.groupByKey().mapValues(list)

        for a in grouped.take(10):
            print(a)

        sc.stop()
    except:
        traceback.print_exc()
        sc.stop()

user_weekday, user_puid, user_doid, user_hour, user_minutes = get_user_options()

create_inverted_index(int(user_weekday), user_puid, user_doid, int(user_hour), int(user_minutes))

#2018-01-01 00:21:05 41 24
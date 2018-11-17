import pyspark
from pyspark.sql import SparkSession
import traceback
import calendar

spark = SparkSession.builder.master('local[*]').appName('taxify').getOrCreate()
sc = spark.sparkContext

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

    #Continue asking the user until he/she gives us a weekday
    while(not weekday_correct):
        weekday = input("\nPlease insert you weekday (1- Monday, 2- Tuesday, ..., 7- Sunday): ")
        try:
            if(int(weekday) >= 1 and int(weekday) <= 7):
                weekday_correct = True
            else:
                print("\nPlease insert a number between 1 - 7\n")
        except:
            #User didn't sent us a number
            print("\nPlease insert a number between 1 - 7\n")

    #Continue asking the user until he/she gives us an hour
    while(not time_correct):
        time_input = input("\nPlease insert the desired hour: ")
        try:
            if(int(time_input) >= 0 and int(time_input) <=23):
                time_correct = True
                hour = time_input
            else:
                print("\Hour should be an integer between 0 and 23 \n")
        except:
            #User didn't sent us a number
            print("\Hour should be an integer between 0 and 23 \n")    



    #Continue asking the user until he/she gives us a number between 1 and 265
    while(not pickup_correct):
        pickup_id = input("\nPlease insert you Pick-Up location ID (1 - 265): ")
        try:
            if(int(pickup_id) >= 1 and int(pickup_id) <= 265):
                pickup_correct = True
            else:
                print("\nPlease insert a number between 1 - 265\n")
        except:
            #User didn't sent us a number
            print("\nPlease insert a number between 1 - 265\n")

        

    #Continue asking the user until he/she gives us a number between 1 and 265
    while(not dropoff_correct):
        dropoff_id = input("\nPlease insert you Drop-Off location ID (1 - 265): ")
        try:
            if(int(dropoff_id) >= 1 and int(dropoff_id) <= 265):
                dropoff_correct = True
            else:
                print("\nPlease insert a number between 1 - 265\n")
        except:
            #User didn't sent us a number
            print("\nPlease insert a number between 1 - 265\n")


    return(weekday, pickup_id, dropoff_id, hour)



def transform_line(line):
    """
        Function that transforms every String line in the inverted index into a searchable array
    """
    stripped_line = line.replace("(", "").replace(")", "").replace(" ", "").replace("\'", "")
    return stripped_line.split(",")


    
def search_index(user_weekday = 1, user_puid = "41", user_doid = "24", user_hour = 0, filename = "spark_rdd_results/inverted_index"):
    try:
        lines = sc.textFile(filename) #read the inverted index created previously
        
        #Transform each line into an array
        transform_lines = lines.map(lambda line: transform_line(line)) 
        
        #Filter out the lines that don't match the user's desired pick up zone and drop off zone
        lines_with_puid_doid = transform_lines.filter(lambda arr: arr[2] == str(user_puid) and arr[3] == str(user_doid))
        
        #Filter out the lines that don't match the user's desired weekday
        lines_with_weekday = lines_with_puid_doid.filter(lambda arr: arr[0] == (calendar.day_name[user_weekday - 1]).lower())
        
        #Filter out the lines that don't match user's desired hour
        lines_with_hour = lines_with_weekday.filter(lambda arr: int(arr[1]) == user_hour)
        
        for result in lines_with_hour.collect():
            print(result)

    except:
        traceback.print_exc()
        sc.stop()

user_weekday, user_puid, user_doid, user_hour = get_user_options()

search_index(int(user_weekday), user_puid, user_doid, int(user_hour))
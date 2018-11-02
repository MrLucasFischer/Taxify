import pyspark
import traceback

sc = pyspark.SparkContext('local[*]') #Create spark context

#PU/DO zone ids range from 1 to 265, see taxi_zone_lookup.csv
#Main implementation

#ESTA PARTE FICA PARA DEPOIS, POR ENQUANTO USAMOS UNS DADOS QUAIS-QUERES DEPOIS E QUE NOS PREOCUPAMOS COM USER INTERFACE
def get_user_options():
    pickup_correct = False
    dropoff_correct = False
    pickup_id = ""
    dropoff_id = ""

    #Continue asking the user until he/she gives us a number between 1 and 265
    while(not pickup_correct):
        pickup_id = input("Please insert you Pick-Up location ID (1 - 265):")
        try:
            if(int(pickup_id) >= 1 and int(pickup_id) <= 265):
                pickup_correct = True
        except:
            #User didn't sent us a number
            print("Please insert a number between 1 -265\n")
        

    #Continue asking the user until he/she gives us a number between 1 and 265
    while(not dropoff_correct):
        dropoff_id = input("Please insert you Drop-Off location ID (1 - 265):")
        try:
            if(int(dropoff_id) >= 1 and int(dropoff_id) <= 265):
                dropoff_correct = True
        except:
            #User didn't sent us a number
            print("Please insert a number between 1 -265\n")

    return(pickup_id, dropoff_id)



print(get_user_options())

try :
    lines = sc.textFile('yellow_tripdata_2018-01_sample.csv') #read csv file (change this to the full dataset instead of just the sample) (this is local to my machine)
    for a in lines.take(100):
        print(a)
    sc.stop()
except:
    traceback.print_exc()
    sc.stop()
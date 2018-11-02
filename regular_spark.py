import pyspark
import traceback

sc = pyspark.SparkContext('local[*]') #Create spark context

#PU/DO zone ids range from 1 to 265, see taxi_zone_lookup.csv
#date(position 2, maybe split datetime into weekday and time), PU_ID (position 7), DO_ID (position 8), totalammount (position 16)

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
    first_line = lines.first()
    non_empty_lines = lines.filter(lambda line: len(line) > 0 and line != first_line)  #Filter out empty lines and the first line
    #maybe we filter the lines that don't contain the options the user sent us like the PU_ID, DO_ID etc. 

    organized_lines = non_empty_lines.map(lambda line: (line.split(",")[2], line.split(",")[7], line.split(",")[8], line.split(",")[16])) # (Date, PU_ID, DO_ID, Total_Ammount)
    grouped = organized_lines.groupBy(lambda tuple: (tuple[0], tuple[1], tuple[2]))

    for a in grouped.take(10):
        #See how to iterate over values
        print(a)

    # sc.stop()
except:
    traceback.print_exc()
    sc.stop()
%%file reducer.py
#!/usr/bin/env python
import sys
import numpy as np
import datetime
from datetime import datetime as dt

average_duration = []
average_amount = []
max_time = 0.0
key_name = ""

for line in sys.stdin:
    key, duration, amount, beforeT = line.split("\t")
     
    if(key_name == ""):
        key_name = key
        
    average_duration.append(int(duration))
    average_amount.append(float(amount))
    
    beforeDate = dt.strptime(beforeT.split(".")[0], '%Y-%m-%d %H:%M:%S')
    
    afterT = datetime.datetime.now()
    diffT = afterT - beforeDate
    max_time = max(max_time, (diffT.microseconds / 1000))
    
print(key_name, (np.mean(average_duration), np.mean(average_amount), max_time))
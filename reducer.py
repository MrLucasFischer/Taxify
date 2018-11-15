%%file reducer.py
#!/usr/bin/env python
import sys
import numpy as np

average_duration = []
average_amount = []
key_name = ""

for line in sys.stdin:
    key, duration, amount = line.split("\t")
    
    if(key_name == ""):
        key_name = key
        
    average_duration.append(int(duration))
    average_amount.append(float(amount))
    
print(key_name, (np.mean(average_duration), np.mean(average_amount)))
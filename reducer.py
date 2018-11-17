%%file reducer.py
#!/usr/bin/env python

import sys
import numpy as np
import datetime
from datetime import datetime as dt

inverted_index = {}

for line in sys.stdin:
    key, duration, amount = line.split("\t")
    
    if key in inverted_index:
        index_entry = inverted_index[key]
        index_entry[0].append(int(duration))
        index_entry[1].append(float(amount))
    else:
        inverted_index[key] = ([int(duration)], [float(amount)])
    
    
for key, value in inverted_index.items():
    print(key, (np.mean(value[0]), np.mean(value[1])))
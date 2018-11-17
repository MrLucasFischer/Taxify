%%file mapper_client.py
#!/usr/bin/env python

import sys
import traceback
import datetime
from datetime import datetime as dt
import calendar
import time
import numpy as np

for line in sys.stdin:
    print("{},{}".format(line, 1))
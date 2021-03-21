import sys
import time
import pandas as pd
from datetime import datetime

# 计算天数并加入表中
path = '/home/hadoop/data/test/test.csv'
data = pd.read_csv(path, dtype='object', error_bad_lines=False, engine='python')

days_list = []
for m, n in zip(data['OutHosDate'], data['InHosDate']):
    try:
        Out = datetime.strptime(str(m), "%Y-%m-%d %H:%M:%S").date()
        In = datetime.strptime(str(n), "%Y-%m-%d %H:%M:%S").date()
        tt = (Out - In).days
    except:
        tt = 0
    days_list.append(tt)

# 插入表中
data.insert(loc=10, column='DaysInHos', value=days_list)

data.to_csv(path, index=False)

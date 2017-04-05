#!/Users/tony/anaconda/bin/python
# -*- coding: utf-8 -*-
# @Date    : 2017-03-31 14:28:07
# @Author  : Tony (cz9x@qq.com)

import pandas as pd
import numpy as np
from collections import Counter

fname = "../data/JData_Action_201602.csv"

reader = pd.read_csv(fname, header=0, iterator=True)
chunk = []

chunk = reader.get_chunk(10000)[['user_id', 'type']]
chunk.append(chunk)

behavior_id = chunk.type.astype(int)
type_cnt = Counter(behavior_id)

print type_cnt[1]
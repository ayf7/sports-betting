#!/usr/bin/env python
import sys, os
parent_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(parent_directory)

import pandas as pd

file_directory = os.path.dirname(__file__)
data_directory = os.path.join(file_directory, '../data/')

file = 'xTe.csv'

df = pd.read_csv(os.path.join(data_directory, file))
print(df.shape)
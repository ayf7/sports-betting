#!/usr/bin/env python
import sys, os
import pickle
import numpy as np
import matplotlib.pyplot as plt

parent_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(parent_directory)
file_directory = os.path.dirname(__file__)
data_directory = os.path.join(file_directory, '../data/')

combined_files = ["2022-2023_.pkl"]

for c in combined_files:
    with open(os.path.join(data_directory, c), 'rb') as file:
        features, labels, dates, game_ids = pickle.load(file)
        print(np.array(labels).shape)
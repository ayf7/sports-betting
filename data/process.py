#!/usr/bin/env python
import sys, os
parent_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(parent_directory)

import pickle
import numpy as np
import matplotlib.pyplot as plt

from misc.logger import Logger

log = Logger("Processor")

file_directory = os.path.dirname(__file__)
data_directory = os.path.join(file_directory, '../data/')

combined_files = ["2020-2021.pkl", "2021-2022.pkl", "2022-2023.pkl", "2023-2024.pkl"]

aggregate_features = []
aggregate_labels = []
aggregate_dates = []
aggregate_game_ids = []
for c in combined_files:
    with open(os.path.join(data_directory, c), 'rb') as file:
        features, labels, dates, game_ids = pickle.load(file)
        aggregate_features += features
        aggregate_labels += labels
        aggregate_dates += dates
        aggregate_game_ids += game_ids

xTr = np.array(aggregate_features)
yTr = np.array(aggregate_labels)
games_ids = np.array(aggregate_game_ids)

with open(os.path.join(data_directory, 'xTr.pkl'), 'wb') as file:
    log.info(f"Dumped [xTr]. Shape: {xTr.shape}")
    pickle.dump(xTr, file)
  
with open(os.path.join(data_directory, 'yTr.pkl'), 'wb') as file:
    log.info(f"Dumped [yTr]. Shape: {yTr.shape}")
    pickle.dump(yTr, file)

with open(os.path.join(data_directory, 'game_ids.pkl'), 'wb') as file:
    log.info(f"Dumped [game_ids]. Shape: {games_ids.shape}")
    pickle.dump(games_ids, file)


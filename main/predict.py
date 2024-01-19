#!/usr/bin/env python
import sys, os
parent_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(parent_directory)

import torch
import torch.nn as nn
import torch.optim as optim
import numpy as np
import pickle
from scrape.today_scraper import TodaysGameScraper
from sklearn.model_selection import train_test_split
from models.nn import StandardNN, train_model
from data.lib import to_numpy

file_directory = os.path.dirname(__file__)
data_directory = os.path.join(file_directory, '../data/')

# Data Loading
xTr, yTr, xTe = to_numpy('features.csv', 'scores.csv', 'daily_features.csv')
yTr = (yTr[:,0] - yTr[:,1]).reshape((-1, 1))
xTr = torch.from_numpy(xTr).float()
yTr = torch.from_numpy(yTr).float()
xTe = torch.from_numpy(xTe).float()

# Model Parameters
input_size = len(xTr[0])
hidden_sizes = [2, 1, 0.5]
for i, coef in enumerate(hidden_sizes):
    hidden_sizes[i] = int(coef * input_size)
output_size = len(yTr[0])

# Model Selection
model = StandardNN(input_size, hidden_sizes, output_size)
criterion = nn.MSELoss() 
optimizer = optim.Adam(model.parameters(), lr=0.001)

# Model Training
USE_PARAMS = False

if USE_PARAMS:
    model = torch.load('model.pth')
else:
    train_model(model, criterion, optimizer, xTr, yTr, num_epochs=2400, save_dest='model2.pth')

# Obtaining today's games
todays_games = TodaysGameScraper(verbose=True)

# Predicting models
with torch.no_grad():
    y_predict = model(xTe)
print(y_predict.numpy())
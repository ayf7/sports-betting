#!/usr/bin/env python
import sys, os
parent_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(parent_directory)

import torch
import torch.nn as nn
import torch.optim as optim
from scrape.generator import TodayGenerator
from models.nn import StandardNN, train_model
from data.lib import to_numpy, csv_to_dataframe
from datetime import datetime

file_directory = os.path.dirname(__file__)
data_directory = os.path.join(file_directory, '../data/')

# Data Loading
xTr, yTr, xTe = to_numpy('xTr.csv', 'yTr.csv', 'xTe.csv')
yTr = (yTr[:,0] - yTr[:,1]).reshape((-1, 1))
xTr = torch.from_numpy(xTr).float()
yTr = torch.from_numpy(yTr).float()
xTe = torch.from_numpy(xTe).float()

print(xTr.shape, yTr.shape, xTe.shape)
data = csv_to_dataframe("xTe_raw.csv")

# Model Parameters
input_size = len(xTr[0])
hidden_sizes = [2, 2, 1, 1, 0.5]
for i, coef in enumerate(hidden_sizes):
    hidden_sizes[i] = int(coef * input_size)
output_size = len(yTr[0])

# Model Selection
model = StandardNN(input_size, hidden_sizes, output_size)
criterion = nn.MSELoss() 
optimizer = optim.Adam(model.parameters(), lr=0.001)

# Model Training
def round_num(num:float) -> int:
    if 0 < num and num < 1:
        return 1
    elif 0 > num and num > -1:
        return -1
    return round(num)

games = [[] for _ in range(len(xTe))]

USE_PARAMS = False

for _ in range(20):
    if USE_PARAMS:
        model = torch.load('model.pth')
    else:
        model = StandardNN(input_size, hidden_sizes, output_size)
        criterion = nn.MSELoss() 
        optimizer = optim.Adam(model.parameters(), lr=0.001)
        train_model(model, criterion, optimizer, xTr, yTr, num_epochs=3000, save_dest='model2.pth')

    # Predicting models
    with torch.no_grad():
        y_predict = model(xTe)

    y_predict = y_predict.numpy()

    print()
    for i in range(len(y_predict)):
        games[i].append(round_num(y_predict[i][0]))
        print(games[i])
    print()
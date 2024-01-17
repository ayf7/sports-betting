#!/usr/bin/env python
import torch
import torch.nn as nn
import torch.optim as optim
import numpy as np
import pickle
from sklearn.model_selection import train_test_split
import matplotlib.pylab as plt

import sys, os
parent_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(parent_directory)

file_directory = os.path.dirname(__file__)
data_directory = os.path.join(file_directory, '../data/')

class StandardNN(nn.Module):
    def __init__(self, input_size, hidden_sizes, output_size):
        super(StandardNN, self).__init__()

        layers = []
        sizes = [input_size] + hidden_sizes + [output_size]

        for i in range(len(sizes) - 1):
            layers.append(nn.Linear(sizes[i], sizes[i + 1]))
            layers.append(nn.ReLU())

        # Remove the last ReLU layer
        layers.pop()

        self.network = nn.Sequential(*layers)

    def forward(self, x):
        return self.network(x)

def train_model(model:nn.Module, criterion:nn.Module, optimizer:optim, xTr, yTr, num_epochs:int, save_dest:str=None) -> None:
    """
    Trains [model] with the specified parameters. if [save_dest] is specified,
    saves the weights.
    """
    for epoch in range(num_epochs):

        outputs = model(xTr)
        loss = criterion(outputs, yTr)

        optimizer.zero_grad()
        loss.backward()
        optimizer.step()

        if (epoch+1) % 100 == 0:
            print(f'Epoch [{epoch+1}/{num_epochs}], Loss: {loss.item():.4f}')
    
    # save if specified
    if save_dest:
        torch.save(model, save_dest)
    
def validate_model(model:nn.Module, xTe:torch.Tensor, yTe:torch.Tensor, threshold:int, verbose:bool=False) -> None:
    """
    Validates [model] with testing dataset. [threshold] determines which
    predictions are kept wrt absolute value. If [verbose], prints out all
    statistics. Prints prediction/discarded/aggregate accuracy at the very end.
    """
    # Testing
    with torch.no_grad():
        y_predict = model(xTe)

    # Convert tensors to numpy arrays
    yTe = yTe.numpy()
    y_predict = y_predict.numpy()
    result = np.concatenate((yTe, y_predict), axis=1).astype(int)

    predict_correct = 0
    predict_total = 0
    discard_correct = 0

    for r in result:

        # result meets threshold requirements to place a bet (predicted differential <= [threshold])
        if abs(r[1]) >= threshold:
            predict_total += 1
            if (r[0] * r[1] > 0): # checks if the actual vs. predicted have the same size
                predict_correct += 1
                print("\033[92m", f"{   str(r[0]).strip()}\t{str(r[1]).strip()}", "\tHIT\033[0m")
            else:
                print("\033[91m", f"{   str(r[0]).strip()}\t{str(r[1]).strip()}", "\tMISS\033[0m")
        else:
            if (r[0] * r[1] > 0):
                discard_correct += 1
                print(f"{   str(r[0]).strip()}\t{str(r[1]).strip()}", "\tHIT")
            else:
                print(f"{   str(r[0]).strip()}\t{str(r[1]).strip()}", "\tMISS")
    
    print(f"Prediction Accuracy: {predict_correct}/{predict_total} [{round(predict_correct/predict_total*100, 2)}]")

    aggregate_total = len(result)
    discard_total = aggregate_total - predict_total
    print(f"Discarded Accuracy: {discard_correct}/{discard_total} [{round(discard_correct/discard_total*100, 2)}]")

    aggregate_correct = predict_correct + discard_correct
    print(f"Aggregate Accuracy: {aggregate_correct}/{aggregate_total} [{round(aggregate_correct/aggregate_total*100, 2)}]")


if __name__ == '__main__':

    with open(os.path.join(data_directory, 'xTr.pkl'), 'rb') as file:
        xTr = pickle.load(file)

    with open(os.path.join(data_directory, 'yTr.pkl'), 'rb') as file:
        yTr = pickle.load(file)

    yTr = yTr[:,0] - yTr[:,1]
    yTr = yTr.reshape(-1, 1)

    # Split into training and testing sets
    # xTr, xTe, yTr, yTe = train_test_split(xTr, yTr, test_size=0.1) # random_state=7

    days_past = 50
    n = 100
    xTr, yTr = xTr[:-days_past], yTr[:-days_past]
    print(xTr.shape)
    xTe, yTe = xTr[-n:], yTr[-n:]
    xTr, yTr = xTr[:-n], yTr[:-n]

    # Convert into PyTorch tensors
    xTr = torch.from_numpy(xTr).float()
    yTr = torch.from_numpy(yTr).float()
    xTe = torch.from_numpy(xTe).float()
    yTe = torch.from_numpy(yTe).float()

    input_size = len(xTr[0])

    hidden_sizes = [2, 2, 1, 1, 0.5, 0.5]

    for i, coef in enumerate(hidden_sizes):
        hidden_sizes[i] = int(coef * input_size)
    output_size = len(yTr[0])

    model = StandardNN(input_size, hidden_sizes, output_size)
    criterion = nn.MSELoss() 
    optimizer = optim.Adam(model.parameters(), lr=0.001)

    train_model(model, criterion, optimizer, xTr, yTr, num_epochs=1500, save_dest='model.pth')

    validate_model(model, xTe, yTe, 10, verbose=True)
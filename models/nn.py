#!/usr/bin/env python
import sys, os
parent_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(parent_directory)

import torch
import torch.nn as nn
import torch.optim as optim
import numpy as np
import pickle
import matplotlib.pylab as plt
from torch.nn.utils import clip_grad_norm_

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
    error_threshold = 5
    error_count = 0
    
    for epoch in range(num_epochs):

        outputs = model(xTr)
        loss = criterion(outputs, yTr)

        optimizer.zero_grad()
        loss.backward()
        clip_grad_norm_(model.parameters(), 1.0) # gradient clipping
        optimizer.step()

        if (epoch+1) % 100 == 0:
            if loss.item() < error_threshold:
                error_count += 1
                if error_count == 10:
                    print("Exiting.")
                    break
            else:
                error_count = 0
            print(f'Epoch [{epoch+1}/{num_epochs}], Loss: {loss.item():.4f} [{error_count}]')
    
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
    result = np.concatenate((yTe, y_predict), axis=1)

    # Rounding function: deal with special case 0 < x < 1 -> want to round it to x = 1
    rounded_array = np.round(result)
    rounded_array[(0 < result) & (result < 1)] = 1
    rounded_array[(-1 < result) & (result < 0)] = -1
    result = rounded_array.astype(int)

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
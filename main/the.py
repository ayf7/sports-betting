#!/usr/bin/env python
import os
from config import data_directory
import pandas as pd
from data.lib import aggregate_files, aggregate_to_features, to_numpy

if __name__ == "__main__":
    features, scores = to_numpy("features.csv", "scores.csv")
    print(features)
    print(scores)
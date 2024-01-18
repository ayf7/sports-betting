#!/usr/bin/env python
"""
Libraries supporting commonly used functions
"""
import os
from main.config import data_directory
import pandas as pd
import numpy as np

def csv_to_dataframe(file:str) -> pd.DataFrame:
    """
    Given a file name in data/, return the csv file.
    """
    if not file.endswith('.csv'):
        file += '.csv'
    return pd.read_csv(os.path.join(data_directory, file))


def dataframe_to_csv(df:pd.DataFrame, dest:str) -> None:
    """
    Saves the data frame [df] to [dest] in the data folder.
    """
    dest = os.path.join(data_directory, dest)
    df.to_csv(dest, index=False)


def aggregate_files() -> None:
    """
    Aggregates all year csv's into one, in chronological order.
    """

    combined_files = ["2020-2021", "2021-2022", "2022-2023", "2023-2024"]

    aggregate_df = pd.DataFrame()

    for file in combined_files:
        location = os.path.join(data_directory, f'{file}.csv')
        df = pd.read_csv(location)
        aggregate_df = pd.concat([aggregate_df, df])

    dataframe_to_csv(aggregate_df, "aggregate.csv")

def aggregate_to_features() -> None:
    """
    Generates [xTr] and [yTr]. 
    """
    df = csv_to_dataframe("aggregate.csv")

    # Remove categorical features
    suffixes_to_remove = ['_ID', '_NAME']
    cols_to_remove = ['DATE']
    columns_to_remove = [col for col in df.columns if any(col.endswith(suffix) for suffix in suffixes_to_remove) or col in cols_to_remove]
    df = df.drop(columns=columns_to_remove)

    # Split into input/output
    df_features = df.drop(columns=['HOME_SCORE', 'ROAD_SCORE'])
    df_scores = df[['HOME_SCORE', 'ROAD_SCORE']]

    dataframe_to_csv(df_features, dest='features.csv')
    dataframe_to_csv(df_scores, dest='scores.csv')

def to_numpy(*args:str) -> np.ndarray:
    """
    Wrapper that extracts
    """
    outputs = []
    for file in args:
        df = csv_to_dataframe(file)
        outputs.append(df.values) # appends ndarray
    
    return tuple(outputs)
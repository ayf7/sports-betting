#!/usr/bin/env python
import sys, os
parent_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(parent_directory)

from scrape.data_generator import DataHandler
from scrape.parameters.info import seasons
from datetime import datetime

file_directory = os.path.dirname(__file__)
data_directory = os.path.join(file_directory, '../data/')

if __name__ == '__main__':
    data_handler = DataHandler("2023-2024.pkl")
    data_handler.update(end_date=datetime.today().strftime("%m/%d/%y"))
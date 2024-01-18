#!/usr/bin/env python
import sys, os
parent_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(parent_directory)

from scrape.data_generator import DataHandler
from parameters.info import seasons
from data.lib import aggregate_files

from datetime import datetime
import argparse

file_directory = os.path.dirname(__file__)
data_directory = os.path.join(file_directory, '../data/')
    

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Generate or update data based on season.')
    parser.add_argument('season', help='Season in the format "YYYY-YYYY"')
    parser.add_argument('command', choices=['generate', 'update'], help='Command to perform ("generate" or "update")')
    args = parser.parse_args()

    # Extract season and command input
    s = args.season
    cmd = args.command
    file = s + '.csv'

    data_handler = DataHandler(file)

    if cmd == "generate":
        data_handler.generate(start_date=seasons[s]['startDate'], end_date=seasons[s]['endDate'])\

    elif cmd == "update":
        if s == '2023-2024': # update up to today's value
            end_date = datetime.today().strftime("%m/%d/%y")
        else:
            end_date = seasons[s]['endDate']
            
        data_handler.update(end_date=seasons[s]['endDate'])
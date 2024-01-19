#!/usr/bin/env python
import sys, os, config

from data.lib import aggregate_files, aggregate_to_features, daily_to_features
from scrape.data_generator import DataHandler
from scrape.today_scraper import TodaysGameScraper
from parameters.info import seasons
from datetime import datetime

def generate(season:str):
    file = season + '.csv'
    data_handler = DataHandler(file)
    data_handler.generate(start_date=seasons[season]['startDate'], end_date=seasons[season]['endDate'])

def update(season:str):
    file = season + '.csv'
    data_handler = DataHandler(file)
    if season == '2023-2024': # update up to today's value
        end_date = datetime.today().strftime("%m/%d/%y")
    else:
        end_date = seasons[season]['endDate']
    data_handler.update(end_date=end_date)

def aggregate():
    aggregate_files()

def features():
    aggregate_to_features()

def default_function():
    print("No valid function specified")

def daily():
    todays_games = TodaysGameScraper(verbose=True)
    todays_games.obtain()
    daily_to_features()


function_mapping = {
    'generate': generate,
    'update': update,
    'aggregate': aggregate,
    'features': features,
    'daily': daily
}

def main():
    while True:
        user_input = input("Enter command: ").split()
        first_argument = user_input[0] if user_input else None
        keywords = user_input[1:]

        if first_argument == 'exit':
            break

        selected_function = function_mapping.get(first_argument, default_function)
        if selected_function != 'default_function':
            parameter_names = list(selected_function.__code__.co_varnames)[:selected_function.__code__.co_argcount]
            param_keyword_mapping = dict(zip(parameter_names, keywords))
            selected_function(**param_keyword_mapping)
        else:
            selected_function()

if __name__ == "__main__":
    main()
    
    

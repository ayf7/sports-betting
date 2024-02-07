#!/usr/bin/env python
import sys, os, config

from data.lib import aggregate_files, aggregate_to_features, daily_to_features
from scrape.generator import TodayGenerator, SeasonGenerator
from parameters.info import seasons
from datetime import datetime

def generate(season:str):
    pass

def update(season:str):
    pass

def aggregate():
    aggregate_files()

def features():
    aggregate_to_features()

def default_function():
    print("No valid function specified.")

def daily():
    todays_date = datetime.today().strftime("%m/%d/%Y")
    today_generator = TodayGenerator()
    today_generator.generate(todays_date)
    daily_to_features()
    pass


function_mapping = {
    'generate': generate,
    'update': update,
    'aggregate': aggregate,
    'features': features,
    'daily': daily
}

function_help = {
    'generate': 'generate <YYYY-YYYY>',
    'update': 'update <YYYY-YYYY>',
    'aggregate': 'aggregate',
    'features': 'features',
    'daily': 'daily'
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
            try:
                selected_function(**param_keyword_mapping)
            except TypeError:
                print(f"Insufficient parameters. Usage: {function_help[first_argument]}")
        else:
            selected_function()

if __name__ == "__main__":
    main()
    
    

#!/usr/bin/env python
import sys, os
parent_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(parent_directory)

from collections import defaultdict

from scrape.time_scraper import TimeScraper
from scrape.game_scraper import GameScraper
from scrape.stats_scraper import StatsScraper
from scrape.parameters.info import seasons
from misc.logger import Logger

import time
import json
import pickle
from datetime import datetime, timedelta
import pandas as pd

file_directory = os.path.dirname(__file__)
data_directory = os.path.join(file_directory, '../data/')

class DataHandler():
    """
    Handles all webscraping functions, retrieving inputs and labels.
    """
    pass

    def __init__(self, target:str, verbose:bool=True):
        self.time_scraper = TimeScraper(verbose=verbose)
        self.game_scraper = GameScraper(verbose=verbose)
        self.stats_scraper = StatsScraper(verbose=verbose)

        self.log = Logger("DataHandler", enabled=verbose)

        # Check if the pickled file exists
        self.data = None
        self.target = target

        if os.path.exists(os.path.join(data_directory, self.target)):
            self.log.info("Target file located and loaded.")
            with open(os.path.join(data_directory, self.target), 'rb') as file:
                self.data = pickle.load(file)
                freqs = defaultdict(int)
                features, labels, dates, game_ids = self.data # unpack
                for k in dates:
                    freqs[k] += 1
                # self.log.info(f"Freqs: {freqs}")
                assert len(features) == len(labels)
                assert len(features) == len(game_ids)
                assert len(features) == len(dates)
                self.log.info(f"Number of days spanned: {len(freqs)}")
                self.log.info(f"Number of data points: {len(features)}")
                

        else:
            self.log.info("Target file not found.")
        print()
        
    def _generate_dates(self, start_date:str, end_date:str):
        """
        Helper class that generates dates from start_date to end_date, inclusive.
        """
        date_format = "%m/%d/%y"  # Updated date format
        start = datetime.strptime(start_date, date_format)
        end = datetime.strptime(end_date, date_format)

        current_date = start
        date_list = []

        while current_date <= end:
            date_list.append(current_date.strftime(date_format))
            current_date += timedelta(days=1)

        return date_list

    def append_games(self, d:str, id:str, features:list, labels:list, dates:list, game_ids:list) -> None:
        """
        Given a game id, extract all values and append them to the features and
        labels list.
        """
        try:
            # Game Scraping: get home/road teams ID and starting player ID/position
            stats, score = self.game_scraper.unpack_teams(id)

            # Obtain stats:
            home_players, home_team = self.stats_scraper.get_stats(stats['homeTeam'], player_ids=stats['homeTeamStarters'], date=d, location='Home')
            road_players, road_team = self.stats_scraper.get_stats(stats['roadTeam'], player_ids=stats['roadTeamStarters'], date=d, location='Road')

            home_players = home_players.drop(columns=['PLAYER_ID', 'PLAYER_NAME'])
            road_players = road_players.drop(columns=['PLAYER_ID', 'PLAYER_NAME'])
            home_players = home_players.stack().to_frame().T
            road_players = road_players.stack().to_frame().T

            # Append all features, labels, record the date
            row_data = pd.concat([home_team, home_players, road_team, road_players], axis=1)
            data = row_data.iloc[0].tolist()
            if features:
                assert(len(features[-1]) == len(data))
            else:
                print(self.log.info(f"Features found: {len(data)}"))
            features.append(data)
            labels.append([score['homeScore'], score['roadScore']])
            dates.append(d)
            game_ids.append(id)

            self.log.info("Saved game!")
            
        except KeyError:
            self.log.fail(f"Could not obtain game {id}.")
        pass

    def generate(self, start_date:str, end_date:str) -> None:
        """
        Generates all data from scratch, from the [startTime] to [endTime].
        Data stored in [self.data].
        """
        self.log.info(f"Generating data {start_date} to {end_date} from scratch...")

        # generate list of dates from start to end, inclusive
        datespan = self._generate_dates(start_date, end_date)

        # Jank method of fixing all columns, because dataframe concatenation is finnicky
        with open(os.path.join(file_directory, 'parameters/features.json'), 'r') as file:
            data = json.load(file)
        player_features = data['player_features'][2:]
        team_features = data['team_features']
        feature_cols = [f"ht_{col}" for col in team_features] \
                        + [f"hp{i}_{col}" for i in range(5) for col in player_features] \
                        + [f"rt_{col}" for col in team_features] \
                        + [f"rp{i}_{col}" for i in range(5) for col in player_features]

        # list of features and labels (can be thought of as a NumPy array)
        features = []
        labels = []
        dates = [] # corresponds to the dates
        game_ids = [] # corresponds to game_ids
        try:
            for d in datespan:
                self.log.info(f"Extracting games on {d}...")
                # Time Scraping
                games = self.time_scraper.game_ids(d)
                # for each game, extract each home/road team/player feature
                for id in games:
                    self.append_games(d, id, features, labels, dates, game_ids)
                self.log.info(f"All games {d} has been saved.")
                pass
        
        # Ignore all exceptions (i.e. KeyboardInterrupt still supports quicksaving)
        except KeyboardInterrupt:
            pass

        # Give user choice to save values
        finally:
            try: # always prompt whether to save currently stored values
                inp = input("\nInterrupted. Save current values? [y/N]")
                save = True if inp == 'y' else False
            except:
                save = False
                print()
            # save to data folder
            if save:
                data = (features, labels, dates, game_ids)
                with open(os.path.join(data_directory, self.target), 'wb') as file:
                    pickle.dump(data, file)
                self.log.info(f"Saved to file {self.target}!")
            else:
                self.log.info("Discarding changes - exiting")

    def update(self, end_date:str="") -> None:
        """
        Resumes data collection. Requires that data has been generated and
        exists.
        """
        if self.data == None:
            self.log.fail("Data does not exist. Cannot update.")
            return
        
        try:
            features, labels, dates, game_ids = self.data
        except:
            self.log.fail("Malformed data: cannot unpack.")
        
        if end_date == "": # by default, end_date
            end_date = datetime.now().strftime("%m/%d/%y")
        
        # obtain the last previous date
        last_date = dates[-1]
        last_id = game_ids[-1]
        self.log.info(f"Last game date: {last_date}, last game ID: {last_id}")

        datespan = self._generate_dates(last_date, end_date)
        datespan.pop() # we don't want today
        self.log.info(f"Resuming data generation from {datespan[0]} to {datespan[-1]}...")

        try:
            for d in datespan:
                
                self.log.info(f"Extracting games on {d}...")
                
                # Time Scraping
                games = self.time_scraper.game_ids(d)

                # Edge case: finding the most recent game within
                if d == last_date:
                    idx = games.index(last_id)
                    games = games[idx+1:]
                    self.log.info(f"Resuming last processed game and date. Games: {games}")
                
                # for each game, extract each home/road team/player feature
                for id in games:
                    time.sleep(1) # robots.txt?
                    self.append_games(d, id, features, labels, dates, game_ids)
                    
                self.log.info(f"All games {d} has been saved.")
                pass
        
        # Ignore all exceptions (i.e. KeyboardInterrupt still supports quicksaving)
        except KeyboardInterrupt:
            pass

        # Give user choice to save values
        finally:
            try: # always prompt whether to save currently stored values
                inp = input("\nInterrupted. Save current values? [y/N]")
                save = True if inp == 'y' else False
            except:
                save = False
                print()
            # save to data folder
            if save:
                data = (features, labels, dates, game_ids)
                with open(os.path.join(data_directory, self.target), 'wb') as file:
                    pickle.dump(data, file)
                self.log.info(f"Saved to file {self.target}!")
            else:
                self.log.info("Discarding changes - exiting")

    def retrieve(self, start_time:str, end_time:str) -> pd.DataFrame:
        """
        Retrieves all games from a specific start time to an end time. Requires
        that data has been generated and exists.
        """

if __name__ == '__main__':

    s = "2020-2021"

    file = s + '.pkl'

    data_handler = DataHandler(file)
    # data_handler.generate(start_date=seasons[s]['startDate'],
    #                       end_date=seasons[s]['endDate'])
    data_handler.update(end_date=seasons[s]['endDate'])

    # data_handler = DataHandler("2021-2022.pkl")
    # # data_handler.generate(start_date='11/7/21', end_date='4/10/22')
    # data_handler.update(end_date='4/10/22')

    # data_handler = DataHandler("2022-2023.pkl")
    # # data_handler.generate(start_date='11/7/22', end_date='4/9/23')
    # data_handler.update(end_date='4/9/23')

    # data_handler = DataHandler("2023-2024.pkl")
    # data_handler.generate(start_date='11/7/23', end_date='1/14/24')
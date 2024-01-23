#!/usr/bin/env python
import sys, os
parent_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(parent_directory)

from collections import defaultdict

from scrape.time_scraper import TimeScraper
from scrape.game_scraper import GameScraper
from scrape.stats_scraper import StatsScraper
from parameters.info import seasons
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
    def __init__(self, target:str, verbose:bool=True):
        self.time_scraper = TimeScraper(verbose=verbose)
        self.game_scraper = GameScraper(verbose=verbose)
        self.stats_scraper = StatsScraper(verbose=verbose)

        self.log = Logger("DataHandler", enabled=verbose)

        # Check if the pickled file exists
        self.data = None
        self.target = target

        dir = os.path.join(data_directory, self.target)
        if os.path.exists(dir):
            self.log.info("Target file located and loaded.")
            self.data = pd.read_csv(dir, dtype={'GAME_ID':str})
            self.log.info(self.data)
                

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

    def append_games(self, d:str, id:str, lst:list) -> None:
        """
        Given a game id, extract all values and append them to the features and
        labels list.
        """
        try:
            # Game Scraping: get home/road teams ID and starting player ID/position
            stats, score = self.game_scraper.unpack_teams(id)

            if not stats or not score:
                 return

            # Obtain stats:
            home_players, home_team = self.stats_scraper.get_stats(stats['homeTeam'], player_ids=stats['homeTeamStarters'], date=d, location='Home')
            road_players, road_team = self.stats_scraper.get_stats(stats['roadTeam'], player_ids=stats['roadTeamStarters'], date=d, location='Road')
            
            home_players = home_players.stack().to_frame().T
            road_players = road_players.stack().to_frame().T

            # Append all features, labels, record the date
            row_data = pd.concat([home_team, home_players, road_team, road_players], axis=1)
            data = row_data.iloc[0].tolist()
            data = [str(id), d] + data + [score['homeScore'], score['roadScore']]
            if lst:
                assert(len(lst[-1]) == len(data))
            else:
                print(self.log.info(f"Features found: {len(data)}"))            
            lst.append(data)
            # print(lst)

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
        with open(os.path.join(parent_directory, 'parameters/features.json'), 'r') as file:
            data = json.load(file)
        player_features = data['player_features']
        team_features = data['team_features']
        
        feature_cols = ["GAME_ID", "DATE"] + [f"ht_{col}" for col in team_features] \
                        + [f"hp{i}_{col}" for i in range(5) for col in player_features] \
                        + [f"rt_{col}" for col in team_features] \
                        + [f"rp{i}_{col}" for i in range(5) for col in player_features] \
                        + ["HOME_SCORE", "ROAD_SCORE"]

        # list of features and labels (can be thought of as a NumPy array)
        features = []
        try:
            for d in datespan:
                self.log.info(f"Extracting games on {d}...")
                # Time Scraping
                games = self.time_scraper.game_ids(d)
                # for each game, extract each home/road team/player feature
                for id in games:
                    self.append_games(d, id, features)
                    assert(len(features[-1]) == len(feature_cols)) # RAHHH
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
                df = pd.DataFrame(features, columns=feature_cols)
                print(df)
                df.to_csv(os.path.join(data_directory, self.target), index=False)
                self.log.info(f"Saved to file {self.target}!")
            else:
                self.log.info("Discarding changes - exiting")
        
    def update(self, end_date:str="") -> None:
        """
        Resumes data collection. Requires that data has been generated and
        exists.
        """
        if self.data is None:
            self.log.fail("Data does not exist. Cannot update.")
            return
        
        today = False
        if end_date == "": # by default, end_date
            today = True
            end_date = datetime.now().strftime("%m/%d/%y")
        
        # obtain the last previous date
        last_date = self.data['DATE'].iloc[-1]
        last_id = self.data['GAME_ID'].iloc[-1]
        self.log.info(f"Last game date: {last_date}, last game ID: {last_id}")

        datespan = self._generate_dates(last_date, end_date)
        if today:
            datespan.pop() # we don't want today
        self.log.info(f"Resuming data generation from {datespan[0]} to {datespan[-1]}...")

        features = []
        feature_cols = self.data.columns # reobtain the columns
        try:
            for d in datespan:
                    
                self.log.info(f"Extracting games on {d}...")
                # Time Scraping
                games = self.time_scraper.game_ids(d)

                if d == last_date:
                    idx = games.index(last_id)
                    games = games[idx+1:]
                    self.log.info(f"Resuming last processed game and date. Games: {games}")

                # for each game, extract each home/road team/player feature
                for id in games:
                    self.append_games(d, id, features)
                    if features:
                        assert(len(features[-1]) == len(feature_cols)) # RAHHH
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
                new_df = pd.DataFrame(features, columns=feature_cols)

                self.log.info(f"Finished processing {new_df.shape[0]} new games.")
                df = pd.concat([self.data, new_df], ignore_index=True) # get new combined dataframe
                df.to_csv(os.path.join(data_directory, self.target), index=False)
                self.log.info(f"Saved to file {self.target}!")
            else:
                self.log.info("Discarding changes - exiting")

    def retrieve(self, start_time:str, end_time:str) -> pd.DataFrame:
        """
        Retrieves all games from a specific start time to an end time. Requires
        that data has been generated and exists.
        """
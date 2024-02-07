#!/usr/bin/env python
import sys, os
parent_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(parent_directory)

from scrape.scraper import TeamScraper, TimeScraper, GameScraper, PlayerScraper, TodayScraper
from misc.logger import Logger

from abc import ABC, abstractmethod
import pandas as pd
from misc.logger import Logger
import json
from parameters.info import id_to_team, seasons
from datetime import datetime, timedelta
import traceback

file_directory = os.path.dirname(__file__)
data_directory = os.path.join(file_directory, '../data/')

class Generator(ABC):
    """
    A generic generator class that uses scrapers to generate data and save data
    frames as csv files.
    """
    def __init__(self, verbose=True, season='2023-24', destination='file'):
        with open(os.path.join(parent_directory, 'parameters/features.json'), 'r') as file:
            features = json.load(file)
        self.player_features = features['player_features']
        self.team_features = features['team_features']
        self.season = season
        self.destination = os.path.join(data_directory, f"{destination}.csv") # file name to save the csv to

        self.team_scraper = TeamScraper(verbose)
        self.player_scraper = PlayerScraper(verbose)
        self.logger = Logger("SeasonGenerator")

        # column names
        self.home_player_cols, self.away_player_cols = [], []
        self.home_team_cols, self.away_team_cols = [], []

        # if the destination .csv exists, store it in a dataframe.
        if os.path.exists(self.destination):
            self.df = pd.read_csv(self.destination, dtype={'GAME_ID': str})
            self.logger.info(f"Existing file found. Games: {self.df.shape[0]}, Last Date:")
        else:
            self.df = pd.DataFrame()
        
        # Ten different webscraping dataframes: 4 primary, and 6 backup (3 player/3 team)
        # four primary: fixed time window, location specified
        self.team_away = None
        self.player_away = None 
        self.team_home = None 
        self.player_home = None 
        # three player backup: beginning of season, location specified and not specified
        self.player_away_full = None
        self.player_home_full = None 
        self.player_general = None #
        # three team backup: beginning of the season, location specified and not specified
        self.team_away_full = None
        self.team_home_full = None 
        self.team_general = None 
        pass
    
    def generate_dataframes(self, start_date:str, end_date:str) -> None:
        """
        Generates all ten databases, given the start and end dates.
        """
        self.team_away = self.team_scraper.forward(DateFrom=start_date, DateTo=end_date, Location='Road', features=self.team_features, Season=self.season)
        self.player_away = self.player_scraper.forward(DateFrom=start_date, DateTo=end_date, Location='Road', features=self.player_features, Season=self.season)
        self.team_home = self.team_scraper.forward(DateFrom=start_date, DateTo=end_date, Location='Home', features=self.team_features, Season=self.season)
        self.player_home = self.player_scraper.forward(DateFrom=start_date, DateTo=end_date, Location='Home', features=self.player_features, Season=self.season)
        
        # Obtain 3 player reserves: away/home counterparts since the beginning of the season, and locationless since the beginning
        self.player_away_full = self.player_scraper.forward(DateFrom='', DateTo=end_date, Location='Road', features=self.player_features, Season=self.season)
        self.player_home_full = self.player_scraper.forward(DateFrom='', DateTo=end_date, Location='Home', features=self.player_features, Season=self.season)
        self.player_general = self.player_scraper.forward(DateFrom='', DateTo=end_date, Location='', features=self.player_features, Season=self.season)

        # Obtain 3 team reserves similarly above
        self.team_away_full = self.team_scraper.forward(DateFrom='', DateTo=end_date, Location='Road', features=self.team_features, Season=self.season)
        self.team_home_full = self.team_scraper.forward(DateFrom='', DateTo=end_date, Location='Home', features=self.team_features, Season=self.season)
        self.team_general = self.team_scraper.forward(DateFrom='', DateTo=end_date, Location='', features=self.team_features, Season=self.season)
    pass

    def get_player_values(self, ids, df:pd.DataFrame, df_full:pd.DataFrame, df_general:pd.DataFrame,
                          columns_list:list=[], location:str="") -> list:
        """
        Given the id of a player, find the features that match in [df]. If not
        found, find in [df_full]. If not, find in [df_general], which should
        always be guaranteed.

        If columns_list is initially empty, the list with the column values
        in-place to [columns_list]. This value is not returned.

        Throws an exception if one player is not found.
        """

        lst = []

        populate = not columns_list
        for player in [f'PLAYER_{i}' for i in range(1, 6)]:
            if populate:
                assert location != ""
                cols = df.columns.tolist()
                cols = list(map(lambda x : f"{location.upper()}_{player}_{x}", cols))
                columns_list += cols
            
            player_id = ids[player]

            # Goes through each 'cache' and looks for the most recent stats.
            if player_id in df.index: # past 3 weeks stats
                lst += df.loc[player_id].tolist()

            elif player_id in df_full.index: # stats from start of season
                self.logger.warn(f"{player_id} extracted from beginning of season database.")
                lst += df_full.loc[player_id].tolist()

            elif player_id in df_general.index: # start of season + location invariant
                self.logger.warn(f"{player_id} extracted from beginning of season, locationless database.")
                lst += df_general.loc[player_id].tolist()

            else:
                # for i in df_full.index[10:50]:
                #     print(i)
                self.logger.fail(f"Player {player_id} not found at all. Skipping...")
                return []
        return lst
    
    def get_team_values(self, ids, df:pd.DataFrame, df_full:pd.DataFrame, df_general:pd.DataFrame,
                            columns_list:list=[], location:str="") -> list:
        """
        Given the ids of a team, extract the features and labels (score) of the
        team.

        If columns_list is initially empty, the list with the column values
        in-place to [columns_list]. This value is not returned.
        """
        lst : list = []
        
        if not columns_list:
            assert location != ""
            cols = df.columns.tolist()
            cols = list(map(lambda x : f"{location.upper()}_{x}", cols)) # Append the features
            columns_list += cols
            columns_list.append("SCORE") # Score - TODO: maybe must be different for daily games without score/testing?

        team_id = ids["TEAM_ID"]
        if team_id in df.index:
            lst += df.loc[team_id].tolist()
        
        elif team_id in df_full.index:
            self.logger.warn(f"Team {team_id} extracted from beginning of season database.")
            lst += df_full.loc[team_id].tolist()
            
        elif team_id in df_general.index:
            self.logger.warn(f"Team {team_id} extracted from beginning of season, locationless database.")
            lst += df_general.loc[team_id].tolist()
        
        else:
            self.logger.fail(f"Team not found at all (how did this happen?). Skipping...")
            return []
            
        lst.append(ids["SCORE"])
        assert(len(columns_list) == len(lst))
        return lst
    
    @abstractmethod
    def generate(self, input, destination) -> None:
        """
        Given data, generates values.
        """
    
    def _start_and_end(self, date:str) -> tuple[str]:
        """
        Given a date, returns the two dates, [start_date] and [end_date], to be
        used for generating player and team stats dataframes.

        [start_date] is 21 days before [date], and [end_date] is 1 day before
        [end_date].
        """
        date_format = '%m/%d/%Y'
        date_object = datetime.strptime(date, date_format)

        end_obj = date_object - timedelta(days=1) # end date is yesterday
        start_obj = date_object - timedelta(days=21) # can be tuned, currently set to three weeks

        end_date = end_obj.strftime(date_format)
        start_date = start_obj.strftime(date_format)
        return start_date, end_date

class TodayGenerator(Generator):
    """
    Generates all games for today.
    """

    def __init__(self, verbose=True, destination='xTe_raw'):
        super().__init__(verbose, season='2023-24', destination=destination)
        self.today_scraper = TodayScraper(verbose)

    def generate(self, date:str):
        """
        Generates games.
        """
        start, end = self._start_and_end(date)
        self.generate_dataframes(start, end)
        df = self.today_scraper.forward() # we could probably customize this
        try:
            lst = []
            for game_id in df:
                game = df[game_id]
                self.logger.info(f"Obtaining game {id_to_team[game.loc[1]['TEAM_ID']]} @ {id_to_team[game.loc[0]['TEAM_ID']]}")

                # obtaining all values
                home_player_rows = self.get_player_values(game.iloc[0], self.player_home, self.player_home_full, self.player_general,
                                                                            columns_list=self.home_player_cols, location='home')
                home_team_rows = self.get_team_values(game.iloc[0], self.team_home, self.team_home_full, self.team_general,
                                                        columns_list=self.home_team_cols, location='home')
                away_player_rows = self.get_player_values(game.iloc[1], self.player_away, self.player_away_full, self.player_general,
                                                                        columns_list=self.away_player_cols, location='away')
                away_team_rows = self.get_team_values(game.iloc[1], self.team_away, self.team_away_full, self.team_general,
                                                            columns_list=self.away_team_cols, location='away')
                
                scores = [game.iloc[0]["SCORE"], game.iloc[1]["SCORE"]]
                
                if home_player_rows and home_team_rows and away_player_rows and away_team_rows:                   
                    lst.append([date, game_id] + home_team_rows + home_player_rows + away_team_rows + away_player_rows + scores)
        
        except (Exception, KeyboardInterrupt) as e:
            print("j", e)
            traceback.print_exc()
            print("\nInterrupt occurred")

        finally:
            # Retrieve common columns
            if self.df.empty:
                columns = ["DATE", "GAME_ID"] + self.home_team_cols + self.home_player_cols + self.away_team_cols + self.away_player_cols + ["HOME_SCORE", "AWAY_SCORE"]
            else:
                columns = self.df.columns
            df = pd.DataFrame(lst, columns=columns)
            self.df = pd.concat([self.df, df], axis=0)
            self.df.to_csv(self.destination, index=False)
            self.logger.info(f"Saved info to {self.destination}!")
        pass

class SeasonGenerator():
    """
    Generates all data from a season. The season links to '2023-24.csv' in the data
    folder.
    """

    def __init__(self, season='2023-24', verbose=True):
        with open(os.path.join(parent_directory, 'parameters/features.json'), 'r') as file:
            features = json.load(file)
        self.player_features = features['player_features']
        self.team_features = features['team_features']
        self.season = season
        self.destination = os.path.join(data_directory, f"{season}.csv")

        self.time_scraper = TimeScraper(verbose)
        self.game_scraper = GameScraper(verbose)
        self.team_scraper = TeamScraper(verbose)
        self.player_scraper = PlayerScraper(verbose)
        self.logger = Logger("SeasonGenerator")

        self.home_player_cols, self.away_player_cols = [], []
        self.home_team_cols, self.away_team_cols = [], []

        # Check if loaded values already exist
        if os.path.exists(self.destination):
            self.df = pd.read_csv(self.destination, dtype={'GAME_ID': str})
            self.logger.info(f"Existing file found. Games: {self.df.shape[0]}, Last Date:")
        else:
            self.df = pd.DataFrame()

    def get_player_values(self, ids, df:pd.DataFrame, df_full:pd.DataFrame, df_general:pd.DataFrame,
                          columns_list:list=[], location:str="") -> list:
        """
        Given the id of a player, find the features that match in [df]. If not
        found, find in [df_full]. If not, find in [df_general], which should
        always be guaranteed.

        If columns_list is initially empty, the list with the column values
        in-place to [columns_list]. This value is not returned.

        Throws an exception if one player is not found.
        """

        lst = []

        populate = not columns_list
        for player in [f'PLAYER_{i}' for i in range(1, 6)]:
            if populate:
                assert location != ""
                cols = df.columns.tolist()
                cols = list(map(lambda x : f"{location.upper()}_{player}_{x}", cols))
                columns_list += cols
            
            player_id = ids[player]

            # Goes through each 'cache' and looks for the most recent stats.
            if player_id in df.index: # past 3 weeks stats
                lst += df.loc[player_id].tolist()

            elif player_id in df_full.index: # stats from start of season
                self.logger.warn(f"{player_id} extracted from beginning of season database.")
                lst += df_full.loc[player_id].tolist()

            elif player_id in df_general.index: # start of season + location invariant
                self.logger.warn(f"{player_id} extracted from beginning of season, locationless database.")
                lst += df_general.loc[player_id].tolist()

            else:
                # for i in df_full.index[10:50]:
                #     print(i)
                self.logger.fail(f"Player {player_id} not found at all. Skipping...")
                return []
        return lst
    
    def get_team_values(self, ids, df:pd.DataFrame, df_full:pd.DataFrame, df_general:pd.DataFrame,
                            columns_list:list=[], location:str="") -> list:
        """
        Given the ids of a team, extract the features and labels (score) of the
        team.

        If columns_list is initially empty, the list with the column values
        in-place to [columns_list]. This value is not returned.
        """
        lst : list = []
        
        if not columns_list:
            assert location != ""
            cols = df.columns.tolist()
            cols = list(map(lambda x : f"{location.upper()}_{x}", cols)) # Append the features
            columns_list += cols
            columns_list.append("SCORE") # Score - TODO: maybe must be different for daily games without score/testing?

        team_id = ids["TEAM_ID"]
        if team_id in df.index:
            lst += df.loc[team_id].tolist()
        
        elif team_id in df_full.index:
            self.logger.warn(f"Team {team_id} extracted from beginning of season database.")
            lst += df_full.loc[team_id].tolist()
            
        elif team_id in df_general.index:
            self.logger.warn(f"Team {team_id} extracted from beginning of season, locationless database.")
            lst += df_general.loc[team_id].tolist()
        
        else:
            self.logger.fail(f"Team not found at all (how did this happen?). Skipping...")
            return []
            
        lst.append(ids["SCORE"])
        assert(len(columns_list) == len(lst))
        return lst
    
    def _generate_dates(self, start_date:str, end_date:str) -> list:
        """
        Helper class that generates dates from start_date to end_date,
        inclusive.
        """
        date_format = "%m/%d/%Y"  # Updated date format
        start = datetime.strptime(start_date, date_format)
        end = datetime.strptime(end_date, date_format)

        current_date = start
        date_list = []

        while current_date <= end:
            date_list.append(current_date.strftime(date_format))
            current_date += timedelta(days=1)

        return date_list

    def generate(self, start_date:str, end_date:str, update:bool=False, custom_dest:str='') -> None:
        """
        Generates a dataframe from scratch.
        """
        # Extract every date in the list
        date_list = self._generate_dates(start_date, end_date)
        lst = [] # stores new values generated

        # if we are updating, truncate list to the last saved game's date
        if update:
            if self.df.empty:
                self.logger.info("Cannot retrieve last saved game. Starting from scratch.")
            else:
                last_date = self.df.iloc[-1]["DATE"]
                last_game = self.df.iloc[-1]["GAME_ID"]
                self.logger.info(f"Resuming on date {last_date}...\n")
                idx = date_list.index(last_date)
                date_list = date_list[idx:]
        try:
            for date in date_list:
                self.logger.info(f"[DATE: {date}]\n")

                # Obtain the start and end dates for webscraping
                date_format = '%m/%d/%Y'
                date_object = datetime.strptime(date, date_format)
                yesterday_obj = date_object # - timedelta(days=1) # end date is yesterday
                start_obj = date_object - timedelta(days=21) # can be tuned, currently set to three weeks
                end_date = yesterday_obj.strftime(date_format)
                start_date = start_obj.strftime(date_format)

                # obtain the games
                games = self.time_scraper.forward(gamedate=date)

                # if we are updating, we will resume
                if update and not self.df.empty and date == last_date:
                    idx = games.index(last_game)
                    games = games[idx+1:]
                    game_str = ", ".join(games)
                    self.logger.info(f"Updated values: {game_str}")
                print()

                if not games: continue
                self.logger.info("[RETRIEVING GAMES...]\n")

                # Obtain all 6 webscraping dataframes: player and team from [start] to [end_date]
                team_away = self.team_scraper.forward(DateFrom=start_date, DateTo=end_date, Location='Road', features=self.team_features, Season=self.season)
                player_away = self.player_scraper.forward(DateFrom=start_date, DateTo=end_date, Location='Road', features=self.player_features, Season=self.season)
                team_home = self.team_scraper.forward(DateFrom=start_date, DateTo=end_date, Location='Home', features=self.team_features, Season=self.season)
                player_home = self.player_scraper.forward(DateFrom=start_date, DateTo=end_date, Location='Home', features=self.player_features, Season=self.season)
                
                # Obtain 3 player reserves: away/home counterparts since the beginning of the season, and locationless since the beginning
                player_away_full = self.player_scraper.forward(DateFrom='', DateTo=end_date, Location='Road', features=self.player_features, Season=self.season)
                player_home_full = self.player_scraper.forward(DateFrom='', DateTo=end_date, Location='Home', features=self.player_features, Season=self.season)
                player_general = self.player_scraper.forward(DateFrom='', DateTo=end_date, Location='', features=self.player_features, Season=self.season)

                # Obtain 3 team reserves similarly above
                team_away_full = self.team_scraper.forward(DateFrom='', DateTo=end_date, Location='Road', features=self.team_features, Season=self.season)
                team_home_full = self.team_scraper.forward(DateFrom='', DateTo=end_date, Location='Home', features=self.team_features, Season=self.season)
                team_general = self.team_scraper.forward(DateFrom='', DateTo=end_date, Location='', features=self.team_features, Season=self.season)
                print()
                
                self.logger.info("[EXTRACTING GAMES...]\n")
                for game_id in games:

                    # checkpoint 1: some games were postponed/cancelled.
                    try:
                        teams_ids = self.game_scraper.forward(GameID=game_id, t=0.5)
                    except TypeError:
                        self.logger.fail(f"Failed to obtain game {game_id} attributes. Known reasons include postponement.")
                        continue
                        
                    # checkpoint 2: some special games with teams outside the 30 are skipped.
                    if teams_ids.loc[1]['TEAM_ID'] not in id_to_team or teams_ids.loc[1]['TEAM_ID'] not in id_to_team:
                        self.logger.warn("Skipping game - invalid team found.")
                        continue

                    self.logger.info(f"Obtaining game {id_to_team[teams_ids.loc[1]['TEAM_ID']]} @ {id_to_team[teams_ids.loc[0]['TEAM_ID']]}")

                    # Home team and players
                    home_player_rows = self.get_player_values(teams_ids.iloc[0], player_home, player_home_full, player_general,
                                                                         columns_list=self.home_player_cols, location='home')
                    home_team_rows = self.get_team_values(teams_ids.iloc[0], team_home, team_home_full, team_general,
                                                          columns_list=self.home_team_cols, location='home')

                    # Away team and players
                    away_player_rows = self.get_player_values(teams_ids.iloc[1], player_away, player_away_full, player_general,
                                                                         columns_list=self.away_player_cols, location='away')
                    away_team_rows = self.get_team_values(teams_ids.iloc[1], team_away, team_away_full, team_general,
                                                          columns_list=self.away_team_cols, location='away')

                    # Home and away scores
                    scores = [teams_ids.iloc[0]["SCORE"], teams_ids.iloc[1]["SCORE"]]
    
                    if home_player_rows and home_team_rows and away_player_rows and away_team_rows:                   
                        lst.append([date, game_id] + home_team_rows + home_player_rows + away_team_rows + away_player_rows + scores)

                print()

        except (Exception, KeyboardInterrupt) as e:
            print("j", e)
            traceback.print_exc()
            print("\nInterrupt occurred")
        
        finally:
            # Retrieve common columns
            if self.df.empty:
                columns = ["DATE", "GAME_ID"] + self.home_team_cols + self.home_player_cols + self.away_team_cols + self.away_player_cols + ["HOME_SCORE", "AWAY_SCORE"]
            else:
                columns = self.df.columns

            # Concatenate
            df = pd.DataFrame(lst, columns=columns)
            self.df = pd.concat([self.df, df], axis=0)

            # save
            if custom_dest:
                self.df.to_csv(custom_dest, index=False)
            else:
                self.df.to_csv(self.destination, index=False)
        

if __name__ == '__main__':

    season = '2021-22'
    season_scraper = SeasonGenerator(season)
    start_date = seasons[season]['startDate']

    # set the start date to 2 weeks ahead of start
    date_format = '%m/%d/%Y'
    date_object = datetime.strptime(start_date, date_format)
    date_object = date_object + timedelta(days=14)
    start_date = datetime.strftime(date_object, date_format)

    end_date = seasons[season]['endDate']
    if season == '2023-24':
        end_date = datetime.today() - timedelta(days=1)
        end_date = datetime.strftime(end_date, date_format)
    
    season_scraper.generate(start_date=start_date, end_date=end_date, update=True)

    # today_generator = TodayGenerator()
    # today_generator.generate('01/27/2024')
    pass
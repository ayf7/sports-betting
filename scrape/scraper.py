#!/usr/bin/env python
import sys, os
parent_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(parent_directory)

from abc import ABC, abstractmethod
import requests
import pandas as pd
from misc.logger import Logger
import json
from parameters.info import seasons, id_to_team
from datetime import datetime, timedelta
import time
import traceback

file_directory = os.path.dirname(__file__)
data_directory = os.path.join(file_directory, '../data/')

class Scraper(ABC):
    """
    A generic scraper class
    """
    def __init__(self, log_name="Scraper", verbose=True, required_fields=[], **kwargs):
        self.url = ""
        self.payload = {key: str(value) for key, value in kwargs.items()}
        self.headers= {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36',
            'Referer': 'https://www.nba.com/'
        }
        self.required_fields = required_fields
        self.logger = Logger(log_name, enabled=verbose, indent=1)
        
    def validate(self, **kwargs) -> True:
        """
        Ensures that all required keywords exist in forward call.
        """
        missing_words = []
        for f in self.required_fields:
            if f not in kwargs:
                missing_words.append(f)
        if missing_words:
            self.logger.fail(f"Missing required keywords: {missing_words}")
            return False
        return True
    
    def get_request(self, **kwargs) -> requests.Response:
        """
        Updates necessary values in [self.payload] and gets the request.
        """
        self.payload.update(kwargs)
        return requests.get(self.url, params=self.payload, headers=self.headers)
    
    @abstractmethod
    def extract(self, json, **kwargs) -> pd.DataFrame:
        """
        Given a JSON object, extract and return the necessary data in a pandas
        data frame.
        """

    def forward(self, t:int = 1, **kwargs):
        """
        Gets the request.
        Common keyword options: DateFrom, DateTo, Location
        """
        
        if not self.validate(**kwargs):
            self.logger.fail("Missing Required Keywords.")
            raise Exception("Missing Required Keywords")
            
        response = self.get_request(**kwargs)
        time.sleep(t)
        if response.status_code == 200:
            return self.extract(response.json(), **kwargs)

        else:
            raise Exception(f"HTTP Error - {response.status_code}")

class PlayerScraper(Scraper):
    """
    Obtains the stats of all players.
    """
    def __init__(self, verbose=True):
        super().__init__("PlayScraper", verbose, required_fields=['Season'])
        self.url = "https://stats.nba.com/stats/leaguedashplayerstats"
        self.payload = {
            'LastNGames': '0',
            'LeagueID': '00',
            'MeasureType': 'Base',
            'Month': '0',
            'OpponentTeamID': '0',
            'PORound': '0',
            'PaceAdjust': 'N',
            'PerMode': 'PerGame',
            'Period': '0',
            'PlusMinus': 'N',
            'Rank': 'N',
            'Season': '2023-24',
            'SeasonSegment': '',
            'SeasonType': 'Regular Season',
            'TeamID': '0',
        }
    
    def extract(self, json, **kwargs):
        headers = json['resultSets'][0]['headers']
        rows = json['resultSets'][0]['rowSet']

        df = pd.DataFrame(rows, columns=headers)

        if 'features' in kwargs: # filters columns
            df = df[kwargs['features']]
        
        df.set_index('PLAYER_ID', inplace=True, drop=False)
        d_to = kwargs["DateTo"]
        if "DateFrom" in kwargs and len(kwargs["DateFrom"]) > 0:
            d_from = kwargs["DateFrom"]
            self.logger.info(f"Extracted player statistics from {d_from} to {d_to}. Players: {df.shape[0]}")
        else:
            self.logger.info(f"Extracted player statistics from start to {d_to}. Players: {df.shape[0]}")
        return df

class TeamScraper(Scraper):
    """
    Retrieves the stats of a specific team.
    Common keyword options: DateFrom, DateTo, Location
    """
    def __init__(self, verbose=True):
        super().__init__("TeamScraper", verbose=verbose, required_fields=['Season'])
        self.url = 'https://stats.nba.com/stats/leaguedashteamstats'
        self.payload = {
            'LastNGames': '0',
            'LeagueID': '00',
            'Location': 'Home',
            'MeasureType': 'Base',
            'Month': '0',
            'OpponentTeamID': '0',
            'PORound': '0',
            'PaceAdjust': 'N',
            'PerMode': 'PerGame',
            'Period': '0',
            'PlusMinus': 'N',
            'Rank': 'N',
            'Season': '2023-24',
            'SeasonType': 'Regular Season',
            'TeamID': '0',
            'TwoWay': '0'
        }
    
    def extract(self, json, **kwargs):
        headers = json['resultSets'][0]['headers']
        rows = json['resultSets'][0]['rowSet']

        df = pd.DataFrame(rows, columns=headers)
        
        if 'features' in kwargs: # filters columns
            df = df[kwargs['features']]
        
        df.set_index('TEAM_ID', inplace=True, drop=False)
        d_to, d_from = kwargs["DateTo"], kwargs["DateFrom"]
        self.logger.info(f"Extracted team statistics from {d_from} to {d_to}. Teams: {df.shape[0]}", carriage=True)
        return df

class GameScraper(Scraper):
    """
    Given a game, given a game ID, returns the row with all information of home
    and away team stats / player starter stats.
    """

    def __init__(self, verbose=True):
        super().__init__("GameScraper", verbose=verbose, required_fields=["GameID"])
        self.url = "https://stats.nba.com/stats/boxscoretraditionalv3"
        self.payload = {
            'GameID': '0022300445',
            'LeagueID': '00',
            'endPeriod': '0',
            'endRange': '28800',
            'rangeType': '0',
            'startPeriod': '0',
            'startRange': '0'
        }
    
    def extract(self, json, **kwargs):
        json = json['boxScoreTraditional']
        columns = ["LOCATION", "TEAM_ID", "SCORE"]
        for i in range(1, 6):
            columns.append(f"PLAYER_{i}")
        data = []
        for team in ['homeTeam', 'awayTeam']:
            team_json = json[team]
            lst = [team[:4], team_json['teamId'], team_json['statistics']['points']]
            player_ids = [(team_json['players'][x]['personId'], team_json['players'][x]['position']) for x in range(5)]
            player_ids.sort(key= lambda x :("GFC".index(x[1]), x))
            player_ids = [x[0] for x in player_ids]
            lst += player_ids
            data.append(lst)
        df = pd.DataFrame(data, columns=columns)
        return df
        
class TimeScraper(Scraper):
    """
    Given a date, returns all game ids that happened that day.
    """
    def __init__(self, verbose=True):
        super().__init__("TimeScraper", verbose=verbose, required_fields=["gamedate"])
        self.url = "https://core-api.nba.com/cp/api/v1.3/feeds/gamecardfeed"
        self.payload = {
            "gamedate": "01/02/2024",
            "platform": "web"
        }
        self.headers = {
            'Ocp-Apim-Subscription-Key': '747fa6900c6c4e89a58b81b72f36eb96',
            'Origin': 'https://www.nba.com/',
            'Referer': 'https://www.nba.com/',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
    
    def extract(self, json, **kwargs) -> list:
        """
        Converts a date into the list of game ids on that day.
        Required keyword: gamedate
        """
        game_ids = []
        if not json['modules']:
            self.logger.warn("No games found.")
            return game_ids
        
        for game in json['modules'][0]['cards']:
            game_ids.append(game['cardData']['gameId'])
        
        game_ids.sort(key=lambda x : int(x)) # sort game_ids in increasing order
        game_str = ", ".join(game_ids)
        self.logger.info(f"Retrieved {len(game_ids)} games: {game_str}")
        return game_ids

class SeasonScraper():
    """
    Scrape all data from a season. The season links to '2023-24.csv' in the data
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
        self.logger = Logger("SeasonScraper")

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
        lst = []
        
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

    def generate(self, start_date:str, end_date:str, update:bool=False) -> None:
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
                yesterday_obj = date_object - timedelta(days=1) # end date is yesterday
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
            if self.df.empty:
                columns = ["DATE", "GAME_ID"] + self.home_team_cols + self.home_player_cols + self.away_team_cols + self.away_player_cols + ["HOME_SCORE", "AWAY_SCORE"]
            else:
                columns = self.df.columns

            df = pd.DataFrame(lst, columns=columns)

            self.df = pd.concat([self.df, df], axis=0)
            self.df.to_csv(self.destination, index=False)
        

if __name__ == '__main__':

    season = '2022-23'
    season_scraper = SeasonScraper(season)
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
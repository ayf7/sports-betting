#!/usr/bin/env python
import sys, os
parent_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(parent_directory)

from abc import ABC, abstractmethod
import requests
import pandas as pd
from misc.logger import Logger
from datetime import datetime, timedelta
import time

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

class TodayScraper(Scraper):
    """
    Returns today's games: it's a bit different than scraping past game stats
    due to a different API call.
    """
    def __init__(self, verbose=True, date:str=None):
        super().__init__("TodayScraper", verbose=verbose, required_fields=[])
        # obtain today's date
        date = datetime.today().strftime('%Y%m%d')
        self.url = f"https://stats.nba.com/js/data/leaders/00_daily_lineups_{date}.json"
        self.headers = {
            'Ocp-Apim-Subscription-Key': '747fa6900c6c4e89a58b81b72f36eb96',
            'Origin': 'https://www.nba.com/',
            'Referer': 'https://www.nba.com/',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
    
    def extract(self, json, **kwargs) -> dict[str, pd.DataFrame]:
        """
        Returns a map of each game id to the pd.DataFrame with all information.
        Scores are set to zero by default and should be ignored. Each
        pd.DataFrame has the same structure as the output of the GameScraper.
        """
        list_of_games = json['games']
        columns = ["LOCATION", "TEAM_ID", "SCORE"]
        for i in range(1, 6):
            columns.append(f"PLAYER_{i}")
        # each [g] contains a list of all values
        games = {} # maps each game id to a pandas dataframe
        for g in list_of_games:
            data = []
            for team in ['homeTeam', 'awayTeam']:
                team_json = g[team]
                lst = [team[:4], team_json['teamId'], 0]
                player_ids = [(team_json['players'][x]['personId'], team_json['players'][x]['position']) for x in range(5)]
                player_ids.sort(key= lambda x :("PG/SG/SF/PF/C".index(x[1]), x))
                player_ids = [x[0] for x in player_ids]
                lst += player_ids
                data.append(lst)
            df_game = pd.DataFrame(data, columns=columns)
            games[g['gameId']] = df_game
        return games

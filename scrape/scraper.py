#!/usr/bin/env python
import sys, os
parent_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(parent_directory)

from abc import ABC, abstractmethod
import requests
import pandas as pd
from misc.logger import Logger

class Scraper(ABC):
    """
    A generic scraper class
    """
    def __init__(self, log_name="Scraper", verbose=True, **kwargs):
        self.url = ""
        self.payload = {key: str(value) for key, value in kwargs.items()}
        self.headers= {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36',
            'Referer': 'https://www.nba.com/'
        }
        self.logger = Logger(log_name, enabled=verbose)
        
    
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

    def forward(self, **kwargs):
        """
        Gets the request.
        Common keyword options: DateFrom, DateTo, Location
        """
        response = self.get_request(**kwargs)
        if response.status_code == 200:
            return self.extract(response.json())

        else:
            raise Exception(f"Error: {response.status_code}")

class PlayerScraper(Scraper):
    """
    Obtains the stats of all players.
    """
    def __init__(self, verbose=True):
        super().__init__("Player", verbose)
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
            'Season': '2022-23',
            'SeasonSegment': '',
            'SeasonType': 'Regular Season',
            'TeamID': '0',
        }
    
    def extract(self, json, **kwargs):
       headers = json['resultSets'][0]['headers']
       rows = json['resultSets'][0]['rowSet']
       return pd.DataFrame(rows, columns=headers)

class TeamScraper(Scraper):
    """
    Retrieves the stats of a specific team.
    Common keyword options: DateFrom, DateTo, Location
    """
    def __init__(self, verbose=True):
        super().__init__("Team", verbose)
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
       return pd.DataFrame(rows, columns=headers)

class GameScraper(Scraper):
    """
    Given a game, given a game ID, returns the row with all information of home
    and away team stats / player starter stats.
    """

    def __init__(self, verbose=True):
        super().__init__("Game", verbose)
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
        columns = ["LOCATION", "TEAM_ID"]
        for i in range(1, 6):
            columns.append(f"PLAYER_{i}")
        data = []
        for team in ['homeTeam', 'awayTeam']:
            team_json = json[team]
            lst = [team[:4], team_json['teamId']]
            player_ids = [(team_json['players'][x]['personId'], team_json['players'][x]['position']) for x in range(5)]
            player_ids.sort(key= lambda x :("GFC".index(x[1]), x))
            player_ids = [x[0] for x in player_ids]
            lst += player_ids
            data.append(lst)
        
        return pd.DataFrame(data, columns=columns)





if __name__ == '__main__':
    ps = GameScraper()
    df = ps.forward()
    print(df)
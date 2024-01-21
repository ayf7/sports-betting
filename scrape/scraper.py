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
    def __init__(self, log_name="Scraper", verbose=True, required_fields=[], **kwargs):
        self.url = ""
        self.payload = {key: str(value) for key, value in kwargs.items()}
        self.headers= {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36',
            'Referer': 'https://www.nba.com/'
        }
        self.required_fields = required_fields
        self.logger = Logger(log_name, enabled=verbose)
        
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

    def forward(self, **kwargs):
        """
        Gets the request.
        Common keyword options: DateFrom, DateTo, Location
        """
        
        if not self.validate(**kwargs):
            raise Exception("Missing Required Keywords")
            
        response = self.get_request(**kwargs)
        if response.status_code == 200:
            return self.extract(response.json())

        else:
            raise Exception(f"HTTP Error - {response.status_code}")

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
            'Season': '2023-24',
            'SeasonSegment': '',
            'SeasonType': 'Regular Season',
            'TeamID': '0',
        }
    
    def extract(self, json, **kwargs):
        headers = json['resultSets'][0]['headers']
        rows = json['resultSets'][0]['rowSet']

        df = pd.DataFrame(rows, columns=headers)
        df.set_index('PLAYER_ID', inplace=True)
        return df

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

        df = pd.DataFrame(rows, columns=headers)
        df.set_index('TEAM_ID', inplace=True)
        return df

class GameScraper(Scraper):
    """
    Given a game, given a game ID, returns the row with all information of home
    and away team stats / player starter stats.
    """

    def __init__(self, verbose=True):
        super().__init__("Game", verbose, ["GameID"])
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
        
        df = pd.DataFrame(data, columns=columns)
        return df

class TimeScraper(Scraper):
    def __init__(self, verbose=True):
        super().__init__("Time", verbose, ["gamedate"])
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
        self.logger.info(f"Retrieved {len(game_ids)} games played: {game_ids}")
        return game_ids

from datetime import datetime, timedelta
import time

if __name__ == '__main__':
    time_scraper = TimeScraper()
    game_scraper = GameScraper()
    team_scraper = TeamScraper()
    player_scraper = PlayerScraper()

    date = '01/01/2024'

    # Obtain yesterday's date
    date_format = '%m/%d/%Y'
    date_object = datetime.strptime(date, date_format)
    yesterday_obj = date_object - timedelta(days=1)
    start_obj = date_object - timedelta(days=14)
    yesterday_date = yesterday_obj.strftime(date_format)
    start_date = start_obj.strftime(date_format)

    time.sleep(5)

    games = time_scraper.forward(gamedate='01/01/2024')

    team_away = team_scraper.forward(DateFrom=start_date, DateTo=yesterday_date, Location='Road')
    player_away = player_scraper.forward(DateFrom=start_date, DateTo=yesterday_date, Location='Road')
    team_home = team_scraper.forward(DateFrom=start_date, DateTo=yesterday_date, Location='Home')
    player_home = player_scraper.forward(DateFrom=start_date, DateTo=yesterday_date, Location='Home')

    player_away_full = player_scraper.forward(DateFrom='', DateTo=yesterday_date, Location='Road')
    player_home_full = player_scraper.forward(DateFrom='', DateTo=yesterday_date, Location='Home')

    for df in [team_away, player_away, team_home, player_home, player_away_full, player_home_full]:
        print(df, "\n")

    for game_id in games:
        ids = game_scraper.forward(GameID=game_id)
#!/usr/bin/env python
import sys, os
parent_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(parent_directory)

from collections import defaultdict
from typing import List
from scrape.parameters.info import id_to_team, team_to_id
import requests
from misc.logger import Logger

class TimeScraper:
    """
    Given a specific date, returns the list of all game IDs that happened on
    that day.
    """

    def __init__(self, verbose:bool=False):

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

        self.log = Logger("TimeScraper", enabled=verbose, indent=1)

    def game_ids(self, date:str) -> List[int]:
        """
        Given a date in [MM/DD/YYYY] format, returns the list of game IDs
        (via NBA.com) that occured that day.
        """

        self.payload['gamedate'] = date
        response = requests.get(self.url, params=self.payload, headers=self.headers)

        if response.status_code == 200:

            json_data = response.json()
            game_ids = []
            if json_data['modules']:
                for game in json_data['modules'][0]['cards']:
                    game_ids.append(game['cardData']['gameId'])
                self.log.info(f"Retrieved {len(game_ids)} games played.")
                game_ids.sort(key=lambda x : int(x)) # sort game_ids in increasing order
                self.log.info(game_ids)
                return game_ids
            else: # no games played on this day
                self.log.warn(f"No games found.")
                return []

        else:
            print(f"Error: {response.status_code}")
            print(response.text)
            return []


if __name__ == '__main__':
    time_scraper = TimeScraper()
    list_of_games = time_scraper.game_ids('12/17/2023')
    print(list_of_games)
#!/usr/bin/env python
import sys, os
parent_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(parent_directory)

from collections import defaultdict
from typing import List
from scrape.stats_scraper import StatsScraper
from scrape.parameters.info import id_to_team, team_to_id
import requests
from misc.logger import Logger
from datetime import datetime
import pandas as pd

class TodaysGameScraper:
    """
    Given a specific date, returns the list of all game IDs that happened on
    that day.
    """

    def __init__(self, verbose:bool=False):

        self.url = ""

        self.headers = {
            'Origin': 'https://www.nba.com/',
            'Referer': 'https://www.nba.com/',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }

        self.stats_scraper = self.stats_scraper = StatsScraper(verbose=verbose)

        self.log = Logger("TodaysGames", enabled=verbose, indent=1)

    def obtain(self, date:str=None) -> list:
        """
        Obtains today's game.
        """
        d = date
        if not date:
            d = datetime.today().strftime("%m/%d/%y")
            date = datetime.today().strftime("%Y%m%d")
        else:
            date = datetime.strptime(date, "%m/%d/%y").strftime("%Y%m%d")
        
        # Update url
        self.url = f"https://stats.nba.com/js/data/leaders/00_daily_lineups_{date}.json"
        response = requests.get(self.url, headers=self.headers)

        if response.status_code == 200:
            json_data = response.json()
            list_of_games = json_data['games']

            features = []
            for game in list_of_games:

                # Retrieve information about the game info
                team_info = {}

                team_info['roadTeam'] = game['awayTeam']['teamId']
                team_info['roadTeamStarters'] = [(game['awayTeam']['players'][x]['personId'], game['awayTeam']['players'][x]['position']) for x in range(5)]
                team_info['roadTeamStarters'].sort(key= lambda x :("PG/SG/SF/PF/C".index(x[1]), x)) # Sorting by increasing position
                team_info['roadTeamStarters'] = [x[0] for x in team_info['roadTeamStarters']]

                team_info['homeTeam'] = game['homeTeam']['teamId']
                team_info['homeTeamStarters'] = [(game['homeTeam']['players'][x]['personId'], game['homeTeam']['players'][x]['position']) for x in range(5)]
                team_info['homeTeamStarters'].sort(key= lambda x :("PG/SG/SF/PF/C".index(x[1]), x))
                team_info['homeTeamStarters'] = [x[0] for x in team_info['homeTeamStarters']]

                self.log.info(f"Retrieved upcoming game {id_to_team[team_info['roadTeam']]} @ {id_to_team[team_info['homeTeam']]}")
                
                # Convert to one long list
                home_players, home_team = self.stats_scraper.get_stats(team_info['homeTeam'], player_ids=team_info['homeTeamStarters'], date=d, location='Home')
                road_players, road_team = self.stats_scraper.get_stats(team_info['roadTeam'], player_ids=team_info['roadTeamStarters'], date=d, location='Road')

                home_players = home_players.drop(columns=['PLAYER_ID', 'PLAYER_NAME'])
                road_players = road_players.drop(columns=['PLAYER_ID', 'PLAYER_NAME'])
                home_players = home_players.stack().to_frame().T
                road_players = road_players.stack().to_frame().T

                row_data = pd.concat([home_team, home_players, road_team, road_players], axis=1)
                data = row_data.iloc[0].tolist()
                if features:
                    assert(len(features[-1]) == len(data))
                else:
                    print(self.log.info(f"Features found: {len(data)}"))
                features.append(data)
        
        return features

if __name__ == '__main__':
    tgs = TodaysGameScraper(verbose=True)
    tgs.obtain()
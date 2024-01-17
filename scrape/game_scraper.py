#!/usr/bin/env python
import sys, os
parent_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(parent_directory)

from collections import defaultdict
from typing import List
from scrape.parameters.info import id_to_team, team_to_id
import requests
from misc.logger import Logger

class GameScraper:
    """
    Given a specific game, we extract the home and away team IDs, and for each
    team, the five starters.
    """

    def __init__(self, verbose:bool=False):

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

        self.headers= {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36',
            'Referer': 'https://www.nba.com/'
        }

        self.log = Logger("TimeScraper", enabled=verbose, indent=1)
  
    def unpack_teams(self, game_id:str):
        """
        Given a game [game_id], returns a dictionary with two keys: the 
        """
      
        self.payload['GameID'] = game_id
        response = requests.get(self.url, params=self.payload, headers=self.headers)

        if response.status_code == 200:

            json_data = response.json()
            # print(json_data)
            json_data = json_data['boxScoreTraditional']

            # Home and road team and player stats
            team_info = {}
            team_info['roadTeam'] = json_data['awayTeam']['teamId']
            team_info['roadTeamStarters'] = [(json_data['awayTeam']['players'][x]['personId'], json_data['awayTeam']['players'][x]['position']) for x in range(5)]
            team_info['roadTeamStarters'].sort(key= lambda x :("GFC".index(x[1]), x)) # Sorting by G -> F -> C (increasing position)
            team_info['roadTeamStarters'] = [x[0] for x in team_info['roadTeamStarters']]

            team_info['homeTeam'] = json_data['homeTeam']['teamId']
            team_info['homeTeamStarters'] = [(json_data['homeTeam']['players'][x]['personId'], json_data['homeTeam']['players'][x]['position']) for x in range(5)]
            team_info['homeTeamStarters'].sort(key= lambda x :("GFC".index(x[1]), x))
            team_info['homeTeamStarters'] = [x[0] for x in team_info['homeTeamStarters']]

            # Score information for home/road team
            scores = {}
            scores['roadScore'] = json_data['awayTeam']['statistics']['points']
            scores['homeScore'] = json_data['homeTeam']['statistics']['points']

            self.log.info(f"Retrieved game {id_to_team[team_info['roadTeam']]} @ {id_to_team[team_info['homeTeam']]}. Score: {scores['roadScore']}-{scores['homeScore']}")
            
            return team_info, scores
            

        else:
            self.log.fail(f"Error: {response.status_code}")
            return []
    
if __name__ == '__main__':
    game_scraper = GameScraper()
    data = game_scraper.unpack_teams("0022300347")
    print(data)
    
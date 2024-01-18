#!/usr/bin/env python
import sys, os
parent_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(parent_directory)

from parameters.info import team_to_id, id_to_team
import requests
import json
import pandas as pd
from misc.logger import Logger
from datetime import datetime, timedelta

from typing import List, Tuple

file_directory = os.path.dirname(__file__)

class StatsScraper:
    """
    Given a specific date, return the list of all game IDs
    """
    def __init__(self, verbose:bool=False):

        self.url = 'https://stats.nba.com/stats/teamplayerdashboard'

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
            'SeasonType': 'Regular Season',
            'TeamID': '1610612738',
        }

        self.headers= {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36',
            'Referer': 'https://www.nba.com/'
        }
    
        with open(os.path.join(parent_directory, 'parameters/features.json'), 'r') as file:
            data = json.load(file)
            self.player_features = data['player_features']
            self.team_features = data['team_features']
        
        self.log = Logger("TimeScraper", enabled=verbose, indent=1)
    
    def _generate_season(self, date_str):
        date_object = datetime.strptime(date_str, "%m/%d/%y")
        month = date_object.month
        year = date_object.year
        if month > 8:
            season = f"{year}-{str(year + 1)[2:]}"
        else:
            season = f"{year - 1}-{str(year)[2:]}"
        return season
    
    def get_stats(self, team_id:int, location:str='', date:str='', player_ids:List[int]=None) -> Tuple[pd.DataFrame]:
        """
        Retrieves statistics for a specific team given location, date, and
        player specifications.

        Location must be 'Home', or 'Road'.

        Returns two dataframes: first the player df, then the overall team df.
        """

        if location != 'Road' and location != 'Home' and location != '':
            self.log.warn("Invalid location specified, setting to Home by default.")
            location = 'Home'

        # Support two types of data: direct team ID number, or the name.
        if type(team_id) == int:
            team_id = str(team_id)
        else:
            team_id = team_to_id[team_id]

        # Set payload parameters
        self.payload['TeamID'] = team_id
        self.payload['Location'] = location
        self.payload['DateTo'] = date
        date_start = (datetime.strptime(date, "%m/%d/%y") - timedelta(weeks=3)).strftime("%m/%d/%y")
        self.payload['DateFrom'] = date_start
        self.payload['Season'] = self._generate_season(date)

        # Obtain JSON data
        response = requests.get(self.url, headers=self.headers, params=self.payload)
        if response.status_code == 200:
            json_data = response.json()

            # Obtain player data and convert to pandas dataframe
            player_data = json_data['resultSets'][1]['rowSet']
            player_df = pd.DataFrame(player_data, columns=json_data['resultSets'][1]['headers'])
            player_df = player_df[self.player_features]
            player_df = player_df.reset_index(drop=True)

            # filter players if requested
            if player_ids != None:
                player_df = player_df[player_df['PLAYER_ID'].isin(player_ids)]
                player_df['PLAYER_ID'] = pd.Categorical(player_df['PLAYER_ID'], categories=player_ids, ordered=True)
                player_df = player_df.sort_values('PLAYER_ID')


            # Obtain team data and convert to pandas dataframe
            team_data = json_data['resultSets'][0]['rowSet']
            team_df = pd.DataFrame(team_data, columns=json_data['resultSets'][0]['headers'])
            team_df = team_df[self.team_features]
            team_df = team_df.reset_index(drop=True)

            return player_df, team_df

        else:
            print(f"Error: {response.status_code}")
            print(response.text)
            return []


if __name__=='__main__':
    ps = StatsScraper()

    player_df, team_df = ps.get_stats('Boston Celtics', date='1/4/22', location='Home')
    print(player_df)
    print()
    print(team_df)
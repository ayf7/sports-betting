"""
Consists of all information
"""
import json

# ========= TEAM IDS AND NAMES =================================================

id_to_team = {
  1610612737: 'Atlanta Hawks',
  1610612738: 'Boston Celtics',
  1610612751: 'Brooklyn Nets',
  1610612766: 'Charlotte Hornets',
  1610612741: 'Chicago Bulls',
  1610612739: 'Cleveland Cavaliers',
  1610612742: 'Dallas Mavericks',
  1610612743: 'Denver Nuggets',
  1610612765: 'Detroit Pistons',
  1610612744: 'Golden State Warriors',
  1610612745: 'Houston Rockets',
  1610612754: 'Indiana Pacers',
  1610612746: 'Los Angeles Clippers',
  1610612747: 'Los Angeles Lakers',
  1610612763: 'Memphis Grizzlies',
  1610612748: 'Miami Heat',
  1610612749: 'Milwaukee Bucks',
  1610612750: 'Minnesota Timberwolves',
  1610612740: 'New Orleans Pelicans',
  1610612752: 'New York Knicks',
  1610612760: 'Oklahoma City Thunder',
  1610612753: 'Orlando Magic',
  1610612755: 'Philadelphia 76ers',
  1610612756: 'Phoenix Suns',
  1610612757: 'Portland Trail Blazers',
  1610612758: 'Sacramento Kings',
  1610612759: 'San Antonio Spurs',
  1610612761: 'Toronto Raptors',
  1610612762: 'Utah Jazz',
  1610612764: 'Washington Wizards',
}

team_to_id = {team: id for id, team in id_to_team.items()}

# ========= PLAYER IDS AND NAMES ===============================================



# ========= SEASON DATES =======================================================

seasons = {
  "2019-20": {
    "startDate": "11/7/2019",
    "endDate": "3/10/2020"
  },
  "2020-21": {
    "startDate": "12/22/2020",
    "endDate": "5/16/2021"
  },
  "2021-22": {
    "startDate": "11/7/2021",
    "endDate": "4/10/2022"
  },
  "2022-23": {
    "startDate": "11/7/2022",
    "endDate": "4/9/2023"
  },
  "2023-24": {
    "startDate": "11/7/2023",
    "endDate": "1/16/2024"
  }
}
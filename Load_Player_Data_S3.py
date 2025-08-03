from player_functions import *


#Set AWS Variables
client = boto3.client('s3')
BUCKET_NAME = 's3 bucket name'
PREFIX = 's3 bucket prefix'

#Set Looping Variables
seasons = [str(season) for season in range(2014,2025)]
teams = ['crd','atl', 'rav', 'buf', 'car', 'chi','cin', 'cle', 'dal', 'den', 'det', 'gnb', 'htx', 'clt', 'jax', 'kan',
         'sdg', 'ram', 'rai', 'mia', 'min', 'nwe', 'nor', 'nyg', 'nyj', 'phi', 'pit', 'sea', 'sfo', 'tam', 'oti', 'was']
positions = ['QB', 'RB', 'WR', 'TE', 'K']

#Loop Through Seasons
loop_through_nfl_seasons(seasons, teams, positions)

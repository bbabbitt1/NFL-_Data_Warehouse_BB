#Import necessary libraries

import boto3
import numpy as np
import pandas as pd
import random
import time
import lxml
import html5lib 
import bs4
import io
from football_functions import *

#AWS Credentials 
client = boto3.client('s3')
BUCKET_NAME = 'your-bucket-name'
PREFIX = 'your-prefix-name'

#Set seasons and teams for looping
seasons = [str(season) for season in range(2014,2025)]
vegas_teams = ['crd','atl', 'rav', 'buf', 'car', 'chi','cin', 'cle', 'dal', 'den', 'det', 'gnb', 'htx', 'clt', 'jax', 'kan',
         'sdg', 'ram', 'rai', 'mia', 'min', 'nwe', 'nor', 'nyg', 'nyj', 'phi', 'pit', 'sea', 'sfo', 'tam', 'oti', 'was']

#Run Functions
loop_through_seasons_and_teams(seasons, vegas_teams)

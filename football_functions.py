import boto3
import numpy as np
import pandas as pd
import random
import time
import lxml
import html5lib 
import bs4
import io


#Create Functions
def clean_vegas_df(df):
    #define functions 
    def win_loss_tie(row):
        if row[0]== 'W':
            return 'Won'
        elif row[0]== 'L':
            return 'Lost'
        else:
            return 'Tied' 
    def points_scored(row):
        return row.split(', ')[1].split('-')[0]
    def points_allowed(row):
        return row.split(', ')[1].split('-')[1]
    def game_type(row):
        if int(season) <= 2020 and int(row) < 17:
            return 1
        elif int(season) <= 2020 and int(row) >= 17:
            return 0
        elif int(season) > 2020 and int(row) < 18:
            return 1
        elif int(season) > 2020 and int(row) >= 18:
            return 0
        else:
            return 0
    #add columns
    df['Home'] = df['Opp'].apply(lambda x: 0 if x.startswith('@') else 1)
    df['Opp'] = df['Opp'].apply(lambda x: x.strip('@'))
    df['game_result'] = df.apply(lambda x: win_loss_tie(x), axis=1)
    df['points_scored'] = df['Result'].apply(lambda x: points_scored(x))
    df['points_allowed'] = df['Result'].apply(lambda x: points_allowed(x))
    df.insert(loc=0,column='Season', value = season)
    df['regular_season'] = df['G#'].apply(lambda x: game_type(x))

    #drop result column
    df.drop(columns=['Result'], inplace=True)

    #rename columns
    df = df.rename(columns={'Over/Under':'over_under','vs. Line':'spread_result','OU Result':'over_under_result'}) 

    #return df 
    return df
  
def upload_to_s3(df, team, season):
    #Create CSV in string buffer
    csv_str_buffer = io.StringIO()
    df.to_csv(csv_str_buffer, index=False)

    # Convert to bytes
    csv_bytes_buffer = io.BytesIO(csv_str_buffer.getvalue().encode("utf-8"))
    csv_bytes_buffer.seek(0)

    # Upload to S3 
    client.upload_fileobj( csv_bytes_buffer, BUCKET_NAME, PREFIX + f'season={season} /team={team}/{season}_{team}_vegas_data.csv')
    print(f"Uploaded {season}_{team}_vegas_data.csv to S3 bucket {BUCKET_NAME} with prefix {PREFIX}")

def loop_through_seasons_and_teams(seasons, vegas_teams):
    for season in seasons:
        #Iterate through teams
        for team in vegas_teams:
            # Print the team and season being processed
            print(f"Processing team: {team} for season: {season}")

            #grab pro-football reference URL
            url = 'https://www.pro-football-reference.com/teams/' + team + '/' + season + '_lines.htm'
            
            #Create DataFrame from HTML table
            # Use the table ID to find the correct table 
            df = pd.read_html(url, header=0, attrs={'id':'vegas_lines'})[0]
            
            # Clean the DataFrame
            df = clean_vegas_df(df)

            df.insert(loc=2, column='Team', value = team.upper())
            
            #Upload to S3
            upload_to_s3(df, team, season)

            #Sleep for a random time between 6 and 8 seconds to abide by the website's terms of service
            sleep_time = random.randint(6, 8)
            time.sleep(sleep_time)
            print(f"Paused for {sleep_time} seconds to respect website's terms of service.")

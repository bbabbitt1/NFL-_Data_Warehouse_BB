import pandas as pd
import urllib.request
from bs4 import BeautifulSoup, Comment
from lxml import html
import random
import time
import lxml
import html5lib
import bs4
import io
import boto3

client = boto3.client('s3')
BUCKET_NAME = 's3 bucket name'
PREFIX = 's3 bucket prefix'


def loop_through_nfl_seasons(seasons,teams,positions):
    for season in seasons:
        for team in teams:
            team_url = f'https://www.pro-football-reference.com/teams/{team}/{season}_roster.htm'
            team_id = 'roster'
            stats_id = 'stats'

            req = urllib.request.Request(team_url, headers={'User-Agent': 'Mozilla/5.0'})

            with urllib.request.urlopen(req) as response:
                page = response.read()

            # Step 2: Parse with BeautifulSoup to find HTML comments
            soup = BeautifulSoup(page, 'lxml')
            comments = soup.find_all(string=lambda text: isinstance(text, Comment))

            roster_html = None
            for comment in comments:
                if 'table' in comment and 'id="roster"' in comment:
                    roster_html = comment
                    break
            if roster_html:
                df_roster = pd.read_html(roster_html, attrs={'id': 'roster'})[0]
                tree = html.fromstring(roster_html)
                player_links = tree.xpath('//table[@id="roster"]//td[@data-stat="player"]/a/@href')
                while len(player_links) < len(df_roster):
                    player_links.append(None)

                df_roster['Player_URL'] = player_links
                df_roster['Full_Player_URL'] = [
                    f'https://www.pro-football-reference.com{href.replace(".htm", "")}/gamelog/{season}' if href else None
                    for href in player_links
                ]
            else:
                raise ValueError("Roster table not found in HTML comments.")
            df_roster = df_roster[['Player', 'Pos', 'G', 'GS', 'Player_URL', 'Full_Player_URL']]
            df_roster = df_roster[df_roster['Pos'].isin(positions)]

            for player_name, position, url in zip(df_roster['Player'], df_roster['Pos'], df_roster['Full_Player_URL']):
                try:
                    # Load the player's stats table (if it exists)
                    player_df = pd.read_html(url, header=1, attrs={'id': 'stats'})[0]
                    player_df.insert(loc=0, column='Player', value=player_name)
                    player_df.insert(loc=1, column='Position', value=position)
                    player_df.insert(loc=2, column='Season', value=season)
                    player_df.rename(columns={'Unnamed: 6': 'Home'}, inplace=True)
                    player_df['Home'] = player_df['Home'].apply(lambda x: 0 if x == '@' else 1)
                    player_df = player_df[:-1]
                    # Create CSV in string buffer
                    csv_str_buffer = io.StringIO()
                    player_df.to_csv(csv_str_buffer, index=False)

                    #         # Convert to bytes
                    csv_bytes_buffer = io.BytesIO(csv_str_buffer.getvalue().encode("utf-8"))
                    csv_bytes_buffer.seek(0)

                    #         # Upload to S3
                    client.upload_fileobj(csv_bytes_buffer, BUCKET_NAME,
                                          PREFIX + f'season={season} /position={position}/{season}_{position}_{player_name}_data.csv')

                    # Print confirmation
                    print(
                        f"Uploaded {season}_{position}_{player_name}.csv to S3 bucket {BUCKET_NAME} with prefix {PREFIX}")

                    sleep_time = random.randint(6, 8)
                    time.sleep(sleep_time)
                    print(f"Paused for {sleep_time} seconds to respect website's terms of service.")

                except Exception as e:
                    print(f"Failed to read {player_name} at {url}: {e}")

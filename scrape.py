import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import boto3
from io import StringIO

s3_client = boto3.client('s3')

def upload_to_s3(df, bucket_name, file_name):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    s3_client.put_object(Bucket=bucket_name, Key=file_name, Body=csv_buffer.getvalue())

def league_table():
    url = 'https://www.bbc.com/sport/football/premier-league/table'
    headers = []
    page = requests.get(url)
    soup = BeautifulSoup(page.text, "html.parser")
    table = soup.find("table", class_="ssrcss-14j0ip6-Table e3bga5w5")

    for i in table.find_all('th'):
        headers.append(i.text)
    league_table = pd.DataFrame(columns=headers)
    for j in table.find_all('tr')[1:]:
        row_data = j.find_all('td')
        row = [i.text for i in row_data]
        length = len(league_table)
        league_table.loc[length] = row
    league_table.drop(["Form, Last 6 games, Oldest first"], axis=1, inplace=True)
    
    upload_to_s3(league_table, 'premier-league-data', 'league_table.csv')
    return league_table

def top_scorers():
    url = 'https://www.bbc.com/sport/football/premier-league/top-scorers'
    headers = []
    page = requests.get(url)
    soup = BeautifulSoup(page.text, "html.parser")
    table = soup.find("table", class_="gs-o-table")

    for i in table.find_all('th'):
        headers.append(i.text)
    top_scorers = pd.DataFrame(columns=headers)
    for j in table.find_all('tr')[1:]:
        row_data = j.find_all('td')
        row = [i.text for i in row_data]
        length = len(top_scorers)
        top_scorers.loc[length] = row
    
    upload_to_s3(top_scorers, 'premier-league-data', 'top_scorers.csv')
    return top_scorers

def detail_top():
    url = 'https://www.worldfootball.net/goalgetter/eng-premier-league-2023-2024/'
    headers = []
    page = requests.get(url)
    soup = BeautifulSoup(page.text, "html.parser")
    table = soup.find("table", class_="standard_tabelle")

    for i in table.find_all('th'):
        headers.append(i.text)
    detail_top_scorer = pd.DataFrame(columns=headers)
    for j in table.find_all('tr')[1:]:
        row_data = j.find_all('td')
        row = [i.text for i in row_data]
        length = len(detail_top_scorer)
        detail_top_scorer.loc[length] = row
    
    upload_to_s3(detail_top_scorer, 'premier-league-data', 'detailed_top_scorers.csv')
    return detail_top_scorer

def all_time_table():
    url = 'https://www.worldfootball.net/alltime_table/eng-premier-league/pl-only/'
    headers = ['pos','#', 'Team',' Matches', 'wins', 'Draws', 'Losses', 'Goals' ,'Dif', 'Points']
    page = requests.get(url)
    soup = BeautifulSoup(page.text, "html.parser")
    table = soup.find("table", class_="standard_tabelle")

    alltime_table = pd.DataFrame(columns=headers)
    for j in table.find_all('tr')[1:]:
        row_data = j.find_all('td')
        row = [i.text for i in row_data]
        length = len(alltime_table)
        alltime_table.loc[length] = row
    
    upload_to_s3(alltime_table, 'premier-league-data', 'all_time_table.csv')
    return alltime_table

def goals_per_season():
    url = 'https://www.worldfootball.net/stats/eng-premier-league/1/'
    headers = []
    page = requests.get(url)
    soup = BeautifulSoup(page.text, "html.parser")
    table = soup.find("table", class_="standard_tabelle")

    for i in table.find_all('th'):
        headers.append(i.text)
    goals_per_season = pd.DataFrame(columns=headers)
    for j in table.find_all('tr')[1:]:
        row_data = j.find_all('td')
        row = [i.text for i in row_data]
        length = len(goals_per_season)
        goals_per_season.loc[length] = row
    goals_per_season.drop(goals_per_season.index[-1], inplace=True)
    
    upload_to_s3(goals_per_season, 'premier-league-data', 'goals_per_season.csv')
    return goals_per_season

# Example function calls
if __name__ == "__main__":
    league_table()
    top_scorers()
    detail_top()
    all_time_table()
    goals_per_season()

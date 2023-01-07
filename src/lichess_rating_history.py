import requests
import pandas as pd
import os

def write_lichess_data_to_csv():
    usernames = ['pcmcd']
    modes = ["ultraBullet","bullet","blitz","rapid","classical","correspondence","chess960","crazyhouse","antichess","atomic","horde","kingOfTheHill","racingKings","threeCheck"]

    url = 'https://lichess.org/api/user/{username}/perf/{perf}'

    #generate url for each player
    urls = []
    for player in usernames:
        urls.append(url.replace("{username}",player).replace("{perf}",modes[2]))
    #urls

    response = requests.get(url = urls[0])
    json_response = response.json()

    data = {}
    username = json_response['user']['name']

    rating = json_response['perf']['glicko']['rating']
    rating_deviation = json_response['perf']['glicko']['deviation']
    rating_games_played = json_response['perf']['nb']
    recent_progress = json_response['perf']['progress']

    rating_rank = json_response['rank']
    rating_percentile = json_response['percentile']

    wins = json_response['stat']['count']['win']
    draws = json_response['stat']['count']['draw']
    losses = json_response['stat']['count']['loss']

    data[(username,modes[2])] = {"rating":rating,"rating_deviation":rating_deviation,"rating_games_played":rating_games_played,"recent_progress":recent_progress,"rating_rank":rating_rank,"rating_percentile":rating_percentile,"wins":wins,"draws":draws,"losses":losses}

    #data[('pcmcd','blitz')]

    #dictionary to dataframe
    df = pd.DataFrame.from_dict(data,orient='index').reset_index()
    df = df.rename(columns={"level_0":"username","level_1":"game_mode"})
    #df.head()

    #write to csv

    #create direcory if not exists
    #TODO
    #if file already exists, only append, else full write
    #TODO

    #df.to_csv("\lichess_rating_history.csv",index_label='row')
    return print(df.head(5))

#write_lichess_data_to_csv()
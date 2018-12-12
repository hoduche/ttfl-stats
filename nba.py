import argparse
from constants import headers
import datetime as dt
from dateutil import parser
import json
import requests
import pandas as pd
import pytz


def __compute_game_date(date=None):
    if date is None:
        return dt.datetime.now(pytz.timezone('EST')).date()
    elif isinstance(date, str):
        return parser.parse(date).date()
    else:
        return date


def __compute_season_start_year(date):
    date = __compute_game_date(date)
    if date.month in range(1, 8):
        return date.year - 1
    elif date.month in range(8, 13):
        return date.year


def __build_gameids(date):
    date = __compute_game_date(date)
    year = __compute_season_start_year(date)
    with open('DataWarehouse/schedule' + str(year) + '_dict.json', 'r') as schedule_file:
        schedule = json.loads(schedule_file.read())
    return [each[0] for each in schedule[str(date)]]


def __build_game_url(gameId, startPeriod=0, endPeriod=14, startRange=0, endRange=2147483647, rangeType=0):
    domain = 'https://stats.nba.com'
    endpoint = 'stats/boxscoretraditionalv2'
    parameters = ('gameId=' + gameId + '&startPeriod=0&endPeriod=14&startRange=0&endRange=2147483647&rangeType=0')
    game_url = domain + '/' + endpoint + '/?' + parameters
    return game_url


def __build_game_urls(date):
    return [__build_game_url(each_gameid) for each_gameid in __build_gameids(date)]


def __download_boxscores(date):
    return [json.loads(requests.get(each_url, headers=headers).text) for each_url in __build_game_urls(date)]


def __save_boxscores(boxscores):
    for each_boxscore in boxscores:
        with open('DataLake/boxscore' + each_boxscore['parameters']['GameID'] + '.json', 'w+') as output_file:
            json.dump(each_boxscore, output_file)


def __get_game_player_stats(boxscores):
    player_stats = []
    for each_boxscore in boxscores:
        for each_result in each_boxscore['resultSets']:
            if each_result['name'] == 'PlayerStats':
                player_stats.append(each_result)
    return player_stats


def __concat_game_player_stats(boxscores):
    player_stats = __get_game_player_stats(boxscores)
    dfs = [pd.DataFrame(each_player_stats['rowSet'], columns=each_player_stats['headers']) for each_player_stats in player_stats]
    return pd.concat(dfs)


def compute_player_totals(df):
    result = pd.DataFrame(df.PTS + df.REB + df.AST + df.STL + df.BLK - df.TO + 2*df.FGM - df.FGA + 2*df.FG3M - df.FG3A + 2*df.FTM - df.FTA, columns=['TOTAL'])
    result = result.set_index(df.PLAYER_NAME)
    return result


if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('-d', '--date', help='enter date', type=str, default=None)
    args = arg_parser.parse_args()
    boxscores = __download_boxscores(args.date)
    __save_boxscores(boxscores)
    in_df = __concat_game_player_stats(boxscores)
    out_df = compute_player_totals(in_df)
    out_df = out_df.sort_values(['TOTAL'], ascending=[0])
    out_df.to_html('ttfl_output.html')

import argparse
import collections
import datetime as dt
import json
import pylab

from dateutil import parser
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pathlib
import pytz
import requests
import six
from twython import Twython

from constants import nba_headers
from auth import (consumer_key, consumer_secret, access_token, access_token_secret)


def __sanity_check(verbose=False):
    if not pathlib.Path('DataLake').is_dir():
        if verbose:
            print('Create DataLake repository')
        pathlib.Path('DataLake').mkdir()
    if not pathlib.Path('DataWarehouse').is_dir():
        if verbose:
            print('Create DataWarehouse repository')
        pathlib.Path('DataWarehouse').mkdir()


def __get_eastern_date(date=None):
    if date is None:
        return dt.datetime.now(pytz.timezone('EST')).date()
    elif isinstance(date, str):
        return parser.parse(date).date()
    else:
        return date


def __get_season_start_year(date=None):
    date = __get_eastern_date(date)
    if date.month in range(1, 8):
        return date.year - 1
    elif date.month in range(8, 13):
        return date.year


def __build_game_url_v1(gameId, eastern_date):
    domain = 'http://data.nba.net'
    endpoint = 'prod/v1'
    extension = '_boxscore.json'
    game_url = domain + '/' + endpoint + '/' + eastern_date.strftime('%Y%m%d') + '/' + gameId + extension
    return game_url


def __build_game_url_v2(gameId):
    domain = 'https://stats.nba.com'
    endpoint = 'stats/boxscoretraditionalv2'
    parameters = 'gameId=' + gameId + '&startPeriod=0&endPeriod=14&startRange=0&endRange=2147483647&rangeType=0'
    game_url = domain + '/' + endpoint + '/?' + parameters
    return game_url


def __render_mpl_table(data, col_width=3.3, row_height=0.625, font_size=12,
                     header_color='#40466e', row_colors=['#f1f1f2', 'w'], edge_color='black',
                     bbox=[0, 0, 1, 1], header_columns=0, ax=None, **kwargs):
# https://stackoverflow.com/questions/26678467/export-a-pandas-dataframe-as-a-table-image
    if ax is None:
        size = (np.array(data.shape[::-1]) + np.array([0, 1])) * np.array([col_width, row_height])
        fig, ax = plt.subplots(figsize=size)
        ax.axis('off')

    mpl_table = ax.table(cellText=data.values, bbox=bbox, colLabels=data.columns, **kwargs)

    mpl_table.auto_set_font_size(False)
    mpl_table.set_fontsize(font_size)

    for k, cell in  six.iteritems(mpl_table._cells):
        cell.set_edgecolor(edge_color)
        if k[0] == 0 or k[1] < header_columns:
            cell.set_text_props(weight='bold', color='w')
            cell.set_facecolor(header_color)
        else:
            cell.set_facecolor(row_colors[k[0]%len(row_colors) ])
    return ax


def get_nba_schedule(year, verbose=False):
    """pipeline for downloading, transforming and saving nba season schedule

    get nba season(year, year + 1) schedule from datawarehouse
    if not there yet, download it from nba website, save it to datalake, convert it
    to a python dict and save it to datawarehouse

    Arguments:
        year {int} -- beginning year of the season
        verbose {bool} -- display logs when activated

    Returns:
        dict -- key    = startDateEastern,
                values = (gameId, startTimeUTC)
    """

    __sanity_check(verbose)

    if pathlib.Path('DataWarehouse/schedule_' + str(year) + '_dict.json').is_file():
        if verbose:
            print('Load season ' + str(year) + '/' + str(year+1) + ' schedule from DataWarehouse')
        with open('DataWarehouse/schedule_' + str(year) + '_dict.json', 'r') as schedule_dict_cached:
            schedule_dict = json.loads(schedule_dict_cached.read())
    else:
        if verbose:
            print('Download season ' + str(year) + '/' + str(year+1) + ' schedule from NBA website')
        url = 'http://data.nba.net/prod/v2/' + str(year) + '/schedule.json'
        response = requests.get(url, headers=nba_headers)
        schedule_json = json.loads(response.text)
        if verbose:
            print('Save season ' + str(year) + '/' + str(year+1) + ' schedule to DataLake')
        with open('DataLake/schedule_' + str(year) + '.json', 'w+') as schedule_json_cached:
            json.dump(schedule_json, schedule_json_cached)
        schedule_dict = collections.defaultdict(list)
        for each_game in schedule_json['league']['standard']:
            eastern_date = parser.parse(each_game['startDateEastern']).strftime('%Y%m%d')  # str forced by json
            schedule_dict[eastern_date].append((each_game['gameId'], each_game['startTimeUTC']))
        if verbose:
            print('Save season ' + str(year) + '/' + str(year+1) + ' schedule to DataWarehouse')
        with open('DataWarehouse/schedule_' + str(year) + '_dict.json', 'w+') as schedule_dict_cached:
            json.dump(schedule_dict, schedule_dict_cached)

    return schedule_dict


def compute_ttfl_statistics(eastern_date_string=None, verbose=False):
    """pipeline for downloading daily nba boxscores, computing and saving ttfl totals for all players

    get daily nba boxscores from datalake (if not there yet, download them from nba website),
    compute ttfl daily totals for each player and save them to datawarehouse

    Arguments:
        eastern_date_string {string} -- eastern date with approximately 3 to 10 games played that day
                                        many string format accepted: '20181214', '14 Dec 2018', ...
                                        if no date passed, take the current eastern date when calling the function
        verbose {bool} -- display logs when activated

    Returns:
        pandas dataframe -- ranking of daily players with their ttfl total
    """

    eastern_date = __get_eastern_date(eastern_date_string)
    year = __get_season_start_year(eastern_date)

    games_ttfl_v1 = []
    for each_game in get_nba_schedule(year, verbose)[eastern_date.strftime('%Y%m%d')]:
        game_id = each_game[0]
        game_url_v1 = __build_game_url_v1(game_id, eastern_date)
        if verbose:
            print('Download game ' + game_id + ' boxscore v1 from NBA website')
        boxscore_json_v1 = json.loads(requests.get(game_url_v1, headers=nba_headers).text)
        if verbose:
            print('Save game ' + game_id + ' boxscore v1 to DataLake')
        with open('DataLake/boxscore_' + game_id + '_v1.json', 'w+') as boxscore_json_v1_cached:
            json.dump(boxscore_json_v1, boxscore_json_v1_cached)
        if verbose:
            print('Compute TTFL totals for game ' + game_id + ' from boxscore v1')
        if 'stats' in boxscore_json_v1.keys():
            series_list = [pd.to_numeric(pd.Series(each_result), errors='coerce') for each_result in boxscore_json_v1['stats']['activePlayers']]
            df = pd.DataFrame(series_list)
            game_ttfl_v1 = pd.DataFrame(df['personId']).rename(columns={'personId': 'PLAYER_ID'})
            game_ttfl_v1['TOTAL_V1'] = df.points + df.totReb + df.assists + df.steals + df['blocks'] - df.turnovers + 2 * df.fgm \
                                    - df.fga + 2 * df.tpm - df.tpa + 2 * df.ftm - df.fta
            games_ttfl_v1.append(game_ttfl_v1)
    if games_ttfl_v1:
        day_ttfl_v1 = pd.concat(games_ttfl_v1).fillna(0)
        numeric_columns = ['PLAYER_ID', 'TOTAL_V1']
        day_ttfl_v1[numeric_columns] = day_ttfl_v1[numeric_columns].astype(int)

    games_ttfl_v2 = []
    for each_game in get_nba_schedule(year, verbose)[eastern_date.strftime('%Y%m%d')]:
        game_id = each_game[0]
        game_url_v2 = __build_game_url_v2(game_id)
        if verbose:
            print('Download game ' + game_id + ' boxscore v2 from NBA website')
        boxscore_json_v2 = json.loads(requests.get(game_url_v2, headers=nba_headers).text)
        if verbose:
            print('Save game ' + game_id + ' boxscore v2 to DataLake')
        with open('DataLake/boxscore_' + game_id + '_v2.json', 'w+') as boxscore_json_v2_cached:
            json.dump(boxscore_json_v2, boxscore_json_v2_cached)
        if verbose:
            print('Compute TTFL totals for game ' + game_id + ' from boxscore v2')
        series_list = []
        for each_result in boxscore_json_v2['resultSets']:
            if each_result['name'] == 'PlayerStats':
                df = pd.DataFrame(each_result['rowSet'], columns=each_result['headers'])
                game_ttfl_v2 = df[['PLAYER_ID', 'PLAYER_NAME']].copy()
                game_ttfl_v2['TOTAL_V2'] = df.PTS + df.REB + df.AST + df.STL + df.BLK - df.TO + 2 * df.FGM \
                                           - df.FGA + 2 * df.FG3M - df.FG3A + 2 * df.FTM - df.FTA
                games_ttfl_v2.append(game_ttfl_v2)
                break
    day_ttfl_v2 = pd.concat(games_ttfl_v2).fillna(0)
    numeric_columns = ['PLAYER_ID', 'TOTAL_V2']
    day_ttfl_v2[numeric_columns] = day_ttfl_v2[numeric_columns].astype(int)

    if games_ttfl_v1:
        day_ttfl = pd.merge(day_ttfl_v1, day_ttfl_v2, on='PLAYER_ID', how='left')
        day_ttfl = day_ttfl[['PLAYER_ID', 'PLAYER_NAME', 'TOTAL_V1', 'TOTAL_V2']]
        day_ttfl = day_ttfl.sort_values(['TOTAL_V1'], ascending=[0])
        day_ttfl = day_ttfl.set_index('PLAYER_ID')
    else:
        day_ttfl = day_ttfl_v2
        day_ttfl = day_ttfl.set_index('PLAYER_ID')

    if verbose:
        print('Save TTFL totals for the ' + str(len(day_ttfl_v2)) + ' games on '+ eastern_date.strftime('%Y/%m/%d') + ' to DataWareHouse')
    with open('DataWarehouse/ttfl_' + eastern_date.strftime('%Y%m%d') + '.json', 'w+') as day_ttfl_cached:
        json.dump(day_ttfl.to_json(orient='values'), day_ttfl_cached)

    return day_ttfl


if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser()
    arg_parser = argparse.ArgumentParser(description='Compute, save and tweet Top 30 ttfl totals for the day')
    arg_parser.add_argument('-d', '--date', type=str, default=None, help='eastern date with approximately '
                                                                         '3 to 10 games played that day')
    arg_parser.add_argument('-v', '--verbose', action='store_true', help='display logs when activated')
    args = arg_parser.parse_args()
    day_ttfl = compute_ttfl_statistics(args.date, args.verbose)

    output = __render_mpl_table(day_ttfl.head(30))
    pylab.savefig('ttfl_output.png', bbox_inches='tight')

    twitter = Twython(consumer_key, consumer_secret, access_token, access_token_secret)
    message = 'TTFL totals for ' + __get_eastern_date(args.date).strftime('%Y/%m/%d')
    with open('ttfl_output.png', 'rb') as photo:
        response = twitter.upload_media(media=photo)
    twitter.update_status(status=message, media_ids=[response['media_id']])

    day_ttfl.head(30).reset_index().to_html('ttfl_output.html')

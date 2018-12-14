import argparse
import collections
import datetime as dt
import json
import pathlib

from dateutil import parser
import requests
import pandas as pd
import pytz

from constants import nba_headers


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


def __build_game_url(gameId, startPeriod=0, endPeriod=14, startRange=0, endRange=2147483647, rangeType=0):
    domain = 'https://stats.nba.com'
    endpoint = 'stats/boxscoretraditionalv2'
    parameters = ('gameId=' + gameId + '&startPeriod=0&endPeriod=14&startRange=0&endRange=2147483647&rangeType=0')
    game_url = domain + '/' + endpoint + '/?' + parameters
    return game_url


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

    if pathlib.Path('DataWarehouse/schedule' + str(year) + '_dict.json').is_file():
        if verbose:
            print('Load season ' + str(year) + '/' + str(year+1) + ' schedule from DataWarehouse')
        with open('DataWarehouse/schedule' + str(year) + '_dict.json', 'r') as schedule_dict_cached:
            schedule_dict = json.loads(schedule_dict_cached.read())
    else:
        if verbose:
            print('Download season ' + str(year) + '/' + str(year+1) + ' schedule from NBA website')
        url = 'http://data.nba.net/prod/v2/' + str(year) + '/schedule.json'
        response = requests.get(url, headers=nba_headers)
        schedule_json = json.loads(response.text)
        if verbose:
            print('Save season ' + str(year) + '/' + str(year+1) + ' schedule to DataLake')
        with open('DataLake/schedule' + str(year) + '.json', 'w+') as schedule_json_cached:
            json.dump(schedule_json, schedule_json_cached)
        schedule_dict = collections.defaultdict(list)
        for each_game in schedule_json['league']['standard']:
            eastern_date = str(parser.parse(each_game['startDateEastern']).date())  # str forced by json
            schedule_dict[eastern_date].append((each_game['gameId'], each_game['startTimeUTC']))
        if verbose:
            print('Save season ' + str(year) + '/' + str(year+1) + ' schedule to DataWarehouse')
        with open('DataWarehouse/schedule' + str(year) + '_dict.json', 'w+') as schedule_dict_cached:
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

    __sanity_check(verbose)

    eastern_date = __get_eastern_date(eastern_date_string)
    year = __get_season_start_year(eastern_date)
    games_ttfl = []
    for each_game in get_nba_schedule(year, verbose)[str(eastern_date)]:
        game_id = each_game[0]
        game_url = __build_game_url(game_id)
        if verbose:
            print('Download game ' + game_id + ' boxscore from NBA website')
        boxscore_json = json.loads(requests.get(game_url, headers=nba_headers).text)
        if verbose:
            print('Save game ' + game_id + ' boxscore to DataLake')
        with open('DataLake/boxscore' + game_id + '.json', 'w+') as boxscore_json_cached:
            json.dump(boxscore_json, boxscore_json_cached)
        if verbose:
            print('Compute TTFL totals for game ' + game_id)
        for each_result in boxscore_json['resultSets']:
            if each_result['name'] == 'PlayerStats':
                df = pd.DataFrame(each_result['rowSet'], columns=each_result['headers'])
                ttfl = pd.DataFrame(df.PTS + df.REB + df.AST + df.STL + df.BLK - df.TO + 2 * df.FGM
                                    - df.FGA + 2 * df.FG3M - df.FG3A + 2 * df.FTM - df.FTA, columns=['TOTAL'])
                ttfl = ttfl.set_index(df.PLAYER_NAME)
                games_ttfl.append(ttfl)
                break
    day_ttfl = pd.concat(games_ttfl)
    day_ttfl = day_ttfl.sort_values(['TOTAL'], ascending=[0])
    if verbose:
        print('Save TTFL totals for the ' + str(len(games_ttfl)) + ' games on '+ str(eastern_date) + ' to DataWareHouse')
    with open('DataWarehouse/ttfl-' + str(eastern_date) + '.json', 'w+') as day_ttfl_cached:
        json.dump(day_ttfl.to_dict(), day_ttfl_cached)

    return day_ttfl


if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser()
    arg_parser = argparse.ArgumentParser(description='Compute, save and tweet Top 30 ttfl totals for the day')
    arg_parser.add_argument('-d', '--date', type=str, default=None, help='eastern date with approximately '
                                                                         '3 to 10 games played that day')
    arg_parser.add_argument('-v', '--verbose', action='store_true', help='display logs when activated')
    args = arg_parser.parse_args()
    day_ttfl = compute_ttfl_statistics(args.date, args.verbose)
    day_ttfl.head(30).to_html('ttfl_output.html')

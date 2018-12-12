import argparse
from collections import defaultdict
from constants import headers
from dateutil import parser
import json
import requests


def build_schedule_dict(year):
    """pipeline for downloading, transforming and saving season schedule
    
    Arguments:
        year {int} -- beginning year of the season
    """
    nba_schedule = __download_nba_schedule(year)
    __save_nba_schedule(year, nba_schedule)
    dict_schedule = __transform_nba_schedule_to_dict(nba_schedule)
    __save_dict_schedule(year, dict_schedule)


def __download_nba_schedule(year):
    """downloads season (year, year + 1) schedule from nba website
    this is typically done once a year before season start

    Arguments:
        year {int} -- beginning year of the season
    
    Returns:
        json -- complete season schedule from nba website
    """
    url = 'http://data.nba.net/prod/v2/' + str(year) + '/schedule.json'
    response = requests.get(url, headers=headers)
    return json.loads(response.text)


def __save_nba_schedule(year, nba_schedule):
    """saves nba season (year, year + 1) schedule to datalake
    
    Arguments:
        year {int} -- beginning year of the season
        nba_schedule {json} -- complete season schedule from nba website
    """
    with open('DataLake/schedule' + str(year) + '.json', 'w+') as output_file:
        json.dump(nba_schedule, output_file)


def __transform_nba_schedule_to_dict(nba_schedule):
    """transforms nba season (year, year + 1) schedule to dict
    
    Arguments:
        nba_schedule {json} -- complete season schedule from nba website
    
    Returns:
        dict -- key = startDateEastern and values = (gameId, startTimeUTC)
    """
    dict_schedule = defaultdict(list)
    for each_game in nba_schedule['league']['standard']:
        eastern_date = str(parser.parse(each_game['startDateEastern']).date())
        dict_schedule[eastern_date].append((each_game['gameId'], each_game['startTimeUTC']))
    return dict_schedule


def __save_dict_schedule(year, dict_schedule):
    """saves nba season (year, year + 1) schedule dict to datalake
    
    Arguments:
        year {int} -- beginning year of the season
        schedule_dict {dict} -- key = startDateEastern and values = (gameId, startTimeUTC)
    """
    with open('DataWarehouse/schedule' + str(year) + '_dict.json', 'w+') as output_file:
        json.dump(dict_schedule, output_file)


if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('year', help='enter start year of the season', type=int)
    args = arg_parser.parse_args()
    build_schedule_dict(args.year)

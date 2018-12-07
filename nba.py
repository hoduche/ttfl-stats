import json
import requests
import pandas as pd

def build_url(gameId, startPeriod=0, endPeriod=14, startRange=0, endRange=2147483647, rangeType=0):
    domain = 'https://stats.nba.com'
    endpoint = 'stats/boxscoretraditionalv2'
    parameters = ('gameId=' + gameId +
                  '&startPeriod=' + str(startPeriod) +
                  '&endPeriod=' + str(endPeriod) +
                  '&startRange=' + str(startRange) +
                  '&endRange=' + str(endRange) +
                  '&rangeType=' + str(rangeType))
    url = domain + '/' + endpoint + '/?' + parameters
    return url

def get_boxcore(gameId):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36'}
    response = requests.get(build_url(gameId), headers=headers)
    return json.loads(response.text)

def get_player_stats(boxscore):
    dict = __get_player_stats_dict(boxscore)
    df = pd.DataFrame(dict['rowSet'], columns=dict['headers'])
    return df

def __get_boxcore_local():
    f = open('DataWarehouse/boxCore0021600732.json', 'r')
    result = json.loads(f.read())
    f.close()
    return result

def __get_player_stats_dict(boxscore):
    for each_result in boxscore['resultSets']:
        if each_result['name'] == 'PlayerStats':
            return each_result

def compute_player_totals(df):
    result = pd.DataFrame(df.PTS + df.REB + df.AST + df.STL + df.BLK - df.TO + 2*df.FGM - df.FGA + 2*df.FG3M - df.FG3A + 2*df.FTM - df.FTA, columns=['TOTAL'])
    result = result.set_index(df.PLAYER_NAME)
    return result

def __main():
    gameId = '0021600732'
    bs = get_boxcore(gameId)
    in_df = get_player_stats(bs)
    out_df = compute_player_totals(in_df)
    out_df.to_html('ttfl_output.html')

if __name__ == '__main__':
    __main()

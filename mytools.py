import argparse
from apiclient.discovery import build
import httplib2
from oauth2client import client
from oauth2client import file
from oauth2client import tools
from pprint import pprint
import pandas as pd
import json
import requests
from requests.exceptions import ConnectionError
from time import sleep
from datetime import date, timedelta, datetime
import datetime
from datetime import datetime as dt
from datetime import date, timedelta
import time
import numpy as np
from getpass import getpass

def auth_ga():
    scope = ['https://www.googleapis.com/auth/analytics.readonly']
    api_name = 'analytics'
    api_version = 'v3'
    client_secrets_path = 'client_secret.json'
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter, parents=[tools.argparser])
    flags = parser.parse_args([])
    flow = client.flow_from_clientsecrets(client_secrets_path, scope=scope, message=tools.message_if_missing(client_secrets_path))
    storage = file.Storage(api_name + '.dat')
    credentials = storage.get()
    credentials = tools.run_flow(flow, storage, flags)


def update_token(client_id, client_secret, refresh_token):
    """Обновление токена для запросов к API. Возвращает токен"""
    url_token = 'https://accounts.google.com/o/oauth2/token'
    params = { 'client_id' : client_id, 'client_secret' : client_secret,
               'refresh_token' : refresh_token, 'grant_type' : 'refresh_token' }
    r = requests.post(url_token, data = params )
    print('Токен выдан до {}'.format(datetime.datetime.today() + timedelta( hours = 1 )))
    return r.json()['access_token']



def get_date(LastDate):
    my_date = []
    for i in range(LastDate):
        temp_date = datetime.datetime.now()
        temp_date = temp_date - timedelta(days=i+1)
        my_date.append(temp_date.strftime("%Y-%m-%d"))
    return my_date

def replacement_data(dict_date):
    df_replace = pd.read_csv('ga.csv', sep=';', encoding='utf8')
    df_replace['date'] = df_replace['date'].astype(str)
    dict_dates = [i.replace('-', '') for i in dict_date]
    for dates in dict_dates:
        df_replace = df_replace[df_replace.date.str.contains(dates) == False]
    df_replace.to_csv("ga.csv", index=False, header=True, sep=";", encoding='utf8')

def import_ga(start_dates, end_dates,dimmensions_ga,metrics_ga,filters_ga,ga_id,need_auth,LastDate,replacement):

    if need_auth == 'yes':
        auth_ga()

    config = json.load( open('analytics.dat') )
    client_id = config['client_id']
    client_secret = config['client_secret']
    refresh_token = config['refresh_token']
    token = update_token(client_id, client_secret, refresh_token)

    if LastDate > 0:
        dict_date = get_date(LastDate)
        start_dates = dict_date[-1]
        end_dates = dict_date[0]

    strtindex_ga = 1
    maxresults_ga='10000'
    start_date = datetime.datetime.strptime(start_dates, '%Y-%m-%d')
    end_date = datetime.datetime.strptime(end_dates, '%Y-%m-%d')
    delta = timedelta(days=1)
    td = datetime.datetime.date(start_date)
    df_new = pd.DataFrame()
    new_list = dimmensions_ga.split(',') + metrics_ga.split(',')
    new_dict = {}
    for i, j in enumerate (new_list):
        new_dict[i] = j.replace('ga:','')

    while start_date <= end_date:
        ddf = 0
        strtindex_ga = 1
        keyforga = 'https://www.googleapis.com/analytics/v3/data/ga?ids=ga:'+ga_id+'&start-date='+start_date.strftime('%Y-%m-%d')+'&end-date='+start_date.strftime('%Y-%m-%d')+'&metrics='+metrics_ga+'&dimensions='+dimmensions_ga +'&filters='+filters_ga+'&max-results='+maxresults_ga+'&start-index='+str(strtindex_ga)+'&access_token='+token
        keyforga = keyforga.replace(':', '%3A')
        keyforga = keyforga.replace(',', '%2C')
        keyforga = keyforga.replace('|', '%7C')
        keyforga = keyforga.replace('!', '%21')
        keyforga = keyforga.replace('=', '%3D')
        keyforga = keyforga.replace(';', '%3B')
        keyforga = keyforga.replace('%3Dga', '=ga')
        keyforga = keyforga.replace('date%3D', 'date=')
        keyforga = keyforga.replace('metrics%3D', 'metrics=')
        keyforga = keyforga.replace('dimensions%3D', 'dimensions=')
        keyforga = keyforga.replace('metrics%3D', 'metrics=')
        keyforga = keyforga.replace('filters%3D', 'filters=')
        keyforga = keyforga.replace('token%3D', 'token=')
        keyforga = keyforga.replace('index%3D', 'index=')
        keyforga = keyforga.replace('results%3D', 'results=')
        keyforga = keyforga.replace('https%3A', 'https:')
        api_query_uri = keyforga
        r = requests.get(api_query_uri)
        data= r.json()
        df = pd.DataFrame(data['rows'])
        df = df.rename(columns=new_dict)
        df_new = df_new.append(df, ignore_index = False)
        ddf = len(df)
        print(start_date.strftime('%Y-%m-%d'))
        print(ddf)

        while ddf >= 10000:
            strtindex_ga = int(strtindex_ga)
            strtindex_ga += 10000
            keyforga = 'https://www.googleapis.com/analytics/v3/data/ga?ids=ga:'+ ga_id+'&start-date='+start_date.strftime('%Y-%m-%d')+'&end-date='+start_date.strftime('%Y-%m-%d')+'&metrics='+metrics_ga+'&dimensions='+dimmensions_ga +'&filters='+filters_ga+'&max-results='+maxresults_ga+'&start-index='+str(strtindex_ga)+'&access_token='+token
            keyforga = keyforga.replace(':', '%3A')
            keyforga = keyforga.replace(',', '%2C')
            keyforga = keyforga.replace('|', '%7C')
            keyforga = keyforga.replace('!', '%21')
            keyforga = keyforga.replace('=', '%3D')
            keyforga = keyforga.replace(';', '%3B')
            keyforga = keyforga.replace('%3Dga', '=ga')
            keyforga = keyforga.replace('date%3D', 'date=')
            keyforga = keyforga.replace('metrics%3D', 'metrics=')
            keyforga = keyforga.replace('dimensions%3D', 'dimensions=')
            keyforga = keyforga.replace('metrics%3D', 'metrics=')
            keyforga = keyforga.replace('filters%3D', 'filters=')
            keyforga = keyforga.replace('token%3D', 'token=')
            keyforga = keyforga.replace('index%3D', 'index=')
            keyforga = keyforga.replace('results%3D', 'results=')
            keyforga = keyforga.replace('https%3A', 'https:')
            api_query_uri = keyforga
            r = requests.get(api_query_uri)
            data= r.json()
            df = pd.DataFrame(data['rows'])
            df = df.rename(columns=new_dict)
            df_new = df_new.append(df, ignore_index = False)
            time.sleep(0.5)
            ddf = len(df)
            print(start_date.strftime('%Y-%m-%d'))
            print(ddf)
        start_date += delta
        time.sleep(0.5)

    if replacement == 'no':
        df_new.to_csv("ga.csv", index=False, header=True, sep=";", encoding='utf8')
    else:
        replacement_data(dict_date)
        x1 = pd.read_csv('ga.csv', sep=';', encoding='utf8', header=0)
        x1 = df_new.append(x1, ignore_index=False)
        x1.to_csv('ga.csv', index=False, header=True, sep=';', encoding='utf8')

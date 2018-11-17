import pandas as pd
import numpy as np
import requests
from BeautifulSoup import BeautifulSoup
import os
from fake_useragent import UserAgent
import psycopg2
import urlparse
import os
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError
import psycopg2
import urlparse
import os
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError
import imp
try:
    ip_spoofer = imp.load_source('module', './python/IpSpoofer.py')
except:
    # # For testing
    ip_spoofer = imp.load_source('module', '../python/IpSpoofer.py')

def open_connection():
    url = urlparse.urlparse(os.environ["DATABASE_URL"])
    connection = psycopg2.connect(
        database=url.path[1:],
        user=url.username,
        password=url.password,
        host=url.hostname,
        port=url.port
        )
    return connection

def collect_player(game_id):
    ## request page
    print 'request'
    url = 'http://www.espn.com/mens-college-basketball/boxscore?gameId={}'.format(game_id)
    page = ip_spoofer.IpSpoofer.request_page(url)

    ## Find team order
    print 'team order'
    team_1_id = page.findAll('div', {'class': 'logo'})[0].find('a').get('href').split('team/_/id/')[1].split('/')[0]
    team_1_name = page.findAll('div', {'class': 'logo'})[0].find('a').get('href').split('{}/'.format(team_1_id))[1].replace('-', ' ').title()

    team_2_id = page.findAll('div', {'class': 'logo'})[1].find('a').get('href').split('team/_/id/')[1].split('/')[0]
    team_2_name = page.findAll('div', {'class': 'logo'})[1].find('a').get('href').split('{}/'.format(team_2_id))[1].replace('-', ' ').title()

    ## Find Tables
    table = page.findAll('table')
    cols = [x.text.lower() for x in table[1].find('thead').find('tr').findAll('th')]

    ## Split teams
    print 'split tables'
    team_1_table = table[1]
    t1_b1 = team_1_table.findAll('tbody')[0]
    t1_b2 = team_1_table.findAll('tbody')[1]

    team_2_table = table[2]
    t2_b1 = team_2_table.findAll('tbody')[0]
    t2_b2 = team_2_table.findAll('tbody')[1]


    ############################################
    ############################################
    ########### Collect team 1 stats ###########
    ############################################
    ############################################
    print 'collect team 1 stats'
    stats_array = []
    p_id_array = []
    for tr in t1_b1:
        try:
            p_id = tr.find('td').find('a').get('href').split('player/_/id/')[1]
            stats_array.append([x.text for x in tr.findAll('td')])
            p_id_array.append(p_id)
        except:
            pass ## no player id no player

    for tr in t1_b2:
        try:
            p_id = tr.find('td').find('a').get('href').split('player/_/id/')[1]
            stats_array.append([x.text for x in tr.findAll('td')])
            p_id_array.append(p_id)
        except:
            pass ## no player id no player

    ## Add all stats to DF
    individual_stats = pd.DataFrame(stats_array, columns=cols)
    individual_stats['player_id'] = p_id_array
    individual_stats['team_id'] = team_1_id
    individual_stats['team_name'] = team_1_name
    individual_stats['game_id'] = game_id

    ## Fix stats
    individual_stats['ft_make'] = individual_stats['ft'].apply(lambda row: int(row.split('-')[0]))
    individual_stats['ft_attempt'] = individual_stats['ft'].apply(lambda row: int(row.split('-')[1]))

    individual_stats['3pt_make'] = individual_stats['3pt'].apply(lambda row: int(row.split('-')[0]))
    individual_stats['3pt_attempt'] = individual_stats['3pt'].apply(lambda row: int(row.split('-')[1]))

    individual_stats['2pt_make'] = individual_stats['fg'].apply(lambda row: int(row.split('-')[0]))
    individual_stats['2pt_make'] = individual_stats['2pt_make'] - individual_stats['3pt_make']

    individual_stats['2pt_attempt'] = individual_stats['fg'].apply(lambda row: int(row.split('-')[1]))
    individual_stats['2pt_attempt'] =individual_stats['2pt_attempt'] - individual_stats['3pt_attempt']

    ## clean cols
    individual_stats = individual_stats.rename(columns={'starters': 'player_name', 'to': 'take_away'})
    individual_stats.columns = [x.replace('2', 'two_').replace('3', 'three_') for x in individual_stats.columns]

    ## put in db
    print 'save team 1 stats'
    try:
        individual_stats.to_sql('ncaa_player_info',
                                create_engine(os.environ['DATABASE_URL']),
                                index=False, if_exists='append')
    except IntegrityError as e:
        pass ## already exists


    ############################################
    ############################################
    ########### Collect team 2 stats ###########
    ############################################
    ############################################
    print 'collect team 2 stats'
    stats_array = []
    p_id_array = []
    for tr in t2_b1:
        try:
            p_id = tr.find('td').find('a').get('href').split('player/_/id/')[1]
            stats_array.append([x.text for x in tr.findAll('td')])
            p_id_array.append(p_id)
        except:
            pass ## no player id no player

    for tr in t2_b2:
        try:
            p_id = tr.find('td').find('a').get('href').split('player/_/id/')[1]
            stats_array.append([x.text for x in tr.findAll('td')])
            p_id_array.append(p_id)
        except:
            pass ## no player id no player

    ## Add all stats to DF
    individual_stats = pd.DataFrame(stats_array, columns=cols)
    individual_stats['player_id'] = p_id_array
    individual_stats['team_id'] = team_2_id
    individual_stats['team_name'] = team_2_name
    individual_stats['game_id'] = game_id

    ## Fix stats
    individual_stats['ft_make'] = individual_stats['ft'].apply(lambda row: int(row.split('-')[0]))
    individual_stats['ft_attempt'] = individual_stats['ft'].apply(lambda row: int(row.split('-')[1]))

    individual_stats['3pt_make'] = individual_stats['3pt'].apply(lambda row: int(row.split('-')[0]))
    individual_stats['3pt_attempt'] = individual_stats['3pt'].apply(lambda row: int(row.split('-')[1]))

    individual_stats['2pt_make'] = individual_stats['fg'].apply(lambda row: int(row.split('-')[0]))
    individual_stats['2pt_make'] = individual_stats['2pt_make'] - individual_stats['3pt_make']

    individual_stats['2pt_attempt'] = individual_stats['fg'].apply(lambda row: int(row.split('-')[1]))
    individual_stats['2pt_attempt'] =individual_stats['2pt_attempt'] - individual_stats['3pt_attempt']

    ## clean cols
    individual_stats = individual_stats.rename(columns={'starters': 'player_name', 'to': 'take_away'})
    individual_stats.columns = [x.replace('2', 'two_').replace('3', 'three_') for x in individual_stats.columns]

    ## put in db
    print 'save team 2 stats'
    try:
        individual_stats.to_sql('ncaa_player_info',
                                create_engine(os.environ['DATABASE_URL']),
                                index=False, if_exists='append')
    except IntegrityError as e:
        pass ## already exists



df_ids = pd.read_sql_query("select distinct game_id from ncaa_game_info", open_connection())

more_to_collect = True

while more_to_collect == True:
    ## find ids that have been collected
    collected_ids = pd.read_sql_query("select distinct game_id from ncaa_player_info", open_connection())
    id_list = list(set(df_ids['game_id']) - set(collected_ids['game_id']))

    ## if no ids left collection is done
    if id_list == 0:
        more_to_collect = False

    else:
        ## pick random id
        game_id = np.random.choice(id_list)
        print game_id
        collect = True
        count = 0
        ## try to collect
        while collect == True:
            try:
                print
                collect_player(game_id=game_id)
                collect = False
            except:
                count += 1
                if count >= 10:
                    collect = False

import pandas as pd
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

teams_df = pd.read_sql_query("select * from ncaa_teams", open_connection())


for year in range(2018, 2003, -1):
    print year
    for i in teams_df.index:
        team_id = teams_df.loc[i, 'team_id']
        team_name = teams_df.loc[i, 'team_name']
        url = 'http://www.espn.com/mens-college-basketball/team/schedule/_/id/{}/season/{}'.format(team_id, year)
        game_stats = pd.DataFrame(columns=['team_id', 'team_name', 'date', 'home', 'opposing_id',
                                           'opposing_team', 'result', 'score', 'ot', 'game_link', 'game_id'])

        collected = False
        count = 0
        while collected == False:
            try:
                page = ip_spoofer.IpSpoofer.request_page(url)
                collected = True

                tbody = page.find('tbody', {'class':'Table2__tbody'})
                for tr in tbody.findAll('tr'):
                    index_ = len(game_stats)

                    try:
                        ## set vars
                        td_list = tr.findAll('td')
                        opposing_team_id = td_list[1].find('a').get('href').split('/id/')[1].split('/')[0]
                        opposing_team_name = td_list[1].find('a').get('href').split('{}/'.format(team_id))[1].replace('-', ' ').title()
                        link = td_list[2].find('a').get('href')

                        ## insert data
                        game_stats.loc[index_, 'date'] = pd.to_datetime('{}, {}'.format(td_list[0].text.split(', ')[1], year), infer_datetime_format=True)
                        game_stats.loc[index_, 'home'] = '@' not in td_list[1].find('span', {'class':'pr2'}).text

                        game_stats.loc[index_, 'opposing_id'] = opposing_team_id
                        game_stats.loc[index_, 'opposing_team'] = opposing_team_name
                        game_stats.loc[index_, 'result'] = td_list[2].find('span').text
                        game_stats.loc[index_, 'score'] = score.replace(' OT', '')
                        game_stats.loc[index_, 'ot'] = ' OT' in score
                        game_stats.loc[index_, 'game_link'] =link
                        game_stats.loc[index_, 'game_id'] = link.split('gameId=')[1]

                    except:
                        pass
            except:
                count += 1
                if count >= 10:
                    collected = True

        game_stats.loc[:, 'team_id'] = team_id
        game_stats.loc[:, 'team_name'] = team_name
        print "Number of records collected {}\n".format(len(game_stats))
        # try:
        game_stats.to_sql('ncaa_game_info',
                          create_engine(os.environ['DATABASE_URL']),
                          index=False, if_exists='append')
        # except IntegrityError as e:
        #     pass

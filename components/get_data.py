
import pandas as pd
from bs4 import BeautifulSoup
from urllib.request import urlopen

from components import columns as col

# class GetData():
#     def __enter__(self):
#         return self
    
#     def get_passing_data(self, start_year, end_year):
#         for_columns = [
#             'team',
#             'att',
#             'cmp',
#             'cmp_percent',
#             'yds_per_att',
#             'pass_td',
#             'td',
#             'int',
#             'rate',
#             'first',
#             'first_percent',
#             'twenty_plus',
#             'forty_plus',
#             'lng',
#             'sck',
#             'sck_y'
#         ]

#         final_columns = [
#             'team',
#             'att',
#             'cmp',
#             'cmp_percent',
#             'yds_per_att',
#             'pass_td',
#             'td',
#             'int',
#             'rate',
#             'first',
#             'first_percent',
#             'twenty_plus',
#             'forty_plus',
#             'lng',
#             'sck',
#             'sck_y',
#             'year'
#         ]

#         passing_data = pd.DataFrame(columns=final_columns)
#         passing_data

#         for year in range(start_year, end_year):
#             response = urlopen('https://www.nfl.com/stats/team-stats/offense/passing/' + str(year) + '/reg/all')
#             html = response.read()
#             soup = BeautifulSoup(html, 'html.parser')

#             table_rows = soup.find_all('tr')

#             l = []
#             for tr in table_rows:
#                 td = tr.find_all('td')
#                 row = [tr.text for tr in td]
#                 l.append(row)
#             passing_data_year = pd.DataFrame(l, columns=for_columns).drop(index=0)
#             passing_data_year['year'] = year

#             passing_data = pd.concat([passing_data, passing_data_year])

#         passing_data.reset_index(drop=True, inplace=True)
#         passing_data.to_parquet('../data/passing_data.parquet', engine='pyarrow')

#         return passing_data
    
    # def __exit__(self, *args, **kwargs):
        # return self


class ScrapData:
    def __init__(self):
        self.url = 'https://www.nfl.com/stats/team-stats/'

        self.stat_type = ['offense', 'defense', 'special-teams']
        self.stat_specific_offense = ['passing',
                                 'rushing',
                                 'receiving',
                                 'scoring',
                                 'downs']
        self.stat_specific_defense = ['passing',
                                 'rushing',
                                 'receiving',
                                 'scoring',
                                 'tackles',
                                 'downs',
                                 'fumbles',
                                 'interceptions']
        self.stat_specific_spec_teams = ['field-goals',
                                    'scoring',
                                    'kickoffs',
                                    'kickoff-returns',
                                    'punting',
                                    'punt-returns']
        
    def get_data(self, start_year, end_year):
        for year in range(start_year, end_year):
            for stat in self.stat_type:
                url = 'https://www.nfl.com/stats/team-stats/' + stat + '/'

                if stat == 'offense':
                    for stat_specific in self.stat_specific_offense:
                        columns = stat + '_' + stat_specific

                        final_data = pd.DataFrame()
                        final_data.columns = col.vars(columns)
                        final_data['year'] = ''

                        response = urlopen(url + stat_specific + '/' + str(year) + '/reg/all')

                        html = response.read()
                        soup = BeautifulSoup(html, 'html.parser')

                        table_rows = soup.find_all('tr')

                        l = []
                        for tr in table_rows:
                            td = tr.find_all('td')
                            row = [tr.text for tr in td]
                            l.append(row)
                        actual_data = pd.DataFrame(l, columns=col.vars(columns)).drop(index=0)
                        actual_data['year'] = year

                        data = pd.concat([final_data, actual_data])

                        data.to_parquet('../data/{stat}_{stat_specific}.parquet')

                elif stat == 'defense':
                    for stat_specific in self.stat_specific_defense:
                        columns = stat + '_' + stat_specific

                        final_data = pd.DataFrame()
                        final_data.columns = col.vars(columns)
                        final_data['year'] = ''

                        response = urlopen(url + stat_specific + '/' + str(year) + '/reg/all')

                        html = response.read()
                        soup = BeautifulSoup(html, 'html.parser')

                        table_rows = soup.find_all('tr')

                        l = []
                        for tr in table_rows:
                            td = tr.find_all('td')
                            row = [tr.text for tr in td]
                            l.append(row)
                        actual_data = pd.DataFrame(l, columns=col.vars(columns)).drop(index=0)
                        actual_data['year'] = year

                        data = pd.concat([final_data, actual_data])

                        data.to_parquet('../data/{stat}_{stat_specific}.parquet')

                elif stat == 'special-teams':
                    for stat_specific in self.stat_specific_spec_teams:
                        columns = stat + '_' + stat_specific

                        final_data = pd.DataFrame()
                        final_data.columns = col.vars(columns)
                        final_data['year'] = ''

                        response = urlopen(url + stat_specific + '/' + str(year) + '/reg/all')

                        html = response.read()
                        soup = BeautifulSoup(html, 'html.parser')

                        table_rows = soup.find_all('tr')

                        l = []
                        for tr in table_rows:
                            td = tr.find_all('td')
                            row = [tr.text for tr in td]
                            l.append(row)
                        actual_data = pd.DataFrame(l, columns=col.vars(columns)).drop(index=0)
                        actual_data['year'] = year

                        data = pd.concat([final_data, actual_data])

                        data.to_parquet('../data/{stat}_{stat_specific}.parquet')

                else:
                    raise Exception
                
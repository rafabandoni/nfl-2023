
import pandas as pd
from bs4 import BeautifulSoup
from urllib.request import urlopen

from components import columns as col

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
        for stat in self.stat_type:
            url = 'https://www.nfl.com/stats/team-stats/' + stat + '/'

            if stat == 'offense':
                for stat_specific in self.stat_specific_offense:
                    columns = 'col.' + stat + '_' + stat_specific

                    final_data = pd.DataFrame(columns=eval(columns))
                    final_data['year'] = ''

                    for year in range(start_year, end_year):
                        response = urlopen(url + stat_specific + '/' + str(year) + '/reg/all')

                        html = response.read()
                        soup = BeautifulSoup(html, 'html.parser')

                        table_rows = soup.find_all('tr')

                        l = []
                        for tr in table_rows:
                            td = tr.find_all('td')
                            row = [tr.text for tr in td]
                            l.append(row)
                        actual_data = pd.DataFrame(l, columns=eval(columns)).drop(index=0)
                        actual_data['year'] = year

                        data = pd.concat([final_data, actual_data])

                    data.to_parquet(f'../data/{stat}_{stat_specific}.parquet')

            elif stat == 'defense':
                for stat_specific in self.stat_specific_defense:
                    columns = 'col.' + stat + '_' + stat_specific

                    final_data = pd.DataFrame(columns=eval(columns))
                    final_data['year'] = ''

                    for year in range(start_year, end_year):
                        response = urlopen(url + stat_specific + '/' + str(year) + '/reg/all')

                        html = response.read()
                        soup = BeautifulSoup(html, 'html.parser')

                        table_rows = soup.find_all('tr')

                        l = []
                        for tr in table_rows:
                            td = tr.find_all('td')
                            row = [tr.text for tr in td]
                            l.append(row)
                        actual_data = pd.DataFrame(l, columns=eval(columns)).drop(index=0)
                        actual_data['year'] = year

                        data = pd.concat([final_data, actual_data])

                    data.to_parquet(f'../data/{stat}_{stat_specific}.parquet')

            elif stat == 'special-teams':
                for stat_specific in self.stat_specific_spec_teams:
                    columns = 'col.' + stat + '_' + stat_specific

                    final_data = pd.DataFrame(columns=eval(columns))
                    final_data['year'] = ''

                    for year in range(start_year, end_year):
                        response = urlopen(url + stat_specific + '/' + str(year) + '/reg/all')

                        html = response.read()
                        soup = BeautifulSoup(html, 'html.parser')

                        table_rows = soup.find_all('tr')

                        l = []
                        for tr in table_rows:
                            td = tr.find_all('td')
                            row = [tr.text for tr in td]
                            l.append(row)
                        actual_data = pd.DataFrame(l, columns=eval(columns)).drop(index=0)
                        actual_data['year'] = year

                        data = pd.concat([final_data, actual_data])

                    data.to_parquet(f'../data/{stat}_{stat_specific}.parquet')

                else:
                    raise Exception
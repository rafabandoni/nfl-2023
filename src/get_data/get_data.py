import pandas as pd
from bs4 import BeautifulSoup
from urllib.request import urlopen

from src.get_data import columns as col

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
                                      'scoring',
                                      'downs',
                                      'fumbles',
                                      'interceptions']
        self.stat_specific_spec_teams = ['field-goals',
                                         'scoring',
                                         'kickoffs',
                                         'kickoff-returns',
                                         'punting',
                                         'punt-returns']
        
    def __enter__(self):
        return self
        
    def get_data(self, start_year, end_year):
        try:
            for stat in self.stat_type:
                print(f'Downloading from {stat}')
                url = 'https://www.nfl.com/stats/team-stats/' + stat + '/'

                if stat == 'offense':
                    for stat_specific in self.stat_specific_offense:
                        print(f'Downloading {stat_specific}')
                        columns = 'col.' + stat.replace('-','_') + '_' + stat_specific.replace('-','_')

                        final_data = pd.DataFrame(columns=eval(columns))
                        final_data['year'] = ''

                        for year in range(start_year, end_year):
                            print(f'Downloading {year}')
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

                        data.to_parquet(f'../data/{stat}/{stat}_{stat_specific}.parquet')

                elif stat == 'defense':
                    for stat_specific in self.stat_specific_defense:
                        print(f'Downloading {stat_specific}')
                        columns = 'col.' + stat.replace('-','_') + '_' + stat_specific.replace('-','_')

                        final_data = pd.DataFrame(columns=eval(columns))
                        final_data['year'] = ''

                        for year in range(start_year, end_year):
                            print(f'Downloading {year}')
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

                        data.to_parquet(f'../data/{stat}/{stat}_{stat_specific}.parquet')

                elif stat == 'special-teams':
                    for stat_specific in self.stat_specific_spec_teams:
                        print(f'Downloading {stat_specific}')
                        columns = 'col.' + stat.replace('-','_') + '_' + stat_specific.replace('-','_')

                        final_data = pd.DataFrame(columns=eval(columns))
                        final_data['year'] = ''

                        for year in range(start_year, end_year):
                            print(f'Downloading {year}')
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

                        data.to_parquet(f'../data/{stat}/{stat}_{stat_specific}.parquet')
        
        except Exception as e:
            print(e)

        else:
            print('Download data finished.')

        finally:
            print('Get data finished.')

    def __exit__(self, *args, **kwargs):
        return self
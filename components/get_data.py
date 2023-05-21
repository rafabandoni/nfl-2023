
import pandas as pd
from bs4 import BeautifulSoup
from urllib.request import urlopen

class GetData():
    def __enter__(self):
        return self
    
    def get_passing_data(self, start_year, end_year):
        for_columns = [
            'team',
            'att',
            'cmp',
            'cmp_percent',
            'yds_per_att',
            'pass_td',
            'td',
            'int',
            'rate',
            'first',
            'first_percent',
            'twenty_plus',
            'forty_plus',
            'lng',
            'sck',
            'sck_y'
        ]

        final_columns = [
            'team',
            'att',
            'cmp',
            'cmp_percent',
            'yds_per_att',
            'pass_td',
            'td',
            'int',
            'rate',
            'first',
            'first_percent',
            'twenty_plus',
            'forty_plus',
            'lng',
            'sck',
            'sck_y',
            'year'
        ]

        passing_data = pd.DataFrame(columns=final_columns)
        passing_data

        for year in range(start_year, end_year):
            response = urlopen('https://www.nfl.com/stats/team-stats/offense/passing/' + str(year) + '/reg/all')
            html = response.read()
            soup = BeautifulSoup(html, 'html.parser')

            table_rows = soup.find_all('tr')

            l = []
            for tr in table_rows:
                td = tr.find_all('td')
                row = [tr.text for tr in td]
                l.append(row)
            passing_data_year = pd.DataFrame(l, columns=for_columns).drop(index=0)
            passing_data_year['year'] = year

            passing_data = pd.concat([passing_data, passing_data_year])

        passing_data.reset_index(drop=True, inplace=True)
        passing_data.to_parquet('../data/passing_data.parquet', engine='pyarrow')

        return passing_data
    
    def __exit__(self, *args, **kwargs):
        return self

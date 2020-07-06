import os
import datetime

import csv
import requests
import json
import gspread
import luigi

import pandas as pd
import numpy as np

from luigi.format import UTF8

with open('token.json', 'r') as tkn:
    auth = json.loads(tkn.read())

TOKEN = auth['token']
HEADERS = {'Authorization': f'OAuth {TOKEN}'}
URL = 'https://api.webmaster.yandex.net/v4/user/{user}/hosts/{host}/'.format(**auth)
NOW = datetime.datetime.now()
DATE_FROM = NOW - datetime.timedelta(days=365)

def fetch_data(url, params=None):
    return requests.get(url, params=params, headers=HEADERS).json()

def str_to_bool(i):
      return i.lower() in ("yes", "true", "t", "1")

class ExportToSheetsTask(luigi.Task):

    @property
    def gc(self):
        return gspread.service_account()

    def csv_to_sheet(self, csv_content, sheet_id):
        sheet = self.gc.open_by_key(sheet_id).get_worksheet(0)
        sht_values = sheet.get_all_values()
        n_row, n_col = np.asarray(sht_values).shape
        cell_list = sheet.range(1, 1, n_row, n_col)

        data = [y for x in csv_content for y in x]
        #data = np.asarray(list(csv_content)).flatten()
        data = data + [''] * (max(len(data), n_col * n_row) - len(data))

        for i in range(len(cell_list)):
            cell_list[i].value = data[i]

        sheet.update_cells(cell_list)


    def requires(self):
        return GetDataTask() # if str_to_bool(self.exist) else None

    def run(self):
        sheets = {f"{x['name']}.csv": x['id']
                        for x in self.gc.list_spreadsheet_files()}

        for input in self.input():
            with input.open('r') as csv_file:
                #content = csv_file.read()
                file_id = sheets[csv_file.name]
                csv_content = csv.reader(csv_file)
                self.csv_to_sheet(csv_content, file_id)
            #input.remove()

class GetDataTask(luigi.Task):

    def output(self):
        files = [f'{x}.csv' for x in [
                   'X', 'sqi_history', 'queries', 'links',
                   'links_history', 'index_history', 'codes'
            ]
        ]
        return [luigi.LocalTarget(filename.format(),
                                  format=UTF8) for filename in files]

    def get_data(self):
        base_values = fetch_data(f'{URL}summary')
        base_values_df = pd.DataFrame({
                    'date': NOW.date(),
                    'x': base_values['sqi'],
                    'excluded': base_values['excluded_pages_count'],
                    'searchable': base_values['searchable_pages_count'],
                    'problems': base_values['site_problems']['CRITICAL']
              }, index=pd.RangeIndex(1)
        )
        sqi_history =  fetch_data(f'{URL}sqi-history')
        sqi_history_df = pd.DataFrame(sqi_history['points'])

        in_search = fetch_data(f'{URL}search-urls/in-search/history',
                                 params={
                                     'date_from': DATE_FROM.date(),
                                     'date_to': NOW.date()
        })
        in_search_df = pd.DataFrame(in_search['history'])

        indexing = fetch_data(f'{URL}indexing/history',
                                 params={
                                     'date_from': DATE_FROM.date(),
                                     'date_to': NOW.date()
        })
        index_data_frames = []

        for key in indexing['indicators'].keys():
            index_data_frames.append(pd.DataFrame(
                index=[x['date'] for x in indexing['indicators'][key]],
                data=[x['value'] for x in indexing['indicators'][key]],
                columns=['value']
        ))
        index_df = pd.concat(index_data_frames,
                             keys=indexing['indicators'].keys()) \
                                             .reset_index() \
                                             .rename(columns={
                                                 'level_0': 'code',
                                                 'level_1': 'date'
                                             })
        index_df['date'] = pd.to_datetime(index_df['date']).dt.date
        index_df = index_df.groupby(['code', 'date'],
                                    as_index=False).sum()

        queries = fetch_data(f'{URL}search-queries/popular/',
                        params={
                                'order_by': 'TOTAL_CLICKS',
                                'query_indicator':
                                                  ('TOTAL_CLICKS',
                                                   'TOTAL_SHOWS',
                                                   'AVG_CLICK_POSITION')
        })
        queries_df = pd.DataFrame([(
            x['query_id'],
            x['query_text'],
            x['indicators']['TOTAL_CLICKS'],
            x['indicators']['TOTAL_SHOWS'],
            x['indicators']['AVG_CLICK_POSITION'])
                             for x in queries['queries']
            ],
            columns=['id', 'keyword', 'clicks', 'shows', 'avg_position']
        )
        queries_df['date'] = [NOW.date()] * len(queries_df)

        links_history = fetch_data(f'{URL}links/external/history',
                            params={'indicator': 'LINKS_TOTAL_COUNT'}
        )
        links_history_df = pd.DataFrame(
            links_history['indicators']['LINKS_TOTAL_COUNT']
        )
        links_samples = fetch_data(f'{URL}links/external/samples',
                                                 params={'limit': 100})
        links_samples_df = pd.DataFrame([(
            x['source_url'],
            x['destination_url'],
            x['discovery_date'],
            x['source_last_access_date'])
                    for x in links_samples['links']
             ],
              columns=links_samples['links'][0].keys()
        )
        links_samples_df['date'] = [NOW.date()] * len(links_samples_df)

        return [base_values_df,
                sqi_history_df,
                queries_df,
                links_samples_df,
                links_history_df,
                in_search_df,
                index_df
        ]

    def run(self):
        data_frames = self.get_data()

        for filename, df in zip(self.output(), data_frames):
            with filename.open('w') as csv_file:
                df.to_csv(csv_file.name, index=False)

if __name__ == '__main__':
    luigi.run()

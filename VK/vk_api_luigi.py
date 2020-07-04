import os
import datetime

import csv
import requests
import json
import gspread
import luigi

import pandas as pd
import numpy as np

from pathlib import Path
from luigi.format import UTF8
from drive import service

with open('vk_api_auth.json', 'r') as tkn:
    auth = json.loads(tkn.read())

TOKEN = auth['token']
V = auth['version']
FILENAMES = ['users', 'posts', 'likes', 'comments']
NOW = datetime.datetime.now()
HOW_OFTEN = 7

def vk(method, params=""):
    if params:
        params = "?%s" % '&'.join([f"{key}={value}" for key, value in params.items()])
    url = "https://api.vk.com/method/%s%s&access_token=%s&v=%s" % (method, params, TOKEN, V)
    data = requests.get(url).json()
    return data

class AggregateGroupDataTask(luigi.Task):
    groups = luigi.ListParameter()

    def requires(self):
        return [GetGroupDataTask(group_id) for group_id in self.groups]

    def output(self):
        #out_files = [f'{fname}.csv' for fname in FILENAMES]
        return [luigi.LocalTarget(f'{fname}.csv'.format(),
                       format=UTF8) for fname in FILENAMES]

    def run(self):
        data_frames = [] #dict.fromkeys(FILENAMES, list())

        #for key in data_frames.keys():
            #file_list = [input for input in self.input() if key in input]
        for section, out_file in zip(zip(*self.input()), self.output()):
           #df = pd.DataFrame() 
           #for fname in section:
           df = pd.concat([pd.read_csv(fname.open('r'))
                                   for fname in section], ignore_index=True)
                    #pd.concat(pd.read_csv(csv_file))
                    #data_frames[key].append(pd.read_csv(csv_file.name))
        #for dfs, out_file in zip(data_frames, self.output()):
        #    df = pd.concat(data_frames[dfs])
           #df['last_seen'] = df['last_seen'].astype(str)
           df = df.convert_dtypes()
           with out_file.open('w') as csv_file:
                df.to_csv(csv_file.name, index=False)

def fetch_data(method, params, offset=100):
        result = []
        offset = offset
        count = vk(method=method, params=params)['response']['count']

        for _ in range(count // offset + 1):
            response = vk(method=method, params=params)['response']
            result += response['items']
            params['offset'] += offset

        return result


class GetGroupDataTask(luigi.Task):
    group_id = luigi.Parameter()

    def get_users(self, x):
        return {'id': x.get('id'),
                'first_name': x.get('first_name'),
                'last_name': x.get('last_name'),
                'sex': x.get('sex'),
                'bdate': x.get('bdate', ''),
                'city_id': x.get('city', {}).get('id', 999),
                'city': x.get('city', {}).get('title', ''),
                'country_id': x.get('country', {}).get('id', 999),
                'country': x.get('country', {}).get('title', ''),
                'photo': x.get('photo_50'),
                'last_seen': x.get('last_seen', {}).get('time', ''),
                'occupation': x.get('occupation', {'name': ''}).get('name', ''),
                'religion': x.get('personal', {}).get('religion', ''),
                'relation': x.get('personal', {}).get('relation', ''),
                'political': x.get('personal', {}).get('political', ''),
                'universitie': x.get('universities', [{'name': ''}])[0]['name']
                                               if x.get('universities') else '',
                'mobile_phone': x.get('mobile_phone', ''),
                'deactivated': x.get('deactivated', '')
    }

    def get_post(self, x):
        return {'post_id': x.get('id'),
		'author': x.get('from_id'),
		'owner': x.get('owner_id'),
		'date': x.get('date', ''),
		'post_type': x.get('post_type', ''),
		'is_repost': 1 if x.get('copy_history') else 0,
		'comments': x.get('comments', {}).get('count', 0),
		'likes': x.get('likes', {}).get('count', 0),
		'reposts': x.get('reposts', {}).get('count', 0),
		'views': x.get('views', {}).get('count', 0),
		'attachments': ', '.join([a['type'] for a
                                         in x.get('attachments')])
                                         if x.get('attachments') else ''
    }

    def get_post_metrics(self, method, parameter, post_ids, params):
        details = {}

        for pid in post_ids:
            params[parameter] = pid
            details[pid] = fetch_data(method, params)
            params['offset'] = 0

        return details

    def extract_users(self, group_id):
        fields = ['sex',
                  'bdate',
                  'career',
                  'country',
                  'city',
                  'contacts',
                  'personal',
                  'education',
                  'universities',
                  'last_seen',
                  'occupation',
                  'relation',
                  'photo_50'
        ]
        count = 1000
        params = dict(group_id=group_id, count=count, extended=1,
                                        fields=','.join(fields), offset=0)
        users_list = fetch_data("groups.getMembers", params=params, offset=count)
        users = pd.DataFrame([self.get_users(x) for x in users_list])
        users['group_id'] = [group_id] * users.shape[0]
        users['date'] = [NOW.date()] * users.shape[0]
        users['last_seen'] = pd.to_datetime(users.last_seen, unit='s')
        return users

    def extract_posts(self, group_id):
        params = dict(owner_id=-group_id, count=100, extended=1, offset=0)
        posts_list = fetch_data("wall.get", params)
        posts = pd.DataFrame([self.get_post(x) for x in posts_list])
        posts['owner'] = posts.owner.map(np.abs)
        posts['date'] = posts['date'].apply(lambda x: pd.to_datetime(x, unit='s'))

        return posts

    def extract_likes(self, group_id, posts_list):
        params = dict(owner_id=-group_id, count=1000, type="post",
                                 skip_own=1, item_id=0, extended=1, offset=0)
        likes_list = self.get_post_metrics("likes.getList", "item_id", posts_list, params)
        likes = pd.concat([pd.DataFrame(likes_list[key]) for key in likes_list.keys()],
                                          keys=likes_list.keys()) \
                                          .reset_index() \
                                          .rename(columns={'level_0': 'post_id'})[
                                              ['post_id', 'id', 'first_name', 'last_name']
                          ]
        likes['group_id'] = [group_id] * len(likes)
        return likes

    def extract_comments(self, group_id, posts_list):
        params = dict(owner_id=-group_id, count=1000, post_id=0, extended=1, offset=0)
        comments_list = self.get_post_metrics("wall.getComments", "post_id", posts_list, params)
        comments = pd.concat([pd.DataFrame(comments_list[key]) for
                                                         key in comments_list.keys()])
        comments['owner_id'] = comments.owner_id.map(np.abs)
        #comments['date']
        comments = comments[['id', 'date', 'from_id', 'post_id', 'owner_id', 'text']]
        return comments

    def output(self):
        out_files = [f'{self.group_id}_{fname}.csv' for fname in FILENAMES]
        return [luigi.LocalTarget(fname.format(),
                                       format=UTF8) for fname in out_files]

    def run(self):
        users = self.extract_users(self.group_id)
        users_list = users.id.to_list()
        posts = self.extract_posts(self.group_id)
        posts_with_comments = posts.query('comments > 0').post_id.to_list()
        posts_with_likes = posts.query('likes > 0').post_id.to_list()
        comments = self.extract_comments(self.group_id, posts_with_comments)
        likes = self.extract_likes(self.group_id, posts_with_likes)
        #likes['is_member'] = likes['id'].apply(lambda x: x in users_list)
        #comments['is_member'] = comments['id'].apply(lambda x: x in users_list)
        data_frames = [users, posts, likes, comments]

        for fname, df in zip(self.output(), data_frames):
            with fname.open('w') as csv_file:
                df.to_csv(csv_file.name, index=False)


class ExportToSheetsTask(luigi.Task):
    groups = luigi.ListParameter()

    @property
    def gc(self):
        return gspread.service_account()

    def csv_to_sheet(self, csv_content, sheet_id, add_mode=None):
        #gc = self.gc
        #gc.login()
        sheet = self.gc.open_by_key(sheet_id).get_worksheet(0)
        sht_values = sheet.get_all_values()
        n_row, n_col = np.asarray(sht_values).shape
        #csv_content = csv.reader(csv_file)
        #data = [y for x in csv_content for y in x]
        data = list(csv_content)
        if add_mode:
            cell_list = sheet.range(n_row, 1,
                                    n_row + len(data[1:]) - 1, n_col)
            data = [y for x in data[1:] for y in x] # remove header
        #data = np.asarray(list(csv_content)).flatten()
        else:
            cell_list = sheet.range(1, 1, n_row, n_col)
            data = [y for x in data for y in x]
            data = data + [''] * (max(len(data), len(cell_list)) - len(data))

        for i in range(len(cell_list)):
            cell_list[i].value = data[i]

        sheet.update_cells(cell_list)

    def requires(self):
        return AggregateGroupDataTask(self.groups)

    def output(self):
        return luigi.LocalTarget('process.log')

    def get_last_modified(self, file_id):
        """Based on Google Drive API v3"""

        timestamp = service().files().get(fileId=file_id, fields='modifiedTime') \
                                     .execute().get('modifiedTime')
        return pd.to_datetime(timestamp).tz_convert(None) # no timezone

    def get_output_files(self):
        inputs = [Path(x.open().name).stem for x in self.input()] # inputs with no extension
        return {f"{x['name']}.csv": (x['id'], self.get_last_modified(x['id']))
                 for x in self.gc.list_spreadsheet_files() if x['name'] in inputs}

    def complete(self):
        outputs = self.get_output_files()
        return all(map(lambda out: (NOW - out[1]).days <= HOW_OFTEN, outputs.values()))

    def run(self):
        #gc = self.gc
        #sheets = {f"{x['name']}.csv": x['id']
        #                for x in self.gc.list_spreadsheet_files()}
        sheets = self.get_output_files()

        mode = False
        with self.output().open('w') as outfile:

            for fname, pred in sheets.items():
                file_id, last_modified = pred
                #if (NOW - last_modified).days >= HOW_OFTEN:
                with open(fname) as csv_file:
                    csv_content = csv.reader(csv_file)
                    mode = True if csv_file.name == 'users.csv' else mode
                    self.gc.login()
                    try:
                        self.csv_to_sheet(csv_content, file_id, add_mode=mode)
                    except Exception as e:
                        print(e)
                #else:
                    #print('since the last report passed: %d' %(NOW - last_modified).days)
                    #continue
                print(f'{file_id}\t{fname}\t{last_modified}', file=outfile)

#            with input.open('r') as csv_file:
#                if csv_file.name != 'users.csv':
#                    #content = csv_file.read()
#                    file_id = sheets[csv_file.name][0]
#                    csv_content = csv.reader(csv_file)
#                    #mode = True if csv_file.name == 'users.csv' else mode
#                    self.gc.login()
#                    try:
#                        self.csv_to_sheet(csv_content, file_id, mode)
#                    except Exception as e:
#                        print(e)
                #input.remove()

if __name__ == '__main__':
    """
    python vk_api_luigi.py ExportToSheetsTask --groups ['25823244','7193'] --local-scheduler

    """
    luigi.run()


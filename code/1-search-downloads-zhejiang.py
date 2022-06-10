# coding=utf-8

# webscrapper for Zhejiang procurement documents
# downloading search results
# June 7, 2022

import requests
from requests.adapters import HTTPAdapter
from concurrent.futures import ThreadPoolExecutor

import re
from bs4 import BeautifulSoup
import time
import random
import csv
import pandas as pd
import os
from threading import Timer
import json
from datetime import datetime
from collections import OrderedDict

# (1) first construct API search urls:
# search database api:
# 'https://zfcgmanager.czt.zj.gov.cn/cms/api/cors/remote/results?url=notice&isGov=true'
# 'https://zfcgmanager.czt.zj.gov.cn/cms/api/cors/remote/results?pageNo=101&pageSize=15&type=0&isExact=0&url=notice'
# res = requests.get('https://zfcgmanager.czt.zj.gov.cn/cms/api/cors/remote/results?url=notice')
# results = json.loads(res.text)
# articles = results['articles']

# FROM: 2015-01-01 / NEW SEARCH: 2002-01-01
# TO: 2021-12-31
# "realCount":2089238,

# &pubDate=2015-07-17&endDate=2015-07-17
# pageSize=100&pageNo=[0:100]

# need to SEARCH BY DAY

# parameters set up
# now construct DAYs
dates = pd.date_range(start="2002-01-01", end="2021-12-31")
dates = [str(date)[0:10] for date in dates]
url_prefix = 'https://zfcgmanager.czt.zj.gov.cn/cms/api/cors/remote/results?&url=notice&pageSize=100&pageNo=1'
search_urls = []
for date in dates:
    url = url_prefix + '&pubDate=' + date + '&endDate=' + date
    search_urls.append(url)
column_names = ['date', 'id', 'mainBidMenuName', 'title', 'projectCode', 'projectName', 'pubDate',
                'districtName', 'type', 'typeName', 'keywords', 'remark', 'url', 'invalid', 'invalidDate']


def get_one_day(search_url):
    user_agent = 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 ' \
                 '(KHTML, like Gecko) Chrome/102.0.5005.61 Mobile Safari/537.36'
    headers = {"User-Agent": user_agent, }

    this_day = search_url[-10:]
    print('now getting {0}'.format(this_day))
    this_day_docs = []
    first_page = None
    while True:
        try:
            response = requests.get(search_url, headers=headers)
            if response.status_code != 200:
                print('Error getting {0} first page. Re-try. {1}'.format(this_day, response.status_code))
                continue
            else:
                response.encoding = 'utf-8'
                first_page = json.loads(response.text)
                if first_page['successFlag']:
                    print('{0} first page success'.format(this_day))
                    break
                else:
                    print(first_page)
                    continue
        except requests.exceptions.RequestException as e:
            print('Error getting {0} first page. Re-try. {1}'.format(this_day, e))
            time.sleep(5)
            continue

    this_day_count = first_page['realCount']

    if this_day_count > 10000:
        print('abnormal found on {0}, doc counts is {1}'.format(this_day, this_day_count))

    total_pn = int(this_day_count/100) + 1
    docs_list = first_page['articles']
    for doc in docs_list:
        doc = OrderedDict(doc)
        doc.update({'date': this_day})
        doc.move_to_end('date', last=False)
        if list(doc.keys()) == column_names:
            this_day_docs.append(list(doc.values()))

    if total_pn > 1:
        for j in range(1, total_pn):
            pn = j + 1
            current_page_url = search_url.replace('&pageNo=1', '&pageNo={0}'.format(pn), 1)
            current_page = None
            while True:
                try:
                    response = requests.get(current_page_url, headers=headers)
                    if response.status_code != 200:
                        print('Error getting {0} page #{1}/{2}. '
                              'Re-try. {3}'.format(this_day, pn, total_pn, response.status_code))
                        continue
                    else:
                        response.encoding = 'utf-8'
                        current_page = json.loads(response.text)
                        if current_page['successFlag'] and current_page['pageNo'] == pn:
                            if pn < total_pn and len(current_page['articles']) != 100:
                                print(current_page)
                                continue
                            else:
                                print('{0} page #{1}/{2} success'.format(this_day, pn, total_pn))
                                break
                        else:
                            print(current_page)
                            continue
                except requests.exceptions.RequestException as e:
                    print('Error getting {0} page #{1}/{2}. Re-try. {3}'.format(this_day, pn, total_pn, e))
                    time.sleep(5)
                    continue

            docs_list2 = current_page['articles']
            for doc in docs_list2:
                doc = OrderedDict(doc)
                doc.update({'date': this_day})
                doc.move_to_end('date', last=False)
                if list(doc.keys()) == column_names:
                    this_day_docs.append(list(doc.values()))

    if len(this_day_docs) == this_day_count:
        print('{0} with {1} docs scrapped and checked'.format(this_day, len(this_day_docs)))
        return this_day_docs
    else:
        print('Error: {0} with {1} docs failed passing checks'.format(this_day, len(this_day_docs)))


def write_data_to_file(batch_results):
    file_there = os.path.exists('/Users/jluo/PycharmProjects/Govt-AI-Contracts/procurements'
                                '/zhejiang_in_local/raw_data/%s.csv' % filename)

    print('start writing data ====> ')
    with open('/Users/jluo/PycharmProjects/Govt-AI-Contracts/procurements'
              '/zhejiang_in_local/raw_data/%s.csv' % filename, 'a', encoding='utf-8') as file:
        # creating a csv writer object
        csv_writer = csv.writer(file)

        if not file_there:
            # writing headers
            headers = ['download_id'] + column_names
            csv_writer.writerow(headers)

        # writing the data rows
        csv_writer.writerows([n] + row for n, row in enumerate(batch_results, 1))
        file.close()
    print('====> finished writing in %d rows of data' % len(batch_results))


def get_and_save_all_days(this_batch_urls):
    global finished_docs_count
    """
    Create a thread pool to download all days simultaneously
    """
    local_start = time.time()
    all_docs_list = []
    futures_list = []
    with ThreadPoolExecutor(max_workers=threads_num) as executor:
        for this_day_url in this_batch_urls:
            futures = executor.submit(get_one_day, this_day_url)
            futures_list.append(futures)

        for future in futures_list:
            this_day_docs = future.result()
            all_docs_list += this_day_docs

        if len(all_docs_list) > 0:
            finished_docs_count += len(all_docs_list)
            global_time_spent = time.time() - global_start
            local_time_spent = time.time() - local_start
            current_speed = len(all_docs_list) / local_time_spent
            left_docs_count = total_docs_count - finished_docs_count
            print("====> \n Total {0} procurement notices downloaded, time elapsed {1} minutes,"
                  " \n this batch speed is {2} notices per second, estimated time to finish: {3} minutes"
                  " \n====>".format(finished_docs_count, global_time_spent / 60,
                                    current_speed, left_docs_count / (current_speed * 60)))
            write_data_to_file(all_docs_list)


if __name__ == "__main__":
    filename = 'zhejiang-local-all-2002-2021'
    total_docs_count = 2089245
    threads_num = 100
    global_start = time.time()

    batch_size = 200
    total_days = len(search_urls)
    url_batches = [search_urls[i:i + batch_size] for i in range(0, total_days, batch_size)]

    finished_docs_count = 0
    for url_batch in url_batches:
        get_and_save_all_days(url_batch)

    df = pd.read_csv('procurements/zhejiang_in_local/raw_data/zhejiang-local-all-2002-2021.csv')
    df.insert(0, 'zhejiang_global_id', df.index+1)
    years = [date[0:4] for date in df.date]
    df.insert(2, 'year', years)
    df.to_csv('procurements/zhejiang_in_local/raw_data/zhejiang-local-all-2002-2021.csv',
              encoding='utf-8', index=False)

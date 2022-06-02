# -*- coding: utf-8 -*-

# webscrapper Govt AI Purchase contracts
# downloading body text results
# May 29, 2022

import os
# from concurrent import futures
from concurrent.futures import ThreadPoolExecutor
import pandas
import requests
from requests.adapters import HTTPAdapter

import re
import time
import random
import csv
from threading import Timer

# params
threads_num = 300


def request_one_text(id_url):
    id, url = id_url
    headers = {
        "User-Agent": 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.5005.61 Mobile Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7,zh-TW;q=0.6,ja;q=0.5',
        'Cache-Control': 'max-age=0', }

    errors = 0
    while True:
        # if errors == 0:
        #     # first pick up a proxy IP order imposed by page index
        #     proxy_index = (id - 1) % len(proxy_IPs)
        # else:
        #     proxy_index = random.randint(0, len(proxy_IPs) - 1)
        # proxy_this_session = proxy_IPs[proxy_index]
        proxy_this_session = []

        try:
            session = requests.Session()
            session.mount('http://', HTTPAdapter(max_retries=3))
            session.mount('https://', HTTPAdapter(max_retries=3))
            response = session.get(url, headers=headers,
                                   proxies=proxy_this_session, timeout=(5, 15)) #allow_redirects=False)
            response.encoding = 'utf-8'
            # print('{0}: {1}'.format(id, response.status_code))
            if response.status_code == 403:
                print('403 Forbidden. pause and re-try Doc #{0}.'.format(id))
                time.sleep(random.randint(3, 7))
                errors += 1
                continue
            elif response.status_code == 404:
                print('404 Not found. skip this Doc #{0}.'.format(id))
                return id, url, 'NA'
            elif response.status_code == 405:
                print('405 Not Allowed. pause and re-try Doc #{0}.'.format(id))
                time.sleep(random.randint(3, 7))
                errors += 1
                continue
            elif response.status_code == 200:
                if re.search('您的访问过于频繁', response.text) is not None:
                    print('requests too frequent. change proxy and re-try Doc #{0}.'.format(id))
                    time.sleep(random.randint(3, 7))
                    errors += 1
                    continue
                elif re.search('中华人民共和国财政部', response.text) is None:
                    print('empty page returned. change proxy and re-try Doc #{0}.'.format(id))
                    time.sleep(random.randint(3, 7))
                    errors += 1
                    continue
                else:
                    print('connection success: 200. Doc #{0}/{1} scrapped'.format(id, total_pn_this_year))
                    # time.sleep(random.randint(3, 7))
                    return id, url, response.text.encode('utf-8')
        except requests.exceptions.RequestException as e:
            print('Re-try doc #{0}/{1}. {2}: {3}'.format(id, total_pn_this_year, url, e))
            errors += 1
            if re.search('incorrect header check', str(e)) is not None and errors > 10:
                headers['Accept-Encoding'] = 'identity'
                print('Error -3 while decompressing: incorrect header check. skip this Doc #{0}.'.format(id))
                return id, url, 'Error -3 while decompressing: incorrect header check'


def save_params_speed_stats(this_stats):
    stats_record = temp_filename + '_stats'
    with open('procurements/nationwide/main-texts/%s.csv' % stats_record, 'a') as f:
        # creating a csv writer object
        csvwriter = csv.writer(f)
        # writing in speed records
        csvwriter.writerows(row for row in this_stats)
        f.close()


def write_data_to_file(results):
    # column names
    headers = ['this_year_id', 'bid_url', 'body_text']
    headers_there = os.path.exists('procurements/nationwide/main-texts/%s.csv' % temp_filename)

    print('start writing data ====> ')
    with open('procurements/nationwide/main-texts/%s.csv' % temp_filename, 'a') as file:
        # creating a csv writer object
        csv_writer = csv.writer(file)

        if headers_there is False:
            # writing headers
            csv_writer.writerow(headers)

        # writing the data rows
        csv_writer.writerows([row for row in results])
        file.close()
        print('====> finished writing in %d rows of data' % len(results))


def download_this_batch(ids_urls):
    """
    Create a thread pool and download specified urls
    """
    job_list = []
    body_text_list = []
    speed_record = []
    start = time.time()

    with ThreadPoolExecutor(max_workers=threads_num) as executor:
        for id_url in ids_urls:
            job = executor.submit(request_one_text, id_url)
            job_list.append(job)

        for job in job_list:
            body_text = job.result()
            body_text_list.append(body_text)

            if len(body_text_list) % 100 == 0:
                stop = time.time()
                time_spent = stop - start
                print("====> \n Now {0} documents downloaded in this batch, time elapsed {1} minutes,"
                      " \n speed is {2} documents per second, estimated time to finish the entire year: {3} minutes"
                      " \n====>".format(len(body_text_list), time_spent / 60, len(body_text_list) / time_spent,
                                        ((total_doc_num - len(body_text_list)) / (len(body_text_list) / time_spent)) / 60))
                speed_record.append([time_spent/60, len(body_text_list)/time_spent,
                                     threads_num])

        body_text_list.sort()
        write_data_to_file(body_text_list)
        save_params_speed_stats(speed_record)
    return body_text_list, speed_record


if __name__ == "__main__":
    global_time_start = time.time()

    years = ['2014', '2015']
    #'2017']
    #['2002', #'2003', '2004', '2005', '2006']
        #'2013', #'2014-2015', '2015-2016', ['2016', '2017', '2018-2019', '2019-2020', '2020-2021', '2021-2022'
    for year in years:
        filepath = 'procurements/nationwide/nationwide-all-' + year + '.csv'
        df = pandas.read_csv(filepath)
        total_pn_this_year = len(df)
        # if year == '2016':
        #     df.drop(index=df.index[:820000],
        #             axis=0,
        #             inplace=True)
        total_doc_num = len(df)
        print('now scrapping Year {0} for {1} documents'.format(year, total_doc_num))
        temp_filename = 'nationwide-all-' + year + '_main_texts'

        ids = list(df.this_year_id)
        urls = list(df.bid_url)
        batch_size = 10000
        id_batches = [ids[i:i+batch_size] for i in range(0, total_doc_num, batch_size)]
        url_batches = [urls[i:i+batch_size] for i in range(0, total_doc_num, batch_size)]

        for id_batch, url_batch in zip(id_batches, url_batches):
            docs_info = zip(id_batch, url_batch)
            body_texts, speed_stats = download_this_batch(docs_info)

        print('Year {0} with {1} docs written into /{2}.csv'.
              format(year, total_doc_num, temp_filename))

        # if to merge with the main file for the year
        # df2 = pandas.DataFrame(body_texts, columns=['this_year_id', 'html_text'])
        # df3 = df.merge(df2, on='this_year_id')
        # print('scrapped data shape is {0}, now writing to disk'.format(df3.shape))
        # new_filepath = 'procurements/nationwide/nationwide-all-' + year + '-with-main-texts.csv'
        # df3.to_csv(new_filepath, encoding='utf-8')


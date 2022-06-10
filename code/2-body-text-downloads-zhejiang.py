
# coding=utf-8

# webscrapper for Zhejiang procurement documents BODY TEXT
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


def get_one_doc(this_doc_info):
    global NA_html_count
    global_id, url_id = this_doc_info
    headers = {"User-Agent": 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) '
                      'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.5005.61 Mobile Safari/537.36', }
    link = 'https://zfcgmanager.czt.zj.gov.cn/cms/api/cors/remote/results?&url=noticeDetail&noticeId=' + str(url_id)

    errors = 0
    while True:
        if errors > 50:
            NA_html_count += 1
            return global_id, link, 'NA'
        try:
            response = requests.get(link, headers=headers, timeout=(5, 10))
            response.encoding = 'utf-8'
            if response.status_code == 403:
                print('403 Forbidden. pause and re-try Doc #{0}.'.format(global_id))
                time.sleep(random.randint(3, 7))
                errors += 1
                continue
            elif response.status_code == 404:
                print('404 Not found. skip this Doc #{0}.'.format(global_id))
                NA_html_count += 1
                return global_id, link, 'NA'
            elif response.status_code == 405:
                print('405 Not Allowed. pause and re-try Doc #{0}.'.format(global_id))
                time.sleep(random.randint(3, 7))
                errors += 1
                continue
            elif response.status_code == 200:
                if re.search('请求超时', response.text) is not None:
                    print('connection timed out. pause and re-try Doc #{0}.'.format(global_id))
                    errors += 1
                    continue
                elif re.search('浙江政府采购网', response.text) is None:
                    print('empty page returned. pause and re-try Doc #{0}: {1}.'.format(global_id, link))
                    errors += 1
                    continue
                else:
                    print('connection success: 200. Doc #{0}/{1} scrapped'.format(global_id, entire_docs_count))
                    return global_id, link, response.text
        except requests.exceptions.RequestException as e:
            print('Re-try doc #{0}/{1}: {2}'.format(global_id, total_docs_count, e))
            errors += 1


def write_data_to_file(batch_results):
    headers = ['zhejiang_global_id', 'doc_url', 'body_text']
    file_there = os.path.exists('raw_data/%s.csv' % filename)

    print('start writing data ====> ')
    with open('raw_data/%s.csv' % filename, 'a', encoding='utf-8') as file:
        # creating a csv writer object
        csv_writer = csv.writer(file)

        if not file_there:
            # writing headers
            csv_writer.writerow(headers)

        # writing the data rows
        csv_writer.writerows([row for row in batch_results])
        file.close()
    print('====> finished writing in %d rows of data' % len(batch_results))


def get_and_save_this_batch(this_batch_two_ids):
    global finished_batch_num
    """
    Create a thread pool to download all days simultaneously
    """
    local_start = time.time()
    this_batch_htmls_list = []
    futures_list = []
    with ThreadPoolExecutor(max_workers=threads_num) as executor:
        for this_doc_two_ids in this_batch_two_ids:
            futures = executor.submit(get_one_doc, this_doc_two_ids)
            futures_list.append(futures)

        for future in futures_list:
            this_doc_html = future.result()
            this_batch_htmls_list.append(this_doc_html)

        this_batch_htmls_list.sort()
        finished_batch_num += 1

        local_time_spent = time.time() - local_start
        total_time_spent = time.time() - global_start
        current_speed = len(this_batch_htmls_list)/local_time_spent
        left_docs_count = total_docs_count - finished_batch_num*batch_size
        print("====> \n Total {0} procurement notices downloaded, time elapsed {1} minutes,"
              " \n this batch speed is {2} notices per second, estimated time to finish: {3} minutes"
              " \n====>".format(finished_batch_num*batch_size, total_time_spent/60,
                                current_speed, left_docs_count/(current_speed*60)))

        this_batch_htmls_list.sort()
        write_data_to_file(this_batch_htmls_list)


if __name__ == "__main__":
    global_start = time.time()
    threads_num = 500
    filename = 'zhejiang-local-all-2002-2021_html_text3'

    zhejiang_local = pd.read_csv('raw_data/zhejiang-local-all-2002-2021.csv')
    entire_docs_count = len(zhejiang_local)
    NA_html_count = 0

    total_docs_count = len(zhejiang_local)
    global_ids = list(zhejiang_local.zhejiang_global_id)
    url_ids = list(zhejiang_local.id)
    batch_size = 50000
    global_id_batches = [global_ids[i:i + batch_size] for i in range(0, total_docs_count, batch_size)]
    url_id_batches = [url_ids[i:i + batch_size] for i in range(0, total_docs_count, batch_size)]

    finished_batch_num = 0
    for global_id_batch, url_id_batch in zip(global_id_batches, url_id_batches):
        docs_info = zip(global_id_batch, url_id_batch)
        get_and_save_this_batch(docs_info)

    print('{0} docs downloading complete with {1} empty htmls'.format(total_docs_count, NA_html_count))
    # OLD: 2089245 docs downloading complete with 527 empty htmls
    # OLD2: 2089245 docs downloading complete with 692 empty htmls
    # NOW: 2089245 docs downloading complete with 527 empty htmls
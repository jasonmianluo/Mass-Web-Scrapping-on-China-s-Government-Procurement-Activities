# coding=utf-8

# webscrapper Govt AI Purchase contracts
# downloading search results
# May 31, 2022

import requests
from requests.adapters import HTTPAdapter
from concurrent.futures import ThreadPoolExecutor

import re
from bs4 import BeautifulSoup
import time
import random
import csv
import pandas
import os
from threading import Timer


# set parameters
proxy_ip_api = 'http://webapi.http.zhimacangku.com/getip?num=201&type=1&pro=&city=0' \
               '&yys=0&port=11&time=1&ts=0&ys=0&cs=0&lb=1&sb=0&pb=4&mr=2&regions='
threads_num = 48
proxy_IPs = []
proxy_set_interval = 1200


def get_search_urls():
    # construct search requests URLs
    dates = [#'&start_time=2013%3A01%3A01&end_time=2014%3A01%3A01',
             #'&start_time=2014%3A01%3A01&end_time=2015%3A01%3A01',
             #'&start_time=2015%3A01%3A01&end_time=2016%3A01%3A01',
             #'&start_time=2016%3A01%3A01&end_time=2017%3A01%3A01',
             #'&start_time=2017%3A01%3A01&end_time=2018%3A01%3A01',
             #'&start_time=2018%3A01%3A01&end_time=2019%3A01%3A01',
             #'&start_time=2019%3A01%3A01&end_time=2020%3A01%3A01',
             #'&start_time=2020%3A01%3A01&end_time=2020%3A12%3A31']
             '&start_time=2021%3A01%3A01&end_time=2021%3A12%3A31']
    urls = []
    url_prefix = 'http://search.ccgp.gov.cn/bxsearch?searchtype=1&page_index=1&bidSort=0&buyerName=&projectId=' \
                 '&pinMu=0&bidType=0&dbselect=bidx&kw='
    url_suffix = '&timeType=6&displayZone=%E5%85%A8%E9%83%A8&zoneId=&pppStatus=0&agentName='

    for date in dates:
        this_url = url_prefix + date + url_suffix
        urls.append(this_url)
    return urls


# def get_search_urls_old():
#     # construct search requests URLs for the OLD database prior to 2013
#     dates = [#'&start_time=2002%3A01%3A01&end_time=2002%3A12%3A31',
#              #'&start_time=2003%3A01%3A01&end_time=2003%3A12%3A31',
#              #'&start_time=2004%3A01%3A01&end_time=2004%3A12%3A31',
#              #'&start_time=2005%3A01%3A01&end_time=2005%3A12%3A31',
#              #'&start_time=2006%3A01%3A01&end_time=2006%3A12%3A31',
#              #'&start_time=2007%3A01%3A01&end_time=2007%3A12%3A31',
#              #'&start_time=2008%3A01%3A01&end_time=2008%3A12%3A31',
#              #'&start_time=2009%3A01%3A01&end_time=2009%3A12%3A31',
#              #'&start_time=2010%3A01%3A01&end_time=2010%3A12%3A31',
#              #'&start_time=2011%3A01%3A01&end_time=2011%3A12%3A31',
#              #'&start_time=2012%3A01%3A01&end_time=2012%3A12%3A31']
#     urls = []
#     url_prefix = 'https://search.ccgp.gov.cn/oldsearch?searchtype=1&page_index=1&bidSort=0&buyerName=' \
#                  '&projectId=&pinMu=0&bidType=0&dbselect=bidx&kw='
#     url_suffix = '&timeType=6&displayZone=&zoneId=&agentName='
#
#     for date in dates:
#         this_url = url_prefix + date + url_suffix
#         urls.append(this_url)
#     return urls


def get_proxies(proxy_api):
    global proxy_IPs
    ip_list = []
    while True:
        try:
            ip_list = requests.get(proxy_api).text.split()
        except requests.RequestException as e:
            print('Retry getting proxies. {0}'.format(e))
            continue
        else:
            break

    proxies_list = []
    for i in range(0, len(ip_list)):
        proxies = {
            "http": "http://" + ip_list[i],
            "https": "https://" + ip_list[i]
        }
        proxies_list.append(proxies)

    print('{0} IPs obtained'.format(len(proxies_list)))
    print(proxies_list[0])
    proxy_IPs = proxies_list
    print('====> \n changed to a new set of {0} proxies, current global time is {1} hours \n====>'
          .format(len(proxy_IPs), (time.time() - global_time_start) / 3600))


def request_one_page(page_url):
    url_pn = int(re.search('page_index=(.+?)&', page_url).group(1))
    error_times = 0
    user_agent_list = ['Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:47.0) Gecko/20100101 Firefox/47.0',
                       'Mozilla/5.0 (Macintosh; Intel Mac OS X x.y; rv:42.0) Gecko/20100101 Firefox/42.0',
                       'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36',
                       'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.106 Safari/537.36 OPR/38.0.2220.41',
                       'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36 Edg/91.0.864.59',
                       'Mozilla/5.0 (compatible; MSIE 9.0; Windows Phone OS 7.5; Trident/5.0; IEMobile/9.0)',
                       'Opera/9.80 (Macintosh; Intel Mac OS X; U; en) Presto/2.2.15 Version/10.00',
                       'Opera/9.60 (Windows NT 6.0; U; en) Presto/2.1.1',
                       'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.64 Mobile Safari/537.36',
                       'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.106 Safari/537.36',
                       'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36',
                       'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.186 Safari/537.36',
                       'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.62 Safari/537.36',
                       'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.101 Safari/537.36',
                       'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.0)',
                       'Mozilla/5.0 (Macintosh; U; PPC Mac OS X 10.5; en-US; rv:1.9.2.15) Gecko/20110303 Firefox/3.6.15',
                       'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.5005.61 Mobile Safari/537.36']
    # randomly choose parameters for this call
    user_agent = user_agent_list[random.randint(0, 7)]
    headers = {"User-Agent": user_agent, }
               # 'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,'
               #           '*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
               # 'Accepted-Encoding': 'gzip, deflate',
               # 'cookie': 'Hm_lvt_9f8bda7a6bb3d1d7a9c7196bfed609b5=1651449777; '
               #           'Hm_lvt_9459d8c503dd3c37b526898ff5aacadd=1651452565; '
               #           'Hm_lpvt_9f8bda7a6bb3d1d7a9c7196bfed609b5=1653876481; '
               #           'JSESSIONID=QPoTxlRMWvXBZLoDLVbg5v8G4KQJ54UFEvlIJQOGrUpmcuyXqrX0!-408971185; '
               #           'Hm_lpvt_9459d8c503dd3c37b526898ff5aacadd=1653894175',
               # 'Host': 'search.ccgp.gov.cn',
               # 'Referer': page_url,
               # 'DNT': '1',
               # 'Upgrade-Insecure-Requests': '1',
               # 'Connection': 'close', }

    while True:
        try:
            if error_times == 0:
                # first pick up a proxy IP order imposed by page index
                proxy_index = (url_pn - 1) % len(proxy_IPs)
            else:
                proxy_index = random.randint(0, len(proxy_IPs) - 1)
            proxy_this_session = proxy_IPs[proxy_index]

            # if error_times > 100:
            #     print('more than 100 consecutive failed attempts on Page {0}. change one new IP'.format(url_pn))
            #     # if failed too many times with current set of proxies, change to one temporary new IP
            #     # this ENSURES quickly jumping out of deadlock in the scenario most proxies are dead
            #     temp_proxy_ip_api = 'http://http.tiqu.letecs.com:81/getip3?num=2&type=1&pro=&city=0&yys=0&port=1' \
            #                         '&pack=235753&ts=0&ys=0&cs=0&lb=1&sb=0&pb=4&mr=1&regions=&gm=4'
            #     proxy_list_temp = get_proxies(temp_proxy_ip_api)
            #     proxy_this_session = proxy_list_temp[0]

            # if error_times > 100:
            #     return 'error NA page {0}'.format(url_pn)

            session = requests.Session()
            session.mount('http://', HTTPAdapter(max_retries=0))
            session.mount('https://', HTTPAdapter(max_retries=0))
            response = requests.get(page_url, proxies=proxy_this_session, headers=headers, timeout=(10, 20))
            if response.status_code == 403:
                print('403 Forbidden. pause and re-try Page {0}.'.format(url_pn))
                # time.sleep(random.randint(1, 3))
                error_times += 1
                continue
            elif response.status_code == 405:
                print('405 Not Allowed. pause and re-try Page {0}.'.format(url_pn))
                time.sleep(random.randint(3, 7))
                error_times += 1
                continue
            elif response.status_code == 200:
                response.encoding = 'utf-8'
                print('connection success: 200')
                if re.search('过于频繁', response.text) is not None:
                    print('requests too frequent. change proxy and re-try Page {0}.'.format(url_pn))
                    # time.sleep(random.randint(1, 5))
                    error_times += 1
                    continue
                elif re.search('中国政府采购网', response.text) is None \
                        or re.search('size: (.+?),\r\n', response.text) is None:
                    # or re.search('size:(.+?),\r\n', response.text) is None:
                    print('empty page returned. change proxy and re-try Page {0}.'.format(url_pn))
                    # time.sleep(random.randint(1, 5))
                    error_times += 1
                    continue
                else:
                    total_pages = int(re.search('size: (.+?),\r\n', response.text).group(1))
                    current_page = int(re.search('current: (.+?),\r\n', response.text).group(1)) + 1
                    bids_num = len(BeautifulSoup(response.text, 'lxml').find(class_='vT-srch-result-list-bid').find_all('li'))

                    if (bids_num != 20 and current_page < total_pages) or total_pages == 0:
                        print('abnormal search results detected on page {0} for {1} bids. Re-try'
                              .format(current_page, bids_num))
                        error_times += 1
                        continue
                    elif current_page != url_pn:
                        print('page index in url is {0} and found in html {1} do not match. wrong page returned. '
                              'change proxy and re-try.'.format(url_pn, current_page)
                              .format(current_page, bids_num))
                        error_times += 1
                        continue
                    else:
                        print("Page #{0}/{1} scrapped".format(current_page, total_pages))
                        return response.text
        except requests.exceptions.RequestException as e:
            print('Change proxy and retry Page {0}: {1}'.format(url_pn, e))
            error_times += 1
            continue


def get_page_urls(search_link):
    first_page_text = request_one_page(search_link)
    total_pages = int(re.search('size: (.+?),\r\n', str(first_page_text)).group(1))

    all_pages_urls = []
    for i in range(0, total_pages):
        current_page_url = search_link.replace('&page_index=1', '&page_index={0}'.format(i+1), 1)
        all_pages_urls.append(current_page_url)
    print("There are {0} pages in total".format(total_pages))

    return all_pages_urls


def parse_page(html_doc):
    while True:
        try:
            # record page index parse
            search_page_index = int(re.search('page_index=(.+?);', html_doc).group(1))

            soup = BeautifulSoup(html_doc, 'lxml')
            bid_list = soup.find(class_='vT-srch-result-list-bid').find_all('li')
            contents = []
            for bid in bid_list:
                contract_url = bid.find('a').get('href')
                contract_name = bid.find('a').text.strip()
                contract_description = bid.find('p').text
                publish_time = bid.find('span').text.split('\r\n')[0]
                purchase_org = bid.find('span').text.split('\r\n')[1].strip()
                purchase_agency = bid.find('span').text.split('\r\n')[2].strip()
                contract_type = bid.find('span').find_all('strong')[0].text.strip()
                province = bid.find('span').find('a').text.strip()
                service_type = bid.find('span').find_all('strong')[1].text.strip()
                features = [contract_url, contract_name, contract_description, publish_time,
                            purchase_org, purchase_agency, contract_type, province, service_type,
                            search_page_index]
                contents.append(features)
            # check bids number on this page
            if len(contents) != 20:
                print('Abnormal detected on Page {0}: there are {1} bids in search results'
                      .format(search_page_index, len(contents)))
            return contents
        except:
            print("parse error, re-do this page")
            continue


def write_data_to_file(results, name):
    # column names
    headers = ['local_download_id', 'bid_url', 'bid_name', 'bid_description', 'publish_time',
               'procurement_govt_org', 'bidding_agency', 'announcement_type', 'province', 'project_type',
               'search_page_index']
    headers_there = os.path.exists('procurements/nationwide/%s.csv' % name)

    print('start writing data ====> ')
    with open('procurements/nationwide/%s.csv' % name, 'a', encoding='UTF-8') as file:
        # creating a csv writer object
        csv_writer = csv.writer(file)

        if headers_there is False:
            # writing headers
            csv_writer.writerow(headers)

        # writing the data rows
        csv_writer.writerows([n] + row for n, row in enumerate(results, 1))
        file.close()
    print('====> finished writing in %d rows of data' % len(results))


def save_params_speed_stats(this_stats, current_file):
    stats_record = current_file + 'stats'
    with open('procurements/nationwide/%s.csv' % stats_record, 'a', encoding='UTF-8') as f:
        # creating a csv writer object
        csvwriter = csv.writer(f)
        # writing in speed records
        csvwriter.writerows(row for row in this_stats)
        f.close()


def download_write_all_pages(urls, current_file):
    global proxy_IPs
    local_start = time.time()
    procurements = []
    speed_record = []

    """
    Create a thread pool to download all pages simultaneously
    """
    futures_list = []
    with ThreadPoolExecutor(max_workers=threads_num) as executor:
        for page_url in urls:
            futures = executor.submit(request_one_page, page_url)
            futures_list.append(futures)

        for future in futures_list:
            page_text = future.result()
            bids = parse_page(page_text)
            procurements = procurements + bids

            if len(procurements) % 100 == 0:
                local_time_spent = time.time() - local_start
                print("====> \n Total {0} procurement announcements downloaded, time elapsed {1} minutes,"
                      " \n cumulative speed is {2} bids per second, estimated time to finish: {3} minutes"
                      " \n====>".format(len(procurements), local_time_spent/60, len(procurements)/local_time_spent,
                                        ((len(urls)*20 - len(procurements))/(len(procurements)/local_time_spent)/60)))
                speed_record.append([local_time_spent/60, len(procurements)/local_time_spent,
                                     threads_num, len(proxy_IPs)])

            if len(procurements) % 10000 == 0:
                print(len(procurements))
                write_data_to_file(procurements[-10000:], current_file)
                print(len(speed_record))
                save_params_speed_stats(speed_record[-100:], current_file)

    return procurements, speed_record


class RepeatedTimer(object):
    def __init__(self, interval, function, *args, **kwargs):
        self._timer = None
        self.function = function
        self.interval = interval
        self.args = args
        self.kwargs = kwargs
        self.is_running = False
        self.start()

    def _run(self):
        self.is_running = False
        self.start()
        self.function(*self.args, **self.kwargs)

    def start(self):
        if not self.is_running:
            self._timer = Timer(self.interval, self._run)
            self._timer.start()
            self.is_running = True

    def stop(self):
        self._timer.cancel()
        self.is_running = False


if __name__ == "__main__":
    global_time_start = time.time()
    get_proxies(proxy_ip_api)

    # get a new set of proxies every X seconds in global operations
    # this runs independently of the main threads pool below
    rt = RepeatedTimer(proxy_set_interval, get_proxies, proxy_ip_api)
    print('automatic change of proxy set scheduled for every {0} minutes. '
          '(independent from main threads pool)'.format(proxy_set_interval/60))

    search_urls = get_search_urls()
    # search_urls =get_search_urls_old()

    for search_url in search_urls:
        filename = 'nationwide-all-' + re.search('start_time=(.+?)%', search_url).group(1) + '-' \
                   + re.search('end_time=(.+?)%', search_url).group(1)
        print("Now search {0} via: {1}".format(filename, search_url))

        page_urls = get_page_urls(search_url)

        # if filename == 'nationwide-all-2021-2021':
        #     del page_urls[0:77000]
        #     print('there are indeed {0} pages left to be scrapped'.format(len(page_urls)))

        data, speed_stats = download_write_all_pages(page_urls, filename)
        last_obs = len(data) % 10000
        if last_obs > 0:
            write_data_to_file(data[-last_obs:], filename)
            save_params_speed_stats(speed_stats[-int(last_obs/100):], filename)

        df = pandas.read_csv('procurements/nationwide/%s.csv' % filename)
        df.insert(0, 'this_year_id', range(1, len(df)+1))
        df.to_csv('procurements/nationwide/%s-new.csv' % filename, encoding='utf-8', index=False)
        print('{0} with {1} docs written into /{2}-new.csv'.format(filename, len(df), filename))



# Jason Luo
# May 27, 2022

import pandas as pd


# 1. Remove extra data into the next year's 01-01

# headers = ['this_year_id', 'local_download_id', 'bid_url', 'bid_name', 'bid_description', 'publish_time',
#                'procurement_govt_org', 'bidding_agency', 'announcement_type', 'province', 'project_type',
#                'search_page_index']

dataset_names = [#'nationwide-all-2013-2014',
                 #'nationwide-all-2014-2015',
                 #'nationwide-all-2015-2016',
                 #'nationwide-all-2016-2017',
                 #'nationwide-all-2017-2018'
                 #'nationwide-all-2018-2019',
                 #'nationwide-all-2019-2020'
                 ]

# extra_row_nums = [17, 23, 118, 26, 123, 193, 277]

import pandas as pd
path = 'procurements/nationwide/nationwide-all-'
df = pd.read_csv(path + '2013-2014.csv')

headers = ['local_download_id', 'bid_url', 'bid_name', 'bid_description', 'publish_time',
           'procurement_govt_org', 'bidding_agency', 'announcement_type', 'province', 'project_type',
           'search_page_index']
df.columns = headers
df = df[df.bid_url != 'bid_url']

# clean zhejiang next year first day's duplicate bids
df.drop(df.index[[0, 1, 2, 3,
                  41044, 41045,
                  183719, 183720, 183721, 183722, 183723,
                  190669,
                  206934, 206935, 206936]], inplace=True)

# df.drop(df.index[0:out_row_num], inplace=True)
df.reset_index(drop=True, inplace=True)
# df.drop(columns=['this_year_id'], inplace=True)
# df = df.rename(columns={'id': 'local_download_id'})
df.insert(0, 'zhejiang_global_id', df.index + 1)


df.to_csv(path + '20XX.csv', encoding='utf-8', index=False)

# 2. catch the missing pages

import re
from collections import Counter
import time
import datetime

urls = list(df['bid_url'])
dates = [re.search('/t(.+?)_', str(url)).group(1) for url in urls]
counts = Counter(dates)

times = list(df.publish_time)
check_len = [len(time) for time in times]
set(check_len)
time_in_the_day = [time[-8:] for time in times]


def get_sec(time_str):
    """Get seconds from time."""
    h, m, s = time_str.split(':')
    return int(h) * 3600 + int(m) * 60 + int(s)


seconds = [get_sec(time) for time in time_in_the_day]

# now work on dates
dates = [time[0:10] for time in times]


# from dates to seconds
def get_sec_from_dates(date):
    dt = datetime.datetime.strptime(date, '%Y.%m.%d')
    sec = time.mktime(dt.timetuple())
    return sec


dates_in_secs = [get_sec_from_dates(date) for date in dates]
timestamp = []
for second, date_in_secs in zip(seconds, dates_in_secs):
    timestamp.append(second + date_in_secs)

timestamp_lead = timestamp[1:len(timestamp)]
timestamp_lead.append(0)
intervals = []
for i, j in zip(timestamp, timestamp_lead):
    intervals.append(i - j)

intervals_in_hour = [second/3600 for second in intervals]

import numpy as np

top_30_index = np.argsort(intervals_in_hour)[-30:]
top_30_values = [intervals_in_hour[i] for i in top_30_index]

bottom_30_index = np.argsort(intervals_in_hour)[0:30]
bottom_30_values = [intervals_in_hour[i] for i in bottom_30_index]

# from bs4 import BeautifulSoup
# missing_bids1 = parse_page(missing_page_1.text)
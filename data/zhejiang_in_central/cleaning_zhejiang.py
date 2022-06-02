# Jason Luo
# June 1, 2022


# 1. Remove extra data into the next year's 01-01


import pandas as pd
path = 'procurements/zhejiang_in_central/zhejiang-all-2013-2022-with-main-texts.csv'
df = pd.read_csv(path)
df.drop(columns=['global_id', 'Unnamed: 0'], inplace=True)

headers = ['local_download_id', 'bid_url', 'bid_name', 'bid_description', 'publish_time',
           'procurement_govt_org', 'bidding_agency', 'announcement_type', 'province', 'project_type']
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
# df = df.rename(columns={'id': 'local_download_id'})
df.insert(0, 'zhejiang_global_id', df.index + 1)
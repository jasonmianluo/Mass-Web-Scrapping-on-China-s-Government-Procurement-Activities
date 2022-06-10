
# June 8, 2022
# clean zhejiang local all

import pandas as pd
import re
from bs4 import BeautifulSoup
import timeit
import time
import numpy as np
import warnings
warnings.filterwarnings("ignore", category=np.VisibleDeprecationWarning)
import html5lib
import jionlp as jio

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', None)


# (1) clean announcement / bid type names

zhejiang_local_all = pd.read_csv('procurements/zhejiang_in_local/raw_data/zhejiang-local-all-2002-2021.csv')
# total docs is 2,089,245

zhejiang_local_all.info()
zhejiang_local_all.typeName.value_counts().sort_values(ascending=False)

# 其他         704480
# 合同公告       440570
# 中标公告       316638
# 采购公告       316382
# 更正公告        55724
# 单一来源公告      45428
# 采购文件公示      40799
# 采购意向公开      39417
# 非政府采购公告     33001
# 废标流标公告      32828
# 成交公告        29127
# 进口产品公示      11078
# 公款竞争性存放     10865
# 其他公告        10040
# 答疑澄清公告       2047
# 结果公告          638
# 资格入围公告        135
# 征求意见           48

for i, typeName in enumerate(list(zhejiang_local_all.typeName)):
    if typeName == '其他' or typeName == '其他公告':
        try:
            new_type = re.search('..公告', zhejiang_local_all.title[i]).group(0)
            if new_type == '采购公告' and '非政府采购公告' in zhejiang_local_all.title[i]:
                zhejiang_local_all.typeName[i] = '非政府采购公告'
            elif '的公告' in new_type:
                new_type = re.search('...公告', zhejiang_local_all.title[i]).group(0)
                zhejiang_local_all.typeName[i] = new_type
            else:
                zhejiang_local_all.typeName[i] = new_type
        except:
            try:
                new_type = re.search('..公示', zhejiang_local_all.title[i]).group(0)
                if '的公示' in new_type:
                    new_type = re.search('...公示', zhejiang_local_all.title[i]).group(0)
                zhejiang_local_all.typeName[i] = new_type
            except:
                pass
            pass
    if (i + 1) % 1000 == 0:
        print('{0}/{1} documents processed'.format(i + 1, len(zhejiang_local_all)))

zhejiang_local_all.typeName.value_counts().sort_values()

# cleaning 其他 infos
zhejiang_local_all.typeName[zhejiang_local_all.typeName == '询价公告'] = '采购公告'
zhejiang_local_all.typeName[zhejiang_local_all.typeName == '磋商公告'] = '采购公告'
zhejiang_local_all.typeName[zhejiang_local_all.typeName == '结果公示'] = '结果公告'
zhejiang_local_all.typeName[zhejiang_local_all.typeName == '废标公告'] = '废标流标公告'
zhejiang_local_all.typeName[zhejiang_local_all.typeName == '中标公示'] = '中标公告'
zhejiang_local_all.typeName[zhejiang_local_all.typeName == '流标公告'] = '废标流标公告'
zhejiang_local_all.typeName[zhejiang_local_all.typeName == '邀请公告'] = '采购公告'
zhejiang_local_all.typeName[zhejiang_local_all.typeName == '谈判公告'] = '采购公告'
zhejiang_local_all.typeName[zhejiang_local_all.typeName == '采购公示'] = '单一来源公告'
zhejiang_local_all.typeName[zhejiang_local_all.typeName == '采购的公告'] = '终止采购公告'
zhejiang_local_all.typeName[zhejiang_local_all.typeName == '结果的公告'] = '结果公告'
zhejiang_local_all.typeName[zhejiang_local_all.typeName == '招标的公告'] = '采购公告'
zhejiang_local_all.typeName[zhejiang_local_all.typeName == '竞价公告'] = '采购公告'
zhejiang_local_all.typeName[zhejiang_local_all.typeName == '采购的公示'] = '单一来源公告'
zhejiang_local_all.typeName[zhejiang_local_all.typeName == '来源公示'] = '单一来源公告'
zhejiang_local_all.typeName[zhejiang_local_all.typeName == '合同公示'] = '合同公告'
zhejiang_local_all.typeName[zhejiang_local_all.typeName == '废标公示'] = '废标流标公告'
zhejiang_local_all.typeName[zhejiang_local_all.typeName == '流标公示'] = '废标流标公告'
zhejiang_local_all.typeName[zhejiang_local_all.typeName == '成交公示'] = '成交公告'
zhejiang_local_all.typeName[zhejiang_local_all.typeName == '磋商的公告'] = '采购公告'
zhejiang_local_all.typeName[zhejiang_local_all.typeName == '谈判的公告'] = '采购公告'
zhejiang_local_all.typeName[zhejiang_local_all.typeName == '合同的公告'] = '合同公告'
zhejiang_local_all.typeName[zhejiang_local_all.typeName == '废标的公告'] = '废标流标公告'
zhejiang_local_all.typeName[zhejiang_local_all.typeName == '来源公告'] = '单一来源公告'
zhejiang_local_all.typeName[zhejiang_local_all.typeName == '流标的公告'] = '废标流标公告'
zhejiang_local_all.typeName[zhejiang_local_all.typeName == '招标公示'] = '采购公告'
zhejiang_local_all.typeName[zhejiang_local_all.typeName == '蹉商公告'] = '采购公告'
zhejiang_local_all.typeName[zhejiang_local_all.typeName == '中标的公示'] = '中标公告'
zhejiang_local_all.typeName[zhejiang_local_all.typeName == '交）公告'] = '成交公告'
zhejiang_local_all.typeName[zhejiang_local_all.typeName == '果）公告'] = '结果公告'
zhejiang_local_all.typeName[zhejiang_local_all.typeName == '购）公告'] = '非政府采购公告'
zhejiang_local_all.typeName[zhejiang_local_all.typeName == '果）公示'] = '结果公告'
zhejiang_local_all.typeName[zhejiang_local_all.typeName == '果)公示'] = '结果公告'
zhejiang_local_all.typeName[zhejiang_local_all.typeName == '果)公告'] = '结果公告'
zhejiang_local_all.typeName[zhejiang_local_all.typeName == '选人公示'] = '结果公告'
# or change to '中标候选人公示' later for better money info extraction if needed


zhejiang_local_all.typeName.value_counts().sort_values()

# save cleaned dataset
zhejiang_local_all.to_csv('procurements/zhejiang_in_local/zhejiang-local-all-2002-2021.csv',
                          encoding='utf-8', index=False)

# (2) now subset to Results docs only
results = [zhejiang_local_all[zhejiang_local_all.typeName == '中标公告'],
           zhejiang_local_all[zhejiang_local_all.typeName == '成交公告'],
           zhejiang_local_all[zhejiang_local_all.typeName == '结果公告']]
zhejiang_local_results = pd.concat(results, ignore_index=True)
zhejiang_local_results.year.value_counts().sort_index(ascending=False)
zhejiang_local_results = zhejiang_local_results[zhejiang_local_results.year != 2014] # drop 3 abnormal obs
zhejiang_local_results.sort_values(by='zhejiang_global_id', inplace=True, ascending=True)
zhejiang_local_results.reset_index(drop=True, inplace=True)
zhejiang_local_results.to_csv('procurements/zhejiang_in_local/zhejiang-local-results.csv',
                              encoding='utf-8', index=False)
# 482390 obs

zhejiang_local_results.year.value_counts().sort_index()
# 2011         1
# 2013         1
# 2015     25120
# 2016     71427
# 2017     52550
# 2018     54417
# 2019     82584
# 2020     90959
# 2021    105331
# Name: year, dtype: int64

# (3) compare with central results
zhejiang_central_results = pd.read_csv('procurements/zhejiang_in_central/zhejiang-central-results.csv')
# 136611 obs

zhejiang_central_results.year.value_counts().sort_index()
# 2005      279
# 2006      420
# 2007      651
# 2008      778
# 2009     9231
# 2010     5813
# 2011     1448
# 2012    14525
# 2013    19648
# 2014    22410
# 2015    19849
# 2016    16662
# 2017     9344
# 2018     2895
# 2019     1690
# 2020     4181
# 2021     6787
# Name: year, dtype: int64


# (4) also subset Zhejiang LOCAL html_text file to results docs ONLY
import pandas as pd
zhejiang_local_results = pd.read_csv('procurements/zhejiang_in_local/zhejiang-local-results.csv')
results_global_ids = list(zhejiang_local_results.zhejiang_global_id)

indices = [False] * 2089245  # len(zhejiang_local_all)
for id in results_global_ids:
    indices[id-1] = True

# needs to read in and process df_html in batches
batch_size = 100000
indice_batches = [indices[i:i+batch_size] for i in range(0, len(indices), batch_size)]

df_list = []
count = 0
chunk_num = 0
with pd.read_csv('procurements/zhejiang_in_local/raw_data/zhejiang-local-all-2002-2021_html_text.csv',
                 chunksize=batch_size) as chunks:
    for chunk in chunks:
        df = chunk[indice_batches[chunk_num]]
        df_list.append(df)
        count += len(df)
        print(count)
        chunk_num += 1

zhejiang_local_results_html = pd.concat(df_list, ignore_index=True)
df_list = [] # release memory
zhejiang_local_results_html.reset_index(drop=True, inplace=True)
zhejiang_local_results_html.to_csv('procurements/zhejiang_in_local/raw_data/zhejiang-local-results_html_text.csv',
                                   encoding='utf-8', index=False)

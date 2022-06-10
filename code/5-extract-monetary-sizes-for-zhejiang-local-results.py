
# Jason Luo
# June 9, 2022
# extract monetary size for zheijiang local

import pandas as pd
from bs4 import BeautifulSoup
import re
import time
import json
import jionlp as jio

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)


df_results = pd.read_csv('procurements/zhejiang_in_local/zhejiang-local-results_raw_text.csv')
df_results.info()
# <class 'pandas.core.frame.DataFrame'>
# RangeIndex: 482390 entries, 0 to 482389
# Data columns (total 21 columns):
#  #   Column              Non-Null Count   Dtype
# ---  ------              --------------   -----
#  0   zhejiang_global_id  482390 non-null  int64
#  1   download_id         482390 non-null  int64
#  2   year                482390 non-null  int64
#  3   date                482390 non-null  object
#  4   id                  482390 non-null  int64
#  5   mainBidMenuName     308183 non-null  object
#  6   title               482389 non-null  object
#  7   projectCode         482390 non-null  object
#  8   projectName         482390 non-null  object
#  9   pubDate             482390 non-null  int64
#  10  districtName        482389 non-null  object
#  11  type                482390 non-null  int64
#  12  typeName            482390 non-null  object
#  13  keywords            0 non-null       float64
#  14  remark              0 non-null       float64
#  15  url                 482390 non-null  object
#  16  invalid             482390 non-null  int64
#  17  invalidDate         482390 non-null  int64
#  18  raw_text            482225 non-null  object
#  19  intext_tables       363080 non-null  object
#  20  intext_links        482225 non-null  object
# dtypes: float64(2), int64(8), object(11)
# memory usage: 77.3+ MB


# 3.1 extract from TABLE
table_moneys = [None] * len(df_results)
for i, tables in enumerate(df_results.intext_tables):

    if i % 10000 == 0:
        print('{0}/{1} documents processed'.format(i, len(df_results)))

    if pd.notna(tables):
        cols = tables.split('\n')
        for col in cols:
            col_name = col[0:col.find('>') + 1]
            if '单价' in col_name:
                continue
            if '（元）' in col_name or '(元)' in col_name or '（元)' in col_name or '(元）' in col_name:
                col = col.replace('（', '')
                col = col.replace('）', '')
                col = col.replace('(', '')
                col = col.replace(')', '')
                col = col.replace('元', '')
                col = col.replace(',', '元。,')
                moneys = jio.ner.extract_money(col, with_parsing=True)
                moneys = [item for item in moneys if item['detail']['definition'] == 'accurate']
                # print(moneys)
                table_moneys[i] = [money['detail']['num'] + money['detail']['case'] for money in moneys
                                   if float(money['detail']['num']) > 100]  # strip ZERO sizes

            elif '（万元）' in col_name or '(万元)' in col_name or '(万元）' in col_name or '（万元)' in col_name:
                col = col.replace('（', '')
                col = col.replace('）', '')
                col = col.replace('(', '')
                col = col.replace(')', '')
                col = col.replace('万元', '')
                col = col.replace(',', '万元。,')
                moneys = jio.ner.extract_money(col, with_parsing=True)
                moneys = [item for item in moneys if item['detail']['definition'] == 'accurate']
                # print(moneys)
                table_moneys[i] = [money['detail']['num'] + money['detail']['case'] for money in moneys
                                   if float(money['detail']['num']) > 100]  # strip ZERO sizes

            elif '价格' in col_name or '总价' in col_name \
                    or '金额' in col_name or '总额' in col_name \
                    or '成交额' in col_name or '成交价' in col_name\
                    or '中标价' in col_name or '中标额' in col_name \
                    or '结果价' in col_name or '最终价' in col_name:
                col = col.replace(',', '元。,')
                moneys = jio.ner.extract_money(col, with_parsing=True)
                moneys = [item for item in moneys if item['detail']['definition'] == 'accurate']
                # print(moneys)
                table_moneys[i] = [money['detail']['num'] + money['detail']['case'] for money in moneys
                                   if float(money['detail']['num']) > 100]  # strip ZERO / TINY sizes

sum([money is not None for money in table_moneys])
# 342571/482390 moneys extracted for zhejiang local 2002-2021 (.7102)

df_results['table_moneys'] = table_moneys


# 3.2 now extract monetary sizes from IN-TEXT
# 金额 section
# 中标结果：
# 成交情况：
# 八．中标结果：
# 中标金额：505000.00元;
# 中标（成交）金额：895.0991000（万元）
# 成交金额： 10250.00

body_moneys = [None] * len(df_results)
for i, body_text in enumerate(df_results.raw_text):

    if i % 10000 == 0:
        print('{0}/{1} documents processed'.format(i, len(df_results)))

    if pd.notna(body_text):
        body_text = body_text.replace('\n', '')
        body_text = body_text.replace('（', '')
        body_text = body_text.replace('）', '')
        body_text = body_text.replace('(', '')
        body_text = body_text.replace(')', '')
        body_text = body_text.replace('元', '元。')
        body_text = body_text.replace('人民币', '人民币。')

        body_text = body_text.replace('.00', '.00元。')  # may intro rating scores but those will be small (<100 mostly)
        body_text = body_text.replace('.00元。0', '.00')
        body_text = body_text.replace('.00元。万', '.00万元。')

        body_text = body_text.replace('RMB', '人民币')
        body_text = body_text.replace('rmb', '人民币')
        body_text = body_text.replace('CNY', '人民币')
        body_text = body_text.replace('cny', '人民币')
        body_text = body_text.replace('USD', '美元')
        body_text = body_text.replace('usd', '美元')

        money_texts = [item['text'] for item in jio.ner.extract_money(body_text, with_parsing=False)]
        money_texts = [text for text in money_texts if ('元' in text or '币' in text)]

        if money_texts:
            moneys = [jio.parse_money(money_text) for money_text in money_texts
                      if jio.parse_money(money_text)['definition'] == 'accurate']
            money_nums = [money['num'] + money['case'] for money in moneys
                          if float(money['num']) > 100]  # strip TINY also prop incorrect sized instances
            body_moneys[i] = money_nums

sum([money is not None for money in body_moneys])
# /482390 moneys extracted for zhejiang local 2002-2021 (?)
# total I have 342571 (moneys from table) + ? (moneys from body) = ? instances extracted
df_results['body_moneys'] = body_moneys

# save data
df_results.to_csv('/Users/jluo/PycharmProjects/Govt-AI-Contracts/procurements/zhejiang_in_local/'
                  'zhejiang-local-results_with_monetary_sizes.csv', encoding='utf-8', index=False)

# check missing cases
indices = list(pd.notna(df_results.table_moneys) + pd.notna(df_results.body_moneys))
# sum(indices) = 125325, meaning 0.9174 rows now having monetary size information extracted

# subset to those missing money information and investigate
missing_indices = [indice is False for indice in indices]
df_money_missing = df_results[missing_indices]
df_money_missing.to_csv('/Users/jluo/PycharmProjects/Govt-AI-Contracts/procurements/zhejiang_in_local/'
                  'zhejiang-local-results_money_missings.csv', encoding='utf-8', index=False)

# subset to those extracted and validate
df_money_extracted = df_results[indices]
df_money_extracted.to_csv('/Users/jluo/PycharmProjects/Govt-AI-Contracts/procurements/zhejiang_in_local/'
                  'zhejiang-local-results_money_extracted.csv', encoding='utf-8', index=False)
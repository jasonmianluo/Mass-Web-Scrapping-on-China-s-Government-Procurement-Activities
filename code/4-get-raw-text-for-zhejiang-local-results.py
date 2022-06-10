

# Jason Luo
# June 8, 2022

import pandas as pd
import re

import requests
from bs4 import BeautifulSoup
import timeit
import time
import numpy as np
import warnings
warnings.filterwarnings("ignore", category=np.VisibleDeprecationWarning)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', None)
import html5lib
import requests

# (1) # extract raw text from html


def test_all2(pages):
    bug_list = []
    for i, page in enumerate(pages):
        if re.search('vT_detail_main', page) is None:
            if re.search('vF_detail_main', page) is None:
                if re.search('text_wrap', page) is None:
                    print(i)
                    bug_list.append(i)
    return bug_list


def html_to_text(one_page):
    if one_page != 'nan':
        soup = BeautifulSoup(one_page, 'lxml')
        raw_text = soup.get_text(separator='\n', strip=True)
        intext_tables = ''
        try:
            tables_text = [table.get_text(separator='\n', strip=True) for table in soup.find_all('table') if len(table)]
            tables_text = [table_text for table_text in tables_text if table_text]
            table_num = len(tables_text)
            for table_text in tables_text:
                raw_text = raw_text.replace(table_text, '\n')

            if table_num > 0:
                html_str = one_page.lower()
                html_str = html_str.replace('colspan=\\"1\\"', 'colspan="1"')
                tables = pd.read_html(html_str)
                for table in tables:
                    table = table.applymap(str)
                    if table.shape != (1, 1):
                        if table.columns.dtype != 'int64':
                            table.loc[-1] = table.columns
                            table.index = table.index + 1
                            table.sort_index(inplace=True)
                            table.columns = list(range(len(table.columns)))

                        col_table = table.copy()
                        col_table.iloc[0, :] += '>'
                        col_table.iloc[1:, :] += ','
                        table_str = str(col_table.transpose()).replace('\\r\\n', '')
                        intext_tables += table_str.replace(' ', '') + '\n'

                        row_table = table.copy()
                        row_table.iloc[:, 0] += '>'
                        row_table.iloc[:, 1:] += ','
                        table_str = str(row_table).replace('\\r\\n', '')
                        intext_tables += table_str.replace(' ', '') + '\n'
        except:
            pass

        intext_links = [url.get('href') for url in soup.find_all('a') if url.get('href')]
        intext_links = intext_links + [url.get('href') for url in soup.find_all('link') if url.get('href')]
        raw_text = raw_text.replace('\\r\\n', '')

        return raw_text, intext_tables, intext_links

    else:
        return '', '', ''


if __name__ == '__main__':

    df = pd.read_csv('zhejiang-local-results.csv')
    df['raw_text'] = ''
    df['intext_tables'] = ''
    df['intext_links'] = ''

    # needs to read in and process df_html in batches
    batch_size = 10000
    with pd.read_csv('raw_data/zhejiang-local-results_html_text.csv', chunksize=batch_size) as chunks:
        finished_chunk_num = 0
        for chunk in chunks:
            print('now work on chunk #{0}'.format(finished_chunk_num+1))
            for index, html in enumerate(list(chunk.body_text)):
                index = index + finished_chunk_num*batch_size
                df.loc[index, ['raw_text', 'intext_tables', 'intext_links']] \
                    = pd.array(html_to_text(str(html)), dtype="object")

                if (index + 1) % 10000 == 0:
                    print('{0}/{1} documents processed'.format(index+1, len(df)))
            finished_chunk_num += 1

    df.to_csv('zhejiang-local-results_raw_text.csv', encoding='utf-8', index=False)


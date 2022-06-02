# Jason Luo
# May 27, 2022
# extract raw text from html

import pandas as pd
import re
from bs4 import BeautifulSoup


def html_to_text(one_page):
    soup = BeautifulSoup(one_page, 'lxml')
    title = soup.title.string
    body = soup.find(class_='vT_detail_main')
    if body is None:
        body = soup.find(class_='vF_detail_main')
    if body is None:
        body = soup.find(class_='text_wrap')

    raw_text = body.get_text()
    link = [url.get('href') for url in body.find_all('a') if url.get('href')]
    link = link + [url.get('href') for url in body.find_all('link') if url.get('href')]
    return title, raw_text, link


if __name__ == '__main__':
    filepath = 'ai-contracts_2013-2022_body_text.csv'
    df = pd.read_csv(filepath)
    df = df[df.local_id != 'id']

    titles = []
    raw_texts = []
    links = []
    for html in list(df.html_text):
        a, b, c = html_to_text(html)
        titles.append(a)
        raw_texts.append(b)
        links.append(c)

    df.drop(columns={'Unnamed: 0', 'local_id', 'html_text'}, inplace=True)
    df.columns = (['bid_id', 'bid_url', 'bid_name', 'bid_description', 'publish_time',
                   'procurement_govt_org', 'bidding_agency', 'announcement_type', 'province', 'project_type'])
    df['title'] = titles
    df['raw_text'] = raw_texts
    df['intext_link'] = links
    df.to_csv('ai-contracts_2013-2022_raw_text.csv', encoding='utf-8', index=False)

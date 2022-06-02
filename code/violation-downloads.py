# webscrapper Govt AI Purchase contracts
# downloading credit violation records
# May 15, 2022

# encoding = utf-8

import pandas, csv
import requests
from bs4 import BeautifulSoup
import time


def get_pages():
    url = 'https://www.ccgp.gov.cn/cr/list'
    page_list = []
    for i in range(1, 10):
        form_data = {'gp': i}
        try:
            response = requests.post(url, data=form_data)
            response.encoding = 'utf-8'
            page_list.append(response.text)
            time.sleep(3)
            print("Page {0} results saved, Now pause for 3 seconds".format(i))
        except requests.RequestException as e:
            print(e)
    return page_list


def parse_pages(page_list):
    all_records = []
    for i in range(0, len(page_list)):
        try:
            soup = BeautifulSoup(page_list[i], 'lxml')
            page_records = soup.find_all('tr', class_='trShow')
            for record in page_records:
                features = [item.text for item in record.find_all('td')]
                all_records.append(features)
        except:
            print("A parse error occurred on Page %d" % (i+1))
    return all_records


def write_data_to_file(results):
    # column names
    headers = ['id', 'company_name', 'SSN', 'company_address', 'violation_detail',
               'penalty', 'punishment_basis', 'punishment_date', 'announcement_date',
               'enforcement_unit']

    with open('contracts-datasets/violations.csv', 'w', encoding='UTF-8') as f:
        # creating a csv writer object
        csvwriter = csv.writer(f)
        # writing headers
        csvwriter.writerow(headers)
        # writing the data rows
        csvwriter.writerows(row for row in results)
    print('成功写入 %d 行违法失信行为记录' % len(results))


if __name__ == "__main__":
    pages = get_pages()
    records = parse_pages(pages)
    write_data_to_file(records)

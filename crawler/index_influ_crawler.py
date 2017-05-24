#!/urs/bin/env python
# -*- coding=utf-8 -*-

# __author__ = 'zhi'


import sys
import requests
from bs4 import BeautifulSoup
import MySQLdb
from public.sql_sentence import *
import json
import csv
import traceback
import random
from time import sleep
import datetime

reload(sys)
sys.setdefaultencoding("utf8")



def cal_index_influ_crawler():
    conn = MySQLdb.Connection(host="localhost", user="root", passwd="1qaz2wsx", db='cmb', charset="UTF8")

    url_index_dict, url_list = calender_data_load(u'财经指数信息.csv')

    today_date = datetime.datetime.today()

    for k in range(540,1080):

        rdm_num = float(random.randrange(0, 200, 1)) / 200
        sleep(rdm_num)

        d_date = datetime.timedelta(k)

        tmp_date = today_date - d_date

        date_str = tmp_date.strftime('%Y-%m-%d')
        print 'processing date ', date_str

        crawl_url = 'http://www.kxt.com/rili/%s.html'%date_str

        res = requests.get(crawl_url, timeout = 30)
        # print res.status_code

        soup_content = BeautifulSoup(res.content, 'lxml')

        container = soup_content.find('div', class_='container mt131')
        if not container:
            continue

        finance_table = container.find('table', class_='finance')

        finance_list = finance_table.find('tbody', class_='finance_list')

        data_list = finance_list.findAll('tr', class_='data')

        print len(data_list)

        for itm_data in data_list:
            td_list = itm_data.findAll('td')


            importance = td_list[3].find('span').get('title')


            url_txt = td_list[-1].find('a').get('href')

            url_txt = 'http://www.kxt.com' + url_txt

            if url_txt in url_list:
                eco_index_name = url_index_dict[url_txt]

                update_sen = "update calender_info set importance = '%s' where eco_index = '%s'"%(importance,eco_index_name)

                execute_insert(conn, update_sen)

    conn.close()

def calender_data_load(filename):
    url_index_dict = {}
    url_list = []
    csvfile = file(filename, 'rb')
    reader = csv.reader(csvfile)
    for line in reader:
        t_url = line[1]
        url_index_dict[t_url] = line[0]
        url_list.append(t_url)

    return url_index_dict, url_list

cal_index_influ_crawler()

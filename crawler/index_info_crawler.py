#!/urs/bin/env python
# -*- coding=utf-8 -*-

# __author__ = 'zhi'


import sys
import requests
from bs4 import BeautifulSoup
import json
import csv
import traceback
import random
from config.config import *
from time import sleep

reload(sys)
sys.setdefaultencoding("utf8")




def cal_index_info_crawler():

    csvfile = file(u'财经指数信息.csv', 'wb')
    writer = csv.writer(csvfile, dialect='excel')

    writer.writerow([u'财经指数', u'url', u'数据公布机构', u'发布频率', u'数据影响', u'数据释义', u'统计方法', u'关注原因',u'金银影响'])

    cal_info_url_list = []

    cal_info_url = RILI_WEB_URL + '/data/usoiac%3deci.html'

    res = requests.get(cal_info_url)

    # print res.status_code

    soup_content = BeautifulSoup(res.content, 'lxml')
    index_nation_list = soup_content.find('div',class_ = 'all-goods')

    li_list = index_nation_list.findAll('li')

    for itm_li in li_list:
        url_text = itm_li.find('a').get('href')
        title = itm_li.find('a').get('title')
        # print url_text
        cal_info_url_list.append(RILI_WEB_URL+url_text)

    print len(li_list)

    for k in range(len(cal_info_url_list)):
        print 'processing ', k, ' of ',len(cal_info_url_list)

        tmp_url = cal_info_url_list[k]

        rdm_num = float(random.randrange(0, 200, 1)) / 200
        sleep(rdm_num)

        # write_data = [tmp_title,tmp_url]

        res = requests.get(tmp_url)

        soup_content = BeautifulSoup(res.content, 'lxml')

        quote_list_wrap = soup_content.find('div', class_='quote_list_wrap')
        quote_list_wrap_mb0 = soup_content.find('div', class_='quote_list_wrap mb0')

        tmp_title = quote_list_wrap.find('div', class_='data_title').get_text()

        print tmp_title

        tmp_publisher = ''
        tmp_frequency = ''
        tmp_influence = ''
        tmp_explain = ''
        tmp_statistic = ''
        tmp_reason = ''
        tmp_gold_influ = ''

        data_info = quote_list_wrap.find('div', class_='data_info')

        dl_list = data_info.findAll('dl')

        for itm_dl in dl_list:
            if itm_dl.find('dt').get_text() == '基本信息':
                basic_info_dd_list = itm_dl.findAll('dd')
                tmp_publisher = basic_info_dd_list[0].get_text().split('：')[1]
                tmp_frequency = basic_info_dd_list[1].get_text().split('：')[1]
            elif itm_dl.find('dt').get_text() == '数据影响':
                tmp_influence = itm_dl.get_text().split('：')[1]
            elif itm_dl.find('dt').get_text() == '数据释义':
                tmp_explain = itm_dl.find('dd').get_text()
            elif itm_dl.find('dt').get_text() == '统计方法':
                tmp_statistic = itm_dl.find('dd').get_text()
            elif itm_dl.find('dt').get_text() == '关注原因':
                tmp_reason = itm_dl.find('dd').get_text()

        quote_list = quote_list_wrap_mb0.find('ul',class_='quote_list')
        li_list = quote_list.findAll('li')

        for itm_li in li_list:
            tmp = itm_li.get('class')
            if tmp:
                continue
            span_list = itm_li.findAll('span',class_='w155')
            if not span_list:
                continue
            try:
                pred_value = float(span_list[1].get_text().replace('%',''))
                cur_value = float(span_list[2].get_text().replace('%',''))
            except:
                continue
            if pred_value==cur_value:
                continue
            elif cur_value>pred_value:
                sig_influ = itm_li.find('span',class_='w180')
                influ_text = sig_influ.find('i').get('title')
                if '利空' in influ_text:
                    tmp_gold_influ = '利空'
                elif '利多' in influ_text:
                    tmp_gold_influ = '利多'
            elif cur_value < pred_value:
                sig_influ = itm_li.find('span', class_='w180')
                influ_text = sig_influ.find('i').get('title')
                if '利空' in influ_text:
                    tmp_gold_influ = '利多'
                elif '利多' in influ_text:
                    tmp_gold_influ = '利空'

            if tmp_gold_influ:
                break

        write_data = [tmp_title, tmp_url, tmp_publisher, tmp_frequency, tmp_influence, tmp_explain, tmp_statistic, tmp_reason,tmp_gold_influ]

        # print write_data

        writer.writerow(write_data)

    csvfile.close()


cal_index_info_crawler()
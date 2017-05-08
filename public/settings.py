#!/usr/lib/env python
#-*- coding=utf-8 -*-


import sys
import MySQLdb

reload(sys)
sys.setdefaultencoding('utf-8')

# 时间粒度对应最细粒度的跨度
# time_precision_step = {'1min':1,'5min':5,'30min':30,'1h':60,'4h':240,'1d':1440}
time_precision_step = {'1d':1}

## calender quant 时间粒度
# calender_time_precision_list = ['1min','5min','30min','1h','4h','1d']
calender_time_precision_list = ['1d']

## calender quant 时间跨度
calender_time_range_list = [1,3,5,10,20] #1,3,5,10,20


## calender index 名单
calender_index_list = [
    '美国季调后非农就业人口'
]
#    '英国工业产出年率'
#    '中国外汇储备',
#     '欧元区综合PMI初值',


## 黄金交易品名单
prod_list = [
    '上金所Au9999'
]
#    '伦敦金现价',
#     '上期所黄金期货',
#     'COMEX期货黄金'
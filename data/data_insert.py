#!/usr/lib/env python
#-*- coding=utf-8 -*-

import sys
import MySQLdb

reload(sys)
sys.setdefaultencoding('utf-8')

data_input_file = '/home/zhi/Desktop/招行/EcoDataCrawler/历史指标合并.csv'

conn = MySQLdb.Connection(host="localhost", user="root", passwd="1qaz2wsx", charset="UTF8")

def data_insert():
    data_input_file = '/home/zhi/Desktop/招行/EcoDataCrawler/历史指标合并.csv'
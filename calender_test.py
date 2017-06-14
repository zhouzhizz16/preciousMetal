#!/usr/lib/env python
#-*- coding=utf-8 -*-


import sys
import MySQLdb
import copy
import datetime
from public.sql_sentence import *
from config.config import *
import csv
import json

reload(sys)
sys.setdefaultencoding('utf-8')

conn = MySQLdb.Connection(host=pm_neo4j_DB.host, user=pm_neo4j_DB.user, passwd=pm_neo4j_DB.password, db=pm_neo4j_DB.database, charset="UTF8")


def time_stream_test(conn):
    data_input_file = '/home/zhi/Desktop/招行/EcoDataCrawler/历史指标合并.csv'

    time_stream_data = []
    with open(data_input_file, 'rb') as f:
        reader = csv.reader(f)

        for row in reader:
            if row[1] == '公布时间':
                continue

            if row[1] < '2016-01-01 00:00:00':
                continue

            itm_data = {'time':'','ecoIndex':'','curValue':0,'predValue':0,'prevValue':0}
            itm_data['time'] = row[1]
            row[2] = row[2].replace('%', '')
            itm_data['curValue'] = 0 if row[2] == '--' else float(row[2])
            row[3] = row[3].replace('%', '')
            itm_data['prevValue'] = 0 if row[3] == '--' else float(row[3])
            row[4] = row[4].replace('%', '')
            itm_data['predValue'] = 0 if row[4] == '--' else float(row[4])
            itm_data['ecoIndex'] = row[5]
            time_stream_data.append(itm_data)

    print 'number of calender eco index: ', len(time_stream_data)

    time_stream_data = sorted(time_stream_data, key=lambda x:x['time'])

    for itm_data in time_stream_data:
        eco_index_name = itm_data['ecoIndex']
        cur_value = itm_data['curValue']
        pred_value = itm_data['predValue']


        up_down = 0

        if pred_value > cur_value:
            up_down = 1
        elif pred_value < cur_value:
            up_down = -1

        select_sent = "select eco_index,pub_time,time_precision,time_range,correlation,num_price_up," \
                      "price_up_ratio,num_price_down,price_down_ratio,ave,std from calender_quant_res " \
                      "where eco_index = '%s' and up_down = %d and type = '%s' and count_similar > 10 " \
                      "and (price_up_ratio > 0.7 or price_down_ratio > 0.7)"\
                      %(eco_index_name,up_down,'预值差异')

        row_res = execute_select(conn, select_sent)

        if row_res:
            print '财经指标: ', eco_index_name, ', 公布时间: ', itm_data['time'], ', 预期值: ', pred_value, ', 公布值: ', cur_value

            for row in row_res:
                row = list(row)
                row[1] = str(row[1])
                # print json.dumps(row,ensure_ascii=False)

                print row[0],row[1],'时间粒度:',row[2],',时间跨度:',row[3],',相关性:',row[4],',价格上涨次数:',row[5],',价格上涨比例:', row[6],',价格下降次数:',row[7],',价格下降比例:',row[8],',平均波动幅度:',row[9],',方差:',row[10],'\n'


time_stream_test(conn)

conn.close()
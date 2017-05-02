#!/usr/lib/env python
#-*- coding=utf-8 -*-

import sys
import MySQLdb
import csv
from public.sql_sentence import *
reload(sys)
sys.setdefaultencoding('utf-8')

# data_input_file = '/home/zhi/Desktop/招行/EcoDataCrawler/历史指标合并.csv'

conn = MySQLdb.Connection(host="localhost", user="root", passwd="1qaz2wsx",db='cmb', charset="UTF8")

def data_insert():
    data_input_file = '/home/zhi/Desktop/招行/EcoDataCrawler/历史指标合并.csv'

    with open(data_input_file, 'rb') as f:
        reader = csv.reader(f)

        eco_ind_name = '美国季调后非农就业人口'
        sql_sen = "select * from calender_info where eco_index = '%s' "%eco_ind_name #
        res_sel = execute_select(conn, sql_sen)

        eco_ind_per = ''
        eco_ind_nation = ''
        for row_sel in res_sel:
            eco_ind_nation = row_sel[1]
            eco_ind_per = row_sel[2]

        for row in reader:
            if row[5] == '美国季调后非农就业人口':
                sql_ins = "replace into calender_rec (eco_index,pub_nation,pub_time,pub_period," \
                           "current_value,previous_value,predict_value,pred_diff,pred_diff_ratio,prev_diff," \
                           "prev_diff_ratio) value ('%s','%s','%s','%s',%d,%d,%d,%d,%d,%d,%d)"\
                           %(eco_ind_name,eco_ind_nation,row[1],eco_ind_per,float(row[2]),float(row[3]),float(row[4]),0,0,0,0)
                try:
                    execute_insert(conn,sql_ins)
                except:
                    pass

    conn.close()


data_insert()





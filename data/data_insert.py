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

def index_data_insert():
    data_input_file = '/home/zhi/Desktop/招行/EcoDataCrawler/历史指标合并.csv'

    with open(data_input_file, 'rb') as f:
        reader = csv.reader(f)

        eco_ind_name = '美国季调后非农就业人口'

        input_data = []
        for row in reader:
            if row[5] == eco_ind_name:
                row[1] = row[1].split(' ')[0]
                row[2] = row[2].replace('%','')
                row[2] = 0 if row[2]=='--' else float(row[2])
                row[3] = row[3].replace('%', '')
                row[3] = 0 if row[3] == '--' else float(row[3])
                row[4] = row[4].replace('%', '')
                row[4] = 0 if row[4] == '--' else float(row[4])
                input_data.append(row)

        processed_input_data = []
        for k in range(len(input_data) - 1):

            prd_diff = input_data[k][2] - input_data[k][4]
            prd_diff_ratio = prd_diff / input_data[k][4]

            prv_diff = input_data[k][2] - input_data[k][3]
            prv_diff_ratio = prv_diff / input_data[k][3]

            input_data[k].append(prd_diff)
            input_data[k].append(prd_diff_ratio)

            input_data[k].append(prv_diff)
            input_data[k].append(prv_diff_ratio)

            processed_input_data.append(input_data[k])

        input_data[len(input_data) - 1].append(0)
        input_data[len(input_data) - 1].append(0)
        input_data[len(input_data) - 1].append(0)
        input_data[len(input_data) - 1].append(0)
        processed_input_data.append(input_data[len(input_data) - 1])

        sql_sen = "select * from calender_info where eco_index = '%s' "%eco_ind_name #
        res_sel = execute_select(conn, sql_sen)

        eco_ind_per = ''
        eco_ind_nation = ''
        importance = ''
        for row_sel in res_sel:
            eco_ind_nation = row_sel[1]
            eco_ind_per = row_sel[2]
            # importance = ' ' #row_sel[3]

        for row in processed_input_data:
            pub_time = row[1].split(' ')[0]
            sql_ins = "replace into calender_rec (eco_index,pub_nation,pub_time,pub_period,importance," \
                       "current_value,previous_value,predict_value,pred_diff,pred_diff_ratio,prev_diff," \
                       "prev_diff_ratio) value ('%s','%s','%s','%s','%s',%f,%f,%f,%f,%f,%f,%f)"\
                       %(eco_ind_name,eco_ind_nation,pub_time,eco_ind_per,importance,float(row[2]),float(row[3]),float(row[4]),
                         float(row[6]),float(row[7]),float(row[8]),float(row[9]))
            try:
                execute_insert(conn,sql_ins)
            except:
                pass




def prod_data_insert():
    data_input_file = '/home/zhi/Desktop/招行/EcoDataCrawler/上金所Au9999.csv'

    with open(data_input_file, 'rb') as f:
        reader = csv.reader(f)

        prod_name = '上金所Au9999'

        input_data = []
        for row in reader:
            row[0] = row[0].split(' ')[0]
            if row[0] == '日期':
                continue
            row[2] = 0 if row[2] == '--' else float(row[2])
            row[5] = 0 if row[5] == '--' else float(row[5])
            row[3] = 0 if row[3] == '--' else float(row[3])
            row[4] = 0 if row[4] == '--' else float(row[4])
            row[9] = 0 if row[9] == '--' else float(row[9])
            row[10] = 0 if row[10] == '--' else float(row[10])
            input_data.append(row)

        processed_input_data = []
        for k in range(len(input_data)-1):
            prv_diff = input_data[k][5] - input_data[k+1][5]
            prv_diff_ratio = prv_diff/input_data[k + 1][5]

            input_data[k].append(prv_diff)
            input_data[k].append(prv_diff_ratio)

            processed_input_data.append(input_data[k])

        input_data[len(input_data)-1].append(0)
        input_data[len(input_data)-1].append(0)
        processed_input_data.append(input_data[len(input_data)-1])


        for row in processed_input_data:

            # pub_time = row[0]
            pub_time = row[0].split(' ')[0]

            if pub_time == '日期':
                continue

            open_p = 0 if row[2]=='--' else float(row[2])
            close_p = 0 if row[5]=='--' else float(row[5])
            high_p = 0 if row[3]=='--' else float(row[3])
            low_p = 0 if row[4]=='--' else float(row[4])
            volume = 0 if row[9]=='--' else float(row[9])
            turnover = 0 if row[10]=='--' else float(row[10])

            sql_ins = "replace into prod_data (prod_name,pub_time,open_price,close_price,high_price," \
                      "low_price,prev_diff,prev_diff_ratio,volume,turnover) value ('%s','%s',%f,%f,%f,%f,%f,%f,%d,%d)" \
                      % (prod_name, pub_time, open_p, close_p, high_p, low_p, row[12], row[13],volume, turnover)
            try:
                execute_insert(conn, sql_ins)
            except:
                pass



index_data_insert()
conn.close()





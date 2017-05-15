#!/usr/lib/env python
#-*- coding=utf-8 -*-


import sys
import MySQLdb
import numpy as np
from settings import *
from sql_sentence import *

reload(sys)
sys.setdefaultencoding('utf-8')


def statistic_cal(conn, mode, similar_time_list, t_pre, t_range, itm_prod):

    similar_time_str = '('
    for k in range(len(similar_time_list) - 1):
        similar_time_str += '\'' + similar_time_list[k].strftime('%Y-%m-%d %H:%M:%S') + '\'' + ','
    similar_time_str += '\'' + similar_time_list[-1].strftime('%Y-%m-%d %H:%M:%S') + '\'' + ')'


    sql_sel = "select pub_time, time_series_id from prod_data where prod_name = '%s' and " \
              "pub_time in %s" % (itm_prod, similar_time_str)

    # print sql_sel
    row_res = execute_select(conn, sql_sel)

    step_size = time_precision_step[t_pre]

    if mode in ['calender_quant','technique_analysis']:

        price_change_list = []
        for row in row_res:
            tmp_id = row[1]

            sql_sel_series = "select pub_time, time_series_id, close_price from prod_data where prod_name = '%s' and " \
                      "time_series_id in (%d,%d)" % (itm_prod, tmp_id , tmp_id+step_size * t_range)
            row_res_series = execute_select(conn, sql_sel_series)

            start_price = None
            end_price = None
            for row_series in row_res_series:
                if row_series[1] == tmp_id:
                    start_price = row_series[2]
                else:
                    end_price = row_series[2]

            if not start_price and not end_price:
                continue

            price_change_list.append(end_price-start_price)

        price_ave = np.average(price_change_list)
        price_std = np.std(price_change_list)

        output = {
            'ave': price_ave,
            'std': price_std,
            'priceChangeList':price_change_list
        }
        return output


    elif mode == 'time_series_analysis':

        price_change_list = []
        price_data_list = []
        for row in row_res:
            tmp_id = row[1]

            sql_sel_series = "select pub_time, time_series_id, close_price from prod_data where prod_name = '%s' and " \
                             "time_series_id between %d and %d" % (itm_prod, tmp_id - COUNT_DATA_PRE, tmp_id + step_size * t_range)
            row_res_series = execute_select(conn, sql_sel_series)

            start_price = None
            end_price = None

            tmp_price_list = []
            for row_series in row_res_series:
                tmp_price_list.append((row_series[0],row_series[2]))
                if row_series[1] == tmp_id:
                    start_price = row_series[2]
                elif row_series[1] == tmp_id + step_size * t_range:
                    end_price = row_series[2]

            if not start_price and not end_price:
                continue

            price_change_list.append(end_price - start_price)
            price_data_list.append(tmp_price_list)

        price_ave = np.average(price_change_list)
        price_std = np.std(price_change_list)

        output = {
            'ave': price_ave,
            'std': price_std,
            'priceChangeList': price_change_list,
            'priceDataList': price_data_list
        }
        return output












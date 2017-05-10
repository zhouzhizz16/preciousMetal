#!/usr/lib/env python
#-*- coding=utf-8 -*-


import sys
import MySQLdb
import copy
import datetime
import numpy as np
from public.settings import *
from public.sql_sentence import *
from public.statistic_cal import *

reload(sys)
sys.setdefaultencoding('utf-8')



def get_calender_quant_analysis(ana_params):
    conn = MySQLdb.Connection(host="localhost", user="root", passwd="1qaz2wsx", db='cmb', charset="UTF8")

    #获取财经指标及交易品
    index_name = ana_params.get('indexName','')
    prod_name = ana_params.get('prodName','')


    sql_sel = "select * from calender_quant_res where eco_index = '%s' and product = '%s'"%(index_name,prod_name)
    row_res = execute_select(conn,sql_sel)

    res = []
    res.append(['eco_index','pub_nation','pub_time','product','importance','time_precision','time_range',
                'type','count_similar','similar_info','correlation','macro_environ','up_down','ave','std'])

    for row in row_res:
        res.append(row)

    conn.close()

    return res



def calender_quant_analysis_offline():
    conn = MySQLdb.Connection(host="localhost", user="root", passwd="1qaz2wsx", db='cmb', charset="UTF8")

    ##循环经济指数,产品
    for itm_prod in prod_list:
        for itm_index in calender_index_list:
            for itm_precision in calender_time_precision_list:
                for itm_range in calender_time_range_list:
                    calender_quant_analysis_cal(conn,itm_prod,itm_index,itm_precision,itm_range)


    conn.close()

    return

##计算特定的产品,财经指数,时间粒度和跨度下的定量分析
def calender_quant_analysis_cal(conn,itm_prod,itm_index,itm_precision,itm_range):

    exotic_data_evaluation(conn, itm_prod, itm_index, itm_precision, itm_range, u'前值差异')

    exotic_data_evaluation(conn, itm_prod, itm_index, itm_precision, itm_range, u'预值差异')

    return


def exotic_data_evaluation(conn,itm_prod,itm_index,itm_precision,itm_range,v_type):

    if v_type == u'前值差异':
        col_type = 'prev_diff_ratio'
        compare_value = 'previous_value'
    else:
        col_type = 'pred_diff_ratio'
        compare_value = 'predict_value'

    sql_sel = "select * from prod_data where prod_name = '%s'" % (itm_prod)
    row_res = execute_select(conn, sql_sel)

    prod_data = []
    for row in row_res:
        prod_data.append(row)


    thr = 0.1

    sql_sel = "select pub_time,%s, current_value, %s from calender_rec " \
              "where eco_index = '%s' and abs(%s) > %d"%(col_type,compare_value,itm_index,col_type,thr)
    row_res = execute_select(conn,sql_sel)

    overall_time_list = []
    overall_index_value_change = []
    inc_time_list = []
    inc_index_value_change = []
    dec_time_list = []
    dec_index_value_change = []

    index_data_list = []
    for row in row_res:
        index_data_list.append(row)

        # if row[0] in prod_time_list:
        #     overall_time_list.append(row[0])
        #     overall_index_value_change.append(float(row[2]) - float(row[3]))
        #
        #     if float(row[1])>0:
        #         inc_time_list.append(row[0])
        #         inc_index_value_change.append(float(row[2]) - float(row[3]))
        #     else:
        #         dec_time_list.append(row[0])
        #         dec_index_value_change.append(float(row[2]) - float(row[3]))




    ##找到相同情况标的位置
    overall_corresp_index_list = []
    inc_corresp_index_list = []
    dec_corresp_index_list = []
    for k in range(len(prod_data)):
        row_data = prod_data[k]
        if row_data[1] in overall_time_list:
            overall_corresp_index_list.append(k)
            if row_data[1] in inc_time_list:
                inc_corresp_index_list.append(k)
            else:
                dec_corresp_index_list.append(k)

    sql_sel = "select * from calender_info where eco_index = '%s'"%itm_index
    row_res = execute_select(conn,sql_sel)
    for row in row_res:
        pub_nation = row[1]
        importance = '' #row[3]

    ##得到统计结果
    init_outcome = {
        'eco_index':itm_index,
        'pub_nation':pub_nation,
        'pub_time':datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'product':itm_prod,
        'importance':importance,
        'time_precision':itm_precision,
        'time_range':itm_range,
        'type':v_type,
        'count_similar':0,
        'similar_info':'',
        'correlation':0,
        'macro_environ':'',
        'up_down':0,
        'ave':0,
        'std':0
    }

    correlation = save_stat_res(conn, init_outcome, 'overall', index_data_list, itm_prod)
    save_stat_res(conn, init_outcome, 'increase', index_data_list, itm_prod,correlation)
    save_stat_res(conn, init_outcome, 'decrease', index_data_list, itm_prod,correlation)

    return


def save_stat_res(conn, init_outcome, label, index_data_list, itm_prod, correlation=0):
    tmp_outcome = copy.copy(init_outcome)

    time_pre = tmp_outcome['time_precision']
    time_range = tmp_outcome['time_range']
    step_size = time_precision_step[time_pre]

    sql_sel = "select pub_time, time_series_id from prod_data where prod_name = '%s'" % (itm_prod)
    row_res = execute_select(conn, sql_sel)
    prod_time_list = []
    prod_id_list = []

    prod_time_id_pair = {}
    for row in row_res:
        prod_time_list.append(row[0])
        prod_id_list.append(row[1])
        prod_time_id_pair[row[0]] = row[1]

    prod_id_max = max(prod_id_list)
    prod_id_min = min(prod_id_list)

    used_index_data_list = []
    if label == 'overall':
        filtered_index_data_list = [row for row in index_data_list if row[0] in prod_time_list]
    elif label == 'increase':
        filtered_index_data_list = [row for row in index_data_list if row[0] in prod_time_list and float(row[1]) > 0]
    elif label == 'decrease':
        filtered_index_data_list = [row for row in index_data_list if row[0] in prod_time_list and float(row[1]) < 0]

    corresp_index_time_list = []
    index_value_change_list = []
    similar_info_str = ''
    for row in filtered_index_data_list:
        time_id = prod_time_id_pair[row[0]]
        if time_id + step_size * time_range >= prod_id_max:
            continue

        corresp_index_time_list.append(row[0])
        similar_info_str += row[0].strftime('%Y-%m-%d %H:%M:%S') + ','    #.strftime('%Y-%m-%d %H:%M:%S')
        index_value_change_list.append(float(row[2]) - float(row[3]))

    tmp_outcome['count_similar'] = len(corresp_index_time_list)
    tmp_outcome['similar_info'] = similar_info_str


    tmp_output = statistic_cal(conn, 'calender_quant', corresp_index_time_list, time_pre, time_range, itm_prod)

    tmp_outcome['ave'] = tmp_output.get('ave',0)
    tmp_outcome['std'] = tmp_output.get('std',0)
    price_change_list = tmp_output.get('priceChangeList',[])

    if label=='overall':

        correlation_cal = np.corrcoef(np.asarray(index_value_change_list),np.asarray(price_change_list))
        correlation = correlation_cal[0][1]

        tmp_outcome['correlation'] = correlation

    elif label == 'increase':

        tmp_outcome['up_down'] = 1
        tmp_outcome['correlation'] = correlation

    else:

        tmp_outcome['up_down'] = -1
        tmp_outcome['correlation'] = correlation


    sql_ins = "replace into calender_quant_res value ('%s','%s','%s','%s','%s','%s',%d,'%s',%d,'%s',%f,'%s'," \
              "%d,%f,%f)"%(tmp_outcome['eco_index'],tmp_outcome['pub_nation'],tmp_outcome['pub_time'],tmp_outcome['product'],
                           tmp_outcome['importance'],tmp_outcome['time_precision'],tmp_outcome['time_range'],
                           tmp_outcome['type'],tmp_outcome['count_similar'],tmp_outcome['similar_info'],
                           tmp_outcome['correlation'], tmp_outcome['macro_environ'], tmp_outcome['up_down'],
                           tmp_outcome['ave'], tmp_outcome['std'])

    # print sql_ins

    execute_insert(conn,sql_ins)


    return correlation



if __name__ == '__main__':
    calender_quant_analysis_offline()






#!/usr/lib/env python
#-*- coding=utf-8 -*-


import sys
import MySQLdb
import copy
import datetime
import numpy as np
from public.settings import *
from public.sql_sentence import *

reload(sys)
sys.setdefaultencoding('utf-8')



def get_calender_quant_analysis(ana_params):
    conn = MySQLdb.Connection(host="localhost", user="root", passwd="1qaz2wsx", db='cmb', charset="UTF8")

    #获取财经指标及交易品
    index_name = ana_params.get('indexName','')
    prod_name = ana_params.get('prodName','')


    sql_sel = "select * from calender_quant_res where eco_index = '%s' and product = '%s'"%(index_name,prod_name)
    row_res = execute_select(sql_sel)

    res = []
    res.append(['eco_index','pub_nation','pub_time','product','importance','time_precision','time_range',
                'type','count_similar','similar_info','correlation','macro_environ','up_down','ave','std'])

    for row in row_res:
        res.append(row)

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
    for row in row_res:
        overall_time_list.append(row[0])
        overall_index_value_change.append(float(row[2])-float(row[3]))
        if float(row[1])>0:
            inc_time_list.append(row[0])
            inc_index_value_change.append(float(row[2]) - float(row[3]))
        else:
            dec_time_list.append(row[0])
            dec_index_value_change.append(float(row[2]) - float(row[3]))


    sql_sel = "select * from prod_data where prod_name = '%s'"%(itm_prod)
    row_res = execute_select(conn, sql_sel)

    prod_data = []
    for row in row_res:
        prod_data.append(row)

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
    row_res = execute_select(sql_sel)
    for row in row_res:
        pub_nation = row[1]
        importance = row[3]

    ##得到统计结果
    init_outcome = {
        'eco_index':itm_index,
        'pub_nation':pub_nation,
        'pub_time':datetime.datetime.now(),
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

    correlation = save_stat_res(conn, init_outcome, 'overall', overall_corresp_index_list, prod_data,overall_index_value_change)
    save_stat_res(conn, init_outcome, 'increase', inc_corresp_index_list, prod_data,inc_index_value_change,correlation)
    save_stat_res(conn, init_outcome, 'decrease', dec_corresp_index_list, prod_data,dec_index_value_change,correlation)

    return


def save_stat_res(conn, init_outcome, label, corresp_index_list, prod_data,index_value_change, correlation=0):
    time_pre = init_outcome['time_precision']
    time_range = init_outcome['time_range']

    tmp_outcome = copy.copy(init_outcome)

    step_size = time_precision_step[time_pre]

    price_change_list = []
    tmp_outcome['count_similar'] = len(corresp_index_list)
    similar_date_list = []
    for itm_index in corresp_index_list:
        start_price = prod_data[itm_index][3]
        end_price = prod_data[itm_index+step_size*time_range][3]

        similar_date_list.append(prod_data[itm_index][1])

        price_change_list.append(end_price-start_price)

    tmp_outcome['similar_info'] = str(similar_date_list)

    tmp_outcome['avg'] = np.average(price_change_list)
    tmp_outcome['std'] = np.std(price_change_list)

    if label=='overall':

        correlation_cal = np.corrcoef(np.asarray(index_value_change),np.asarray(price_change_list))
        correlation = correlation_cal[0][1]

        tmp_outcome['correlation'] = correlation

    elif label == 'increase':

        tmp_outcome['up_down'] = 1

    else:

        tmp_outcome['up_down'] = -1


    sql_ins = "replace into calender_quant_res value ('%s','%s',%s,'%s','%s','%s',%d,'%s',%d,'%s',%d,'%s'," \
              "%d,%d,%d)"%(tmp_outcome['eco_index'],tmp_outcome['pub_nation'],tmp_outcome['pub_time'],tmp_outcome['product'],
                           tmp_outcome['importance'],tmp_outcome['time_precision'],tmp_outcome['time_range'],
                           tmp_outcome['type'],tmp_outcome['count_similar'],tmp_outcome['similar_info'],
                           tmp_outcome['correlation'], tmp_outcome['macro_environ'], tmp_outcome['up_down'],
                           tmp_outcome['ave'], tmp_outcome['std'])

    execute_insert(conn,sql_ins)


    return correlation










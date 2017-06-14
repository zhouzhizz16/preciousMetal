#!/usr/lib/env python
#-*- coding=utf-8 -*-


import sys
import MySQLdb
import copy
import datetime
import numpy as np
from config.config import *
from public.settings import *
from public.sql_sentence import *
from public.statistic_cal import *
from visualization.plot import *

reload(sys)
sys.setdefaultencoding('utf-8')


##根据参数获取财经日历定量分析结果
def get_calender_quant_analysis(ana_params):
    conn = MySQLdb.Connection(host=pm_neo4j_DB.host, user=pm_neo4j_DB.user, passwd=pm_neo4j_DB.password,
                              db=pm_neo4j_DB.database, charset="UTF8")

    #获取财经指标及交易品
    index_name = ana_params.get('indexName','')
    prod_name = ana_params.get('prodName','')
    t_pre = ana_params.get('precision','')
    t_range = ana_params.get('range',0)


    sql_sel = "select * from calender_quant_res where eco_index = '%s' and product = '%s' and " \
              "time_precision = '%s' and time_range = %d "%(index_name,prod_name,t_pre,t_range)
    row_res = execute_select(conn,sql_sel)

    res = []
    res.append(['eco_index','pub_nation','pub_time','product','importance','time_precision','time_range',
                'type','count_similar','similar_info','index_value_change','price_change','correlation',
                'macro_environ','up_down','ave','std'])

    for row in row_res:
        res.append(row)

    show_calender_quant_res(res)

    conn.close()

    return res



def calender_quant_analysis_offline():
    conn = MySQLdb.Connection(host="localhost", user="root", passwd="1qaz2wsx", db='cmb', charset="UTF8")

    overall_prod_list = []
    overall_calender_index_list = []

    sel_sent = "select distinct prod_name from prod_data"
    row_res = execute_select(conn, sel_sent)
    for row in row_res:
        overall_prod_list.append(row[0])

    sel_sent = "select distinct eco_index from calender_rec"
    row_res = execute_select(conn, sel_sent)
    for row in row_res:
        overall_calender_index_list.append(row[0])


    ##循环经济指数,产品
    for itm_prod in overall_prod_list:
        for itm_index in overall_calender_index_list:
            print 'processing ', itm_index, ' on ', itm_prod
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
    if not row_res:
        return
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
        'num_price_up':0,
        'num_price_down':0,
        'ave':0,
        'std':0,
        'index_value_change':'',
        'price_change':''
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
    tmp_outcome['num_price_up'] = tmp_output.get('numPriceUp',0)
    tmp_outcome['num_price_down'] = tmp_output.get('numPriceDown', 0)
    if tmp_outcome['count_similar'] == 0:
        tmp_outcome['price_up_ratio'] = 0
        tmp_outcome['price_down_ratio'] = 0
    else:
        tmp_outcome['price_up_ratio'] = float(tmp_outcome['num_price_up'])/tmp_outcome['count_similar']
        tmp_outcome['price_down_ratio'] = float(tmp_outcome['num_price_down']) / tmp_outcome['count_similar']

    index_value_change_str = ''
    for itm_ind_value in index_value_change_list:
        index_value_change_str += str(itm_ind_value) + ','

    price_change_str = ''
    for itm_price in price_change_list:
        price_change_str += str(itm_price) + ','

    tmp_outcome['index_value_change'] = index_value_change_str
    tmp_outcome['price_change'] = price_change_str

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


    sql_ins = "replace into calender_quant_res value ('%s','%s','%s','%s','%s','%s',%d,'%s',%d,'%s','%s','%s',%f,'%s'," \
              "%d,%d,%f,%d,%f,%f,%f)"%(tmp_outcome['eco_index'],tmp_outcome['pub_nation'],tmp_outcome['pub_time'],
                           tmp_outcome['product'],tmp_outcome['importance'],tmp_outcome['time_precision'],
                           tmp_outcome['time_range'],tmp_outcome['type'],tmp_outcome['count_similar'],
                           tmp_outcome['similar_info'],tmp_outcome['index_value_change'],tmp_outcome['price_change'],
                           tmp_outcome['correlation'], tmp_outcome['macro_environ'], tmp_outcome['up_down'],
                           tmp_outcome['num_price_up'],tmp_outcome['price_up_ratio'],tmp_outcome['num_price_down'],
                           tmp_outcome['price_down_ratio'], tmp_outcome['ave'], tmp_outcome['std'])

    # print sql_ins

    execute_insert(conn,sql_ins)


    return correlation


##展示财经日历定量分析结果
def show_calender_quant_res(res):
    import pandas as pd

    res_data = pd.DataFrame(res[1:],columns=res[0])

    print res_data['eco_index']

    res_data = res_data[['type','up_down','index_value_change','price_change']]

    for row in res_data.values:
        if row[0] == u'预值差异' and row[1] == 0:
            index_tmp_list = row[2].split(',')
            price_tmp_list = row[3].split(',')

            if not index_tmp_list or not price_tmp_list:
                continue

            index_change_list = np.asarray([float(x) for x in index_tmp_list if x])
            price_change_list = np.asarray([float(x) for x in price_tmp_list if x])
            reg_line_plot(index_change_list,price_change_list)

    return



if __name__ == '__main__':
    calender_quant_analysis_offline()

    # ana_params = {
    #     "prodName": '上金所Au9999',
    #     "indexName": '欧元区服务业PMI初值',
    #     "range": 20,
    #     "precision": '1d'
    # }
    #
    # get_calender_quant_analysis(ana_params)






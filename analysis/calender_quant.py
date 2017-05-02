#!/usr/lib/env python
#-*- coding=utf-8 -*-


import sys
import MySQLdb

reload(sys)
sys.setdefaultencoding('utf-8')



def get_calender_quant_analysis(ana_params):

    #获取财经指标及交易品
    index_name = ana_params.get('indexName','')
    prod_name = ana_params.get('prodName','')

    ##特定需求分析
    query_ana = ana_params.get('query','')
    ##通用分析,推荐
    recom_ana = ana_params.get('recommend',True)

    res = {
        'query':{},'recommend':{}
    }

    #从数据库获取结果
    if query_ana:
        res['query'] = get_calender_quant_query(index_name,prod_name)

    #
    if recom_ana:
        res['query'] = get_calender_quant_recom(index_name,prod_name)

    return

def get_calender_quant_query(index_name,prod_name):
    return

def get_calender_quant_recom(index_name,prod_name):
    return


def calender_quant_analysis_offline():
    return
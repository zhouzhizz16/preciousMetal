#!/usr/lib/env python
#-*- coding=utf-8 -*-


import sys
import MySQLdb

from analysis.calender_quant import get_calender_quant_analysis
from analysis.calender_quali import calender_quali_analysis
from analysis.time_series import time_series_analysis
from analysis.technical import technical_analysis

reload(sys)
sys.setdefaultencoding('utf-8')

def precious_metal_analysis(ana_mode,ana_params):

    ##功能模块


    if ana_mode == 'calender_quant':
        # 财经日历定量分析
        res = get_calender_quant_analysis(ana_params)
    elif ana_mode == 'calender_quali':
        # 财经日历定性分析
        res = calender_quali_analysis(ana_params)
    elif ana_mode == 'time_series':
        # 时间序列分析
        res = time_series_analysis(ana_params)
    elif ana_mode == 'technical':
        # 技术指标分析分析
        res = technical_analysis(ana_params)

    return res





if __name__=='__main__':
    ana_params = {
        "prodName":'上金所Au9999',
        "indexName":'美国季调后非农就业人口',
        "range":5,
        "precision":'1d'
    }
    res = precious_metal_analysis('calender_quant', ana_params)

    import json
    print json.dumps(res,ensure_ascii=False,indent=4)
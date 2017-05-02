#!/usr/lib/env python
#-*- coding=utf-8 -*-


import sys
import MySQLdb

from analysis.calender_quant import calender_quant_analysis
from analysis.calender_quali import calender_quali_analysis
from analysis.time_series import time_series_analysis
from analysis.technical import technical_analysis

reload(sys)
sys.setdefaultencoding('utf-8')

def precious_metal_analysis(ana_mode,ana_params):

    ##功能模块


    if ana_mode == 'calender_quant':
        # 财经日历定量分析
        res = calender_quant_analysis(ana_params)
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





# if __name__=='__main__':

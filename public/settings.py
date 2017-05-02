#!/usr/lib/env python
#-*- coding=utf-8 -*-


import sys
import MySQLdb

reload(sys)
sys.setdefaultencoding('utf-8')


## calender quant 时间粒度
calender_time_precision_list = ['1min','5min','30min','1h','4h','1d']

## calender quant 时间跨度
calender_time_range_list = [1,3,5,10,20]

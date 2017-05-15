#!/usr/lib/env python
#-*- coding=utf-8 -*-


import sys
import MySQLdb
import seaborn as sns
import matplotlib.pyplot as plt
from public.settings import COUNT_DATA_PRE
import numpy as np


reload(sys)
sys.setdefaultencoding('utf-8')

## 指数及价格变动回归
def reg_line_plot(index_change_list,price_change_list):
    # plt.figure()
    sns.jointplot(index_change_list,price_change_list,kind='reg');
    ax = plt.gca()
    ax.set_ylabel('index value change')
    ax.set_xlabel('price change')
    plt.show()
    return

## 价格走势加权平均线
def weighted_ave_line_plot(price_data_list):
    COUNT_DATA_PRE = 3

    plt.figure()

    weighted_ave_line = np.zeros(len(price_data_list[0]) - COUNT_DATA_PRE)
    for k in range(len(price_data_list)):
        itm_price_datas = price_data_list[k]
        itm_price_datas = sorted(itm_price_datas,key=lambda x:x[0])

        price_data = np.asarray([x[1] for x in itm_price_datas])
        price_data = price_data - price_data[COUNT_DATA_PRE]
        weighted_ave_line += np.asarray(price_data[COUNT_DATA_PRE:])

        x_axis =  list(range(-COUNT_DATA_PRE,len(weighted_ave_line)))

        plt.plot(x_axis, price_data, '-r')

    weighted_ave_line = weighted_ave_line/len(price_data_list)

    plt.plot(list(range(len(weighted_ave_line))), weighted_ave_line, '--b')

    plt.show()

    return


if __name__=='__main__':
    test_data = [
        [(0, 1), (2, 3), (4, 4), (6, 4), (1, 8), (3, 9), (5, 7)],
        [(0, 3), (2, 5), (4, 7), (6, 8), (1, 4), (3, 3), (5, 4)],
        [(0, 2), (2, 7), (4, 8), (6, 3), (1, 4), (3, 5), (5, 9)]
    ]

    weighted_ave_line_plot(test_data)





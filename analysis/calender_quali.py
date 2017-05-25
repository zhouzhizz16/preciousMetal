#!/usr/lib/env python
# -*- coding=utf-8 -*-


import sys
import MySQLdb
from config.config import *
from public.sql_sentence import *
from fuzzywuzzy import fuzz

reload(sys)
sys.setdefaultencoding('utf-8')


def calender_quali_analysis(ana_params):
    conn = MySQLdb.Connection(host=pm_neo4j_DB.host, user=pm_neo4j_DB.user, passwd=pm_neo4j_DB.password, db=pm_neo4j_DB.database, charset="UTF8")

    try:
        index_name = ana_params.get('indexName','')
        if not index_name:
            conn.close()
            return {}

        select_sql = "select eco_index from calender_info"

        row_res = execute_select(conn,select_sql)

        eco_index_score_list = []
        for row in row_res:
            store_eco_index_name = row[0]
            tmp_score = fuzz.ratio(index_name,store_eco_index_name)

            eco_index_score_list.append((store_eco_index_name,tmp_score))

        eco_index_score_list = sorted(eco_index_score_list,key= lambda x:x[1],reverse=True)

        mateched_eco_index = ''
        if eco_index_score_list[0][1]>0.9:
            mateched_eco_index = eco_index_score_list[0][0]

        print 'index found: ',mateched_eco_index


        select_sql = "select pub_period, importance, influence, index_explain, index_statistic, reason, gold_influence" \
                     " from calender_info where eco_index = '%s' "%mateched_eco_index

        row_res = execute_select(conn, select_sql)
        eco_index_info_dict = {'ecoIndexName':mateched_eco_index}
        for row in row_res:
            eco_index_info_dict['pubPeriod'] = u'公布周期:' + row[0]
            eco_index_info_dict['importance'] = u'重要性:' + row[1]
            eco_index_info_dict['influence'] = u'数据影响:' + row[2]
            eco_index_info_dict['indexExplain'] = u'数据释义:' + row[3]
            eco_index_info_dict['indexStatistic'] = u'统计方法:' + row[4]
            eco_index_info_dict['reason'] = u'关注原因:' + row[5]
            eco_index_info_dict['goldInfluence'] = u'对黄金影响:公布值>预期值时' + row[6] + u'金银,公布值<预期值时则相反'

        conn.close()

        return eco_index_info_dict
    except:
        conn.close()

        return {}
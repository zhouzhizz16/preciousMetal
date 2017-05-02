#!/usr/lib/env python
#-*- coding=utf-8 -*-

import traceback

def execute_insert(conn,sql):
    cur = conn.cursor()
    try:
        cur.execute(sql)
        # rows = cur.fetchall()
    except:
        print traceback.print_exc()
        conn.rollback()
        # rows = 'sql错误'
    conn.commit()
    cur.close()
    # return rows

def execute_select(conn,sql):
    cur = conn.cursor()
    try:
        cur.execute(sql)
        rows = cur.fetchall()
    except:
        print traceback.print_exc()
        conn.rollback()
        rows = 'sql错误'
    cur.close()
    return rows
import pandas as pd
import pymysql
import sqlalchemy
import numpy as np


def conn(server, db):
    connect = None
    if server == 'db_151':
        connect = pymysql.connect(
                user='test_user',
                passwd='qwe123',
                host='192.168.0.151',
                db=db,
                charset='utf8')

    return connect


conn_dic = {"user": 'pycf',
            "passwd": '1qaz@WSXabc',
            "host": '192.168.0.151',
            "db": 'py_bond_2_1',
            "charset": 'utf8',
            "port": '3306'}

conn_str = 'mysql+pymysql://%(user)s:%(passwd)s@%(host)s:%(port)s/%(db)s?charset=utf8' % conn_dic

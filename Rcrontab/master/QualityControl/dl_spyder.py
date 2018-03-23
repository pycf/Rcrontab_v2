import pandas as pd
import sqlalchemy
import numpy as np
from .sql_conn import conn, conn_str


class QualityControl:

    def __init__(self, tables_list):
        self.tables_list = tables_list

    def __call__(self, *args, **kwargs):
        for table_info in self.tables_list:
            server, db, table = table_info
            result = CheckFactory(server, db, table).run()
            if len(result) != 0:
                return result
            else:
                return None


class CheckFactory:

    def __init__(self, server, db, table):
        self.check_dict = {
            'py_bond_2_1': CheckBond

        }
        self.server = server
        self.db = db
        self.table = table

    def run(self):
        if self.db in self.check_dict:
            func = self.check_dict[self.db]
            result = func(self.server, self.table)()
            return result


class CheckBond:
    def __init__(self, server, table):
        db = 'py_bond_2_1'
        self.conn = conn(server, db)
        self.table = table
        self.result = {}

    def __call__(self, *args, **kwargs):
        self.check_null()
        self.check_range()
        self.check_logic()
        return self.result

    # #######第一步检查#########
    # ###列出质控表里判断不能为空的表和对应字段
    def check_null(self):
        issuer_info = "SELECT table_name,column_name FROM py_bond_data_check_mabiao " \
                      "where  check_type=1 and table_name={table_name};".format(table_name=self.table)
        result = pd.read_sql(issuer_info, self.conn)

        # ###循环调用非空质控检查函数
        for idx in result.index:
            self._get_null(result.iloc[idx].values[0], result.iloc[idx].values[1])

    # #######第二步检查#########
    # ###列出质控表里针对范围检查的表和对应字段
    def check_range(self):
        issuer_info = "SELECT table_name,column_name,min_value,max_value " \
                      "FROM py_bond_data_check_mabiao " \
                      "where  check_type=2 and table_name={table_name};".format(table_name=self.table)

        result1 = pd.read_sql(issuer_info, conn)

        # ###循环调用非空质控检查函数
        for idx in result1.index:
            self._get_range(result1.iloc[idx].values[0], result1.iloc[idx].values[1],
                            result1.iloc[idx].values[2], result1.iloc[idx].values[3])

    # #######第三步检查#########
    # 列出质控表里针对逻辑判断的表和对应字段
    def check_logic(self):
        issuer_info = "SELECT table_name,column_name,judging_condition " \
                      "FROM py_bond_data_check_mabiao " \
                      "where  check_type=3 and table_name={table_name};".format(table_name=self.table)

        result2 = pd.read_sql(issuer_info, conn)

        # 循环调用非空质控检查函数
        if self.table in result2.index:
            self._get_judging(result2.iloc[self.table].values[0], result2.iloc[self.table].values[1],
                              result2.iloc[self.table].values[2])

    # ########插入字段为空的数据进推送表
    def _get_null(self, table_name, column_name):
        if_null = "select * from %s where %s is null or %s='';" % (table_name, column_name, column_name)
        # result1=pd.read_sql(if_null, conn)
        result1 = pd.read_sql(if_null, conn)
        # print(pd.isnull(result1))
        if result1.empty:
            pass
        else:
            self.result['null'] = 1
            result1["account"] = len(result1) * ["denglei"]
            result1["error_desc"] = len(result1) * [column_name + '为空']
            engine = sqlalchemy.create_engine(conn_str)
            result1.to_sql(table_name + '_ts', con=engine, if_exists='append', index=False)

    # ########插入字段范围检查有误的数据进推送表
    def _get_range(self, table_name, column_name, min_value, max_value):

        if max_value and not np.isnan(max_value):
            condition1 = str(column_name) + "<" + str(max_value)

        else:
            condition1 = "1=1"
        if min_value and not np.isnan(min_value):
            condition2 = " and " + str(column_name) + ">" + str(min_value)
        else:
            condition2 = "and 1=1"

        if_null = "select * from %s where %s %s;" % (table_name, condition1, condition2)
        result1 = pd.read_sql(if_null, conn)
        if result1.empty:
            pass
        else:
            self.result['range'] = 1
            result1["account"] = len(result1) * ["denglei"]
            result1["error_desc"] = len(result1) * [column_name + '值范围有错，正确范围应为 %s -%s' % (min_value, max_value)]
            engine = sqlalchemy.create_engine(conn_str)
            result1.to_sql(table_name + '_ts', con=engine, if_exists='append', index=False)

    # 插入字段逻辑判断有误的数据进推送表

    def _get_judging(self, table_name, column_name, judging_condition):
        if_null = "select * from %s where %s ;" % (table_name, judging_condition)
        results = pd.read_sql(if_null, conn)

        if results.empty:
            pass
        else:
            self.result['logical'] = 1
            results["account"] = len(results) * ["denglei"]
            results["error_desc"] = len(results) * [column_name + '有逻辑错误，错误原因为:' + judging_condition]
            engine = sqlalchemy.create_engine(conn_str)
            results.to_sql(table_name + '_ts', con=engine, if_exists='append', index=False)



